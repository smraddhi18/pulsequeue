import type { FastifyInstance, LightMyRequestResponse } from 'fastify';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, expect, it, vi } from 'vitest';
import { buildApp } from '../../src/app.js';
import { redisKeys } from '../../src/redis/keys.js';
import { QueueStatsRepository } from '../../src/repositories/stats.repository.js';
import type { QueueStatsResponse } from '../../src/routes/stats.js';
import {
  createAdminRedis,
  createTestDatabase,
  deleteRedisPrefix,
  describeWithRedis,
  RedisOutageProxy,
  redisTestEnv,
  testEnv,
  uniqueRedisPrefix,
  waitUntil,
  type TestDatabase,
} from './helpers.js';
import { authedInject, createTestApiKey } from './auth-helpers.js';

describeWithRedis('queue stats API (Postgres + Redis)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let admin: Redis;
  const prefixes: string[] = [];
  const apps: FastifyInstance[] = [];
  let token: string;

  beforeAll(async () => {
    db = await createTestDatabase();
    admin = createAdminRedis();
    token = (await createTestApiKey(db.pool)).token;
  });

  afterAll(async () => {
    await admin?.quit();
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    for (const app of apps.splice(0)) await app.close();
    for (const prefix of prefixes.splice(0)) await deleteRedisPrefix(admin, prefix);
  });

  /** An API instance with its own key prefix, waited on until Redis is connected. */
  async function startApp(
    overrides: Record<string, string> = {},
    options: { prefix?: string; redis?: boolean } = {},
  ): Promise<FastifyInstance> {
    const prefix = options.prefix ?? uniqueRedisPrefix();
    if (!prefixes.includes(prefix)) prefixes.push(prefix);
    const env =
      options.redis === false
        ? testEnv(overrides)
        : redisTestEnv(prefix, {
            STATS_CACHE_MIN_REFRESH_MS: '0',
            STATS_INVALIDATION_COALESCE_MS: '1',
            ...overrides,
          });
    const app = await buildApp({ env, logger: false, pool: db.pool });
    apps.push(app);
    await app.ready();
    if (options.redis !== false && overrides.STATS_CACHE_ENABLED !== 'false') {
      await waitUntil(async () => {
        const res = await app.inject({ method: 'GET', url: '/ready' });
        return res.json<{ checks: Record<string, string> }>().checks.redis === 'ok';
      });
    }
    return app;
  }

  const stats = (app: FastifyInstance, queue = 'emails') =>
    authedInject(app, token)({ method: 'GET', url: `/queues/${queue}/stats` });

  const body = (res: LightMyRequestResponse) => res.json<QueueStatsResponse>();

  async function enqueue(app: FastifyInstance, queue = 'emails', payload: unknown = {}) {
    const res = await authedInject(
      app,
      token,
    )({
      method: 'POST',
      url: `/queues/${queue}/jobs`,
      payload: { payload },
    });
    expect(res.statusCode).toBe(201);
  }

  it('404 for an unknown queue, never cached', async () => {
    const app = await startApp();
    const res = await stats(app, 'nope');
    expect(res.statusCode).toBe(404);
    expect(res.json()).toMatchObject({ error: { code: 'NOT_FOUND' } });
    await enqueue(app, 'nope');
    const after = await stats(app, 'nope');
    expect(after.statusCode).toBe(200);
    expect(body(after).counts.queued).toBe(1);
  });

  it('MISS, then HIT with the same snapshot; response shape and headers', async () => {
    const app = await startApp();
    await enqueue(app);
    await enqueue(app);
    await waitUntil(
      async () => (await admin.exists(redisKeys(prefixes[0]!).statsDirty('default:emails'))) === 1,
    );

    const first = await stats(app);
    expect(first.statusCode).toBe(200);
    expect(first.headers['x-cache']).toBe('MISS');
    expect(first.headers['cache-control']).toBe('no-store');
    expect(body(first)).toMatchObject({
      queue: 'emails',
      counts: { queued: 2, scheduled: 0, running: 0, succeeded: 0, dead: 0 },
      total: 2,
      nextScheduledAt: null,
      oldestRunningSince: null,
    });
    expect(body(first).oldestQueuedAgeMs).toBeGreaterThanOrEqual(0);

    const second = await stats(app);
    expect(second.headers['x-cache']).toBe('HIT');
    expect(body(second)).toEqual(body(first));
  });

  it('an enqueue invalidates the entry: the next read recomputes', async () => {
    const app = await startApp();
    const keys = redisKeys(prefixes[0]!);
    await enqueue(app);
    await stats(app);
    expect((await stats(app)).headers['x-cache']).toBe('HIT');

    await enqueue(app);
    await waitUntil(async () => (await admin.exists(keys.statsDirty('default:emails'))) === 1);
    const res = await stats(app);
    expect(res.headers['x-cache']).toBe('MISS');
    expect(body(res).counts.queued).toBe(2);
    expect((await stats(app)).headers['x-cache']).toBe('HIT');
  });

  it('serves STALE inside the minimum refresh interval, bounded by it', async () => {
    const app = await startApp({ STATS_CACHE_MIN_REFRESH_MS: '300', STATS_CACHE_TTL_MS: '5000' });
    const keys = redisKeys(prefixes[0]!);
    await enqueue(app);
    await stats(app); // MISS: entry with queued 1
    await enqueue(app);
    await waitUntil(async () => (await admin.exists(keys.statsDirty('default:emails'))) === 1);

    const stale = await stats(app);
    expect(stale.headers['x-cache']).toBe('STALE');
    expect(body(stale).counts.queued).toBe(1);

    await waitUntil(async () => (await stats(app)).headers['x-cache'] === 'MISS', {
      timeoutMs: 2_000,
      intervalMs: 50,
    });
    expect(body(await stats(app)).counts.queued).toBe(2);
  });

  it('stampede: 20 concurrent cold reads cost one Postgres query', async () => {
    const app = await startApp();
    await enqueue(app);
    // Let the enqueue's (asynchronous, coalesced) invalidation land first: if it
    // arrived mid-stampede it would legitimately mark the fresh entry dirty and
    // cause a second compute (MIN_REFRESH is 0 here), which is not a stampede.
    await waitUntil(
      async () => (await admin.exists(redisKeys(prefixes[0]!).statsDirty('default:emails'))) === 1,
    );
    const compute = vi.spyOn(QueueStatsRepository.prototype, 'compute');
    const responses = await Promise.all(Array.from({ length: 20 }, () => stats(app)));
    expect(responses.every((r) => r.statusCode === 200)).toBe(true);
    expect(compute).toHaveBeenCalledTimes(1);
    expect(new Set(responses.map((r) => body(r).computedAt)).size).toBe(1);
  });

  it('two API instances share the cache and one dirty flag: only one recomputes', async () => {
    const prefix = uniqueRedisPrefix();
    const a = await startApp({}, { prefix });
    const b = await startApp({}, { prefix });
    await enqueue(a);
    expect((await stats(a)).headers['x-cache']).toBe('MISS');
    expect((await stats(b)).headers['x-cache']).toBe('HIT');

    await enqueue(b);
    await waitUntil(
      async () => (await admin.exists(redisKeys(prefix).statsDirty('default:emails'))) === 1,
    );
    const [ra, rb] = await Promise.all([stats(a), stats(b)]);
    expect([ra.headers['x-cache'], rb.headers['x-cache']].sort()).toEqual(
      expect.arrayContaining(['MISS']),
    );
    expect([ra, rb].filter((r) => r.headers['x-cache'] === 'MISS')).toHaveLength(1);
  });

  it('BYPASS when the cache is disabled or Redis is not configured', async () => {
    const off = await startApp({ STATS_CACHE_ENABLED: 'false' });
    await enqueue(off);
    expect((await stats(off)).headers['x-cache']).toBe('BYPASS');
    expect((await stats(off)).headers['x-cache']).toBe('BYPASS');

    const none = await startApp({}, { redis: false });
    const res = await stats(none);
    expect(res.headers['x-cache']).toBe('BYPASS');
    expect(body(res).counts.queued).toBe(1);
    const ready = await none.inject({ method: 'GET', url: '/ready' });
    expect(ready.json()).toEqual(expect.objectContaining({ status: 'ready' }));
  });

  it('Redis outage: stats BYPASS from Postgres, enqueue works, /ready degraded (200), then recovers', async () => {
    const proxy = await RedisOutageProxy.start();
    try {
      const prefix = uniqueRedisPrefix();
      prefixes.push(prefix);
      const app = await buildApp({
        env: redisTestEnv(prefix, {
          REDIS_URL: proxy.url,
          STATS_CACHE_MIN_REFRESH_MS: '0',
          REDIS_RECONNECT_BASE_MS: '20',
          REDIS_RECONNECT_MAX_MS: '100',
        }),
        logger: false,
        pool: db.pool,
      });
      apps.push(app);
      await app.ready();
      const ready = () => app.inject({ method: 'GET', url: '/ready' });
      await waitUntil(async () => (await ready()).json<{ status: string }>().status === 'ready');
      await enqueue(app);
      expect((await stats(app)).headers['x-cache']).toBe('MISS');

      await proxy.cut();
      await waitUntil(async () => (await ready()).json<{ status: string }>().status === 'degraded');
      const degraded = await ready();
      expect(degraded.statusCode).toBe(200);
      expect(degraded.json()).toMatchObject({
        status: 'degraded',
        checks: { app: 'ok', postgres: 'ok', redis: 'fail' },
      });

      await enqueue(app); // writes never depend on Redis
      const started = Date.now();
      const res = await stats(app);
      expect(Date.now() - started).toBeLessThan(500);
      expect(res.headers['x-cache']).toBe('BYPASS');
      expect(body(res).counts.queued).toBe(2);

      await proxy.restore();
      await waitUntil(async () => (await ready()).json<{ status: string }>().status === 'ready', {
        timeoutMs: 5_000,
      });
      // The invalidation was lost during the outage: the cached entry
      // (queued 1) is still there and is served until its TTL. That is the
      // documented bound; here we only check the cache is used again.
      expect(['HIT', 'MISS']).toContain((await stats(app)).headers['x-cache']);
    } finally {
      await proxy.stop();
    }
  });
});
