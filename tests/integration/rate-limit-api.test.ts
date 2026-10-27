import type { FastifyInstance, LightMyRequestResponse } from 'fastify';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import {
  createAdminRedis,
  createTestDatabase,
  deleteRedisPrefix,
  describeWithDb,
  describeWithRedis,
  RedisOutageProxy,
  redisTestEnv,
  testEnv,
  uniqueRedisPrefix,
  waitUntil,
  type TestDatabase,
} from './helpers.js';
import { authedInject, createTestApiKey } from './auth-helpers.js';

async function jobCount(db: TestDatabase): Promise<number> {
  const { rows } = await db.pool.query<{ n: number }>('SELECT count(*)::int AS n FROM jobs');
  return rows[0]!.n;
}

describeWithRedis('enqueue rate limiting (Postgres + Redis)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let admin: Redis;
  const prefixes: string[] = [];
  const apps: FastifyInstance[] = [];
  const cleanups: (() => Promise<unknown>)[] = [];
  let acme: string;
  let globex: string;

  beforeAll(async () => {
    db = await createTestDatabase();
    admin = createAdminRedis();
    acme = (await createTestApiKey(db.pool, { tenantId: 'acme' })).token;
    globex = (await createTestApiKey(db.pool, { tenantId: 'globex' })).token;
  });

  afterAll(async () => {
    await admin?.quit();
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
    await db.pool.query('UPDATE tenants SET rate_limit_per_sec = NULL, rate_limit_burst = NULL');
  });

  afterEach(async () => {
    for (const app of apps.splice(0)) await app.close();
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
    for (const prefix of prefixes.splice(0)) await deleteRedisPrefix(admin, prefix);
  });

  /** Tenant default: 3 tokens, refilling at 1/s (slow enough to be deterministic). */
  async function startApp(
    overrides: Record<string, string> = {},
    options: { prefix?: string } = {},
  ): Promise<FastifyInstance> {
    const prefix = options.prefix ?? uniqueRedisPrefix();
    if (!prefixes.includes(prefix)) prefixes.push(prefix);
    const env = redisTestEnv(prefix, {
      RATE_LIMIT_TENANT_PER_SEC: '1',
      RATE_LIMIT_TENANT_BURST: '3',
      AUTH_CACHE_TTL_MS: '0',
      ...overrides,
    });
    const app = await buildApp({ env, logger: false, pool: db.pool });
    apps.push(app);
    await app.ready();
    await waitUntil(async () => {
      const res = await app.inject({ method: 'GET', url: '/ready' });
      return res.json<{ checks: Record<string, string> }>().checks.redis === 'ok';
    });
    return app;
  }

  const enqueue = (app: FastifyInstance, token: string, queue = 'emails', body: unknown = {}) =>
    authedInject(
      app,
      token,
    )({
      method: 'POST',
      url: `/queues/${queue}/jobs`,
      payload: { payload: body },
    });

  async function drain(app: FastifyInstance, token: string, queue = 'emails') {
    const out: LightMyRequestResponse[] = [];
    for (let i = 0; i < 5; i++) out.push(await enqueue(app, token, queue));
    return out;
  }

  it('reports the budget on success and answers 429 with Retry-After once it is spent', async () => {
    const app = await startApp();
    const res = await drain(app, acme);
    expect(res.map((r) => r.statusCode)).toEqual([201, 201, 201, 429, 429]);
    expect(res[0]!.headers).toMatchObject({
      'ratelimit-limit': '3',
      'ratelimit-remaining': '2',
      'ratelimit-reset': '1',
    });
    expect(res[2]!.headers['ratelimit-remaining']).toBe('0');

    const limited = res[3]!;
    expect(limited.headers['retry-after']).toBe('1');
    expect(limited.headers['ratelimit-remaining']).toBe('0');
    expect(limited.json()).toMatchObject({
      error: { code: 'RATE_LIMITED', details: { scope: 'tenant' } },
    });
    // Refused enqueues never reached Postgres.
    expect(await jobCount(db)).toBe(3);
  });

  it('refills: after Retry-After the tenant can enqueue again', async () => {
    const app = await startApp({ RATE_LIMIT_TENANT_PER_SEC: '20', RATE_LIMIT_TENANT_BURST: '1' });
    expect((await enqueue(app, acme)).statusCode).toBe(201);
    expect((await enqueue(app, acme)).statusCode).toBe(429);
    await new Promise((r) => setTimeout(r, 120));
    expect((await enqueue(app, acme)).statusCode).toBe(201);
  });

  it('keeps tenants apart and honours a tenant-specific limit', async () => {
    await db.pool.query(
      `UPDATE tenants SET rate_limit_per_sec = 1, rate_limit_burst = 1 WHERE id = 'globex'`,
    );
    const app = await startApp();
    expect((await drain(app, acme)).filter((r) => r.statusCode === 201)).toHaveLength(3);
    const g = await drain(app, globex);
    expect(g.map((r) => r.statusCode)).toEqual([201, 429, 429, 429, 429]);
    expect(g[0]!.headers['ratelimit-limit']).toBe('1');
  });

  it('a queue limit (PUT settings) applies at once and only to that queue', async () => {
    const app = await startApp({ RATE_LIMIT_TENANT_BURST: '50' });
    const put = await authedInject(
      app,
      acme,
    )({
      method: 'PUT',
      url: '/queues/emails/settings',
      payload: { maxConcurrency: null, rateLimit: { perSec: 1, burst: 2 } },
    });
    expect(put.statusCode).toBe(200);
    const emails = await drain(app, acme, 'emails');
    expect(emails.map((r) => r.statusCode)).toEqual([201, 201, 429, 429, 429]);
    expect(emails[0]!.headers['ratelimit-limit']).toBe('2'); // the tighter queue bucket
    expect(emails[2]!.json()).toMatchObject({ error: { details: { scope: 'queue' } } });
    // Refusals by the queue bucket did not spend tenant tokens: 50 - 2 left.
    const reports = await enqueue(app, acme, 'reports');
    expect(reports.statusCode).toBe(201);
    expect(reports.headers).toMatchObject({ 'ratelimit-limit': '50', 'ratelimit-remaining': '47' });

    // Clearing the limit also applies at once on this instance.
    await authedInject(
      app,
      acme,
    )({
      method: 'PUT',
      url: '/queues/emails/settings',
      payload: { maxConcurrency: null, rateLimit: null },
    });
    expect((await enqueue(app, acme, 'emails')).statusCode).toBe(201);
  });

  it('every API instance spends from the same buckets', async () => {
    const prefix = uniqueRedisPrefix();
    const a = await startApp({ RATE_LIMIT_TENANT_BURST: '4' }, { prefix });
    const b = await startApp({ RATE_LIMIT_TENANT_BURST: '4' }, { prefix });
    const codes = [];
    for (let i = 0; i < 3; i++) {
      codes.push((await enqueue(a, acme)).statusCode, (await enqueue(b, acme)).statusCode);
    }
    expect(codes).toEqual([201, 201, 201, 201, 429, 429]);
  });

  it('invalid requests are rejected before spending quota; other routes are not limited', async () => {
    const app = await startApp({ RATE_LIMIT_TENANT_BURST: '1' });
    for (let i = 0; i < 3; i++) {
      const bad = await authedInject(
        app,
        acme,
      )({
        method: 'POST',
        url: '/queues/emails/jobs',
        payload: { nope: true },
      });
      expect(bad.statusCode).toBe(400);
    }
    expect((await enqueue(app, acme)).statusCode).toBe(201);
    const list = await authedInject(app, acme)({ method: 'GET', url: '/queues/emails/jobs' });
    expect(list.statusCode).toBe(200);
    expect(list.headers['ratelimit-limit']).toBeUndefined();
  });

  it('RATE_LIMIT_ENABLED=false turns limiting off', async () => {
    const app = await startApp({ RATE_LIMIT_ENABLED: 'false', RATE_LIMIT_TENANT_BURST: '1' });
    const res = await drain(app, acme);
    expect(res.every((r) => r.statusCode === 201)).toBe(true);
    expect(res[0]!.headers['ratelimit-limit']).toBeUndefined();
  });

  describe('when Redis is down', () => {
    it('fail-open (default): with Redis down enqueues go through without headers', async () => {
      const proxy = await RedisOutageProxy.start();
      cleanups.push(() => proxy.stop());
      const app = await startApp({
        REDIS_URL: proxy.url,
        RATE_LIMIT_TENANT_BURST: '1',
        REDIS_RECONNECT_BASE_MS: '50',
        REDIS_RECONNECT_MAX_MS: '100',
      });
      expect((await enqueue(app, acme)).statusCode).toBe(201);
      expect((await enqueue(app, acme)).statusCode).toBe(429);
      await proxy.cut();
      const res = await drain(app, acme);
      expect(res.every((r) => r.statusCode === 201)).toBe(true);
      expect(res[0]!.headers['ratelimit-limit']).toBeUndefined();
      // Back to limiting once Redis returns (the bucket is still empty).
      await proxy.restore();
      await waitUntil(async () => (await enqueue(app, acme)).statusCode === 429);
    });

    it('fail-closed: with Redis down enqueues get 503 + Retry-After, reads still work', async () => {
      const proxy = await RedisOutageProxy.start();
      cleanups.push(() => proxy.stop());
      const app = await startApp({ REDIS_URL: proxy.url, RATE_LIMIT_FAIL_MODE: 'closed' });
      expect((await enqueue(app, acme)).statusCode).toBe(201);
      await proxy.cut();
      const res = await enqueue(app, acme);
      expect(res.statusCode).toBe(503);
      expect(res.headers['retry-after']).toBe('1');
      expect(res.json()).toMatchObject({
        error: { code: 'SERVICE_UNAVAILABLE', details: { reason: 'rate_limit_unavailable' } },
      });
      const list = await authedInject(app, acme)({ method: 'GET', url: '/queues/emails/jobs' });
      expect(list.statusCode).toBe(200);
      expect(await jobCount(db)).toBe(1);
    });
  });
});

describeWithDb('enqueue rate limiting without Redis', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let token: string;

  beforeAll(async () => {
    db = await createTestDatabase();
    token = (await createTestApiKey(db.pool)).token;
    app = await buildApp({
      env: testEnv({ RATE_LIMIT_TENANT_BURST: '1' }),
      logger: false,
      pool: db.pool,
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  it('is off (there is nowhere to keep shared buckets): no 429, no headers', async () => {
    for (let i = 0; i < 3; i++) {
      const res = await authedInject(
        app,
        token,
      )({
        method: 'POST',
        url: '/queues/emails/jobs',
        payload: { payload: {} },
      });
      expect(res.statusCode).toBe(201);
      expect(res.headers['ratelimit-limit']).toBeUndefined();
    }
  });
});
