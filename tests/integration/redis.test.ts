import pino from 'pino';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, expect, it } from 'vitest';
import { loadWorkerEnv, type RedisEnv } from '../../src/config/env.js';
import {
  closeRedisClient,
  createRedisClient,
  createRedisServices,
  pingRedis,
  redisKeys,
  RedisStatsCacheStore,
  WakeupSubscriber,
  type RedisClient,
} from '../../src/redis/index.js';
import { makeStats } from '../support/stats.js';
import {
  createAdminRedis,
  deleteRedisPrefix,
  describeWithRedis,
  RedisOutageProxy,
  requireRedisUrl,
  uniqueRedisPrefix,
  waitUntil,
} from './helpers.js';

const logger = pino({ level: 'silent' });
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describeWithRedis('Redis building blocks (real Redis)', { timeout: 20_000 }, () => {
  const prefix = uniqueRedisPrefix();
  const keys = redisKeys(prefix);
  let admin: Redis;
  const cleanups: (() => Promise<unknown>)[] = [];

  /** Redis settings as a worker would load them (the DB URL is never used here). */
  function redisEnv(url = requireRedisUrl(), overrides: Record<string, string> = {}): RedisEnv {
    return loadWorkerEnv({
      DATABASE_URL: 'postgres://unused@localhost/unused',
      REDIS_URL: url,
      REDIS_KEY_PREFIX: prefix,
      REDIS_RECONNECT_BASE_MS: '20',
      REDIS_RECONNECT_MAX_MS: '100',
      REDIS_CONNECT_TIMEOUT_MS: '500',
      ...overrides,
    });
  }
  const settings = (url = requireRedisUrl()) => ({ ...redisEnv(url), REDIS_URL: url });

  function client(url?: string): RedisClient {
    const c = createRedisClient(settings(url), { logger, name: 'pulsequeue:test' });
    cleanups.push(() => closeRedisClient(c));
    return c;
  }

  beforeAll(() => {
    admin = createAdminRedis();
  });

  afterEach(async () => {
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
    await deleteRedisPrefix(admin, prefix);
  });

  afterAll(async () => {
    await admin?.quit();
  });

  it('pingRedis answers once connected and names the connection', async () => {
    const c = client();
    await waitUntil(() => c.status === 'ready');
    await expect(pingRedis(c)).resolves.toBeUndefined();
    const list = await admin.client('LIST');
    expect(String(list)).toContain('name=pulsequeue:test');
  });

  it('commands fail fast (no offline queue) while disconnected', async () => {
    const c = client('redis://127.0.0.1:1');
    await expect(pingRedis(c)).rejects.toThrow(/not connected/);
    const started = Date.now();
    await expect(c.get('x')).rejects.toThrow();
    expect(Date.now() - started).toBeLessThan(200);
  });

  it('a published wake-up reaches the subscriber of that queue only, coalesced', async () => {
    const services = createRedisServices(redisEnv(), {
      logger,
      role: 'api',
      id: 'test',
    });
    cleanups.push(() => services.close());
    const received: string[] = [];
    const listening: boolean[] = [];
    const sub = new WakeupSubscriber({
      client: client(),
      keys,
      queues: ['emails'],
      logger,
      onWake: (q) => received.push(q),
      onListeningChange: (l) => listening.push(l),
    });
    cleanups.push(() => sub.close());
    sub.start();
    await waitUntil(() => sub.isListening);
    expect(listening).toEqual([true]);
    await waitUntil(() => services.client!.status === 'ready');

    // 50 signals inside one window: leading + trailing publish only.
    for (let i = 0; i < 50; i++) services.signals.jobsRunnable('emails');
    services.signals.jobsRunnable('reports'); // not subscribed
    await services.close();
    await waitUntil(() => received.length >= 2);
    await sleep(50);
    expect(received).toEqual(['emails', 'emails']);
  });

  it('jobsChanged marks stats dirty with a TTL but publishes nothing', async () => {
    const services = createRedisServices(
      redisEnv(requireRedisUrl(), { STATS_CACHE_TTL_MS: '5000' }),
      { logger, role: 'worker', id: 'test' },
    );
    cleanups.push(() => services.close());
    await waitUntil(() => services.client!.status === 'ready');
    services.signals.jobsChanged('emails');
    await services.close();
    const ttl = await admin.pttl(keys.statsDirty('emails'));
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(5_000);
  });

  it('stats store: read, claim once, CAS write rejects an older snapshot, TTL set', async () => {
    const c = client();
    await waitUntil(() => c.status === 'ready');
    const store = new RedisStatsCacheStore({ client: c, keys, ttlMs: 3_000 });
    expect(store.isAvailable()).toBe(true);
    expect(await store.read('emails')).toEqual({ entry: null, dirty: false });

    const newer = makeStats({ computedAt: new Date('2026-10-24T10:02:00Z') });
    const older = makeStats({
      computedAt: new Date('2026-10-24T10:01:00Z'),
      counts: { queued: 99, scheduled: 0, running: 0, succeeded: 0, dead: 0 },
      total: 99,
    });
    expect(await store.write('emails', { stats: newer, cachedAt: 1 })).toBe(true);
    expect(await store.write('emails', { stats: older, cachedAt: 2 })).toBe(false);
    const read = await store.read('emails');
    expect(read.entry?.stats).toEqual(newer);
    expect(await admin.pttl(keys.statsEntry('emails'))).toBeGreaterThan(2_000);

    await admin.set(keys.statsDirty('emails'), '1');
    expect((await store.read('emails')).dirty).toBe(true);
    const claims = await Promise.all([1, 2, 3, 4, 5].map(() => store.claimRefresh('emails')));
    expect(claims.filter(Boolean)).toHaveLength(1);
  });

  it('subscriber falls back on an outage and re-subscribes after it (catch-up signal)', async () => {
    const proxy = await RedisOutageProxy.start();
    cleanups.push(() => proxy.stop());
    const listening: boolean[] = [];
    const received: string[] = [];
    const sub = new WakeupSubscriber({
      client: client(proxy.url),
      keys,
      queues: ['emails'],
      logger,
      onWake: (q) => received.push(q),
      onListeningChange: (l) => listening.push(l),
    });
    cleanups.push(() => sub.close());
    sub.start();
    await waitUntil(() => sub.isListening);

    await proxy.cut();
    await waitUntil(() => !sub.isListening);
    expect(listening).toEqual([true, false]);

    await proxy.restore();
    await waitUntil(() => sub.isListening, { timeoutMs: 5_000 });
    expect(listening).toEqual([true, false, true]);

    // The re-subscription really is live.
    await waitUntil(async () => {
      await admin.publish(keys.wakeupChannel('emails'), '{}');
      return received.length > 0;
    });
  });
});
