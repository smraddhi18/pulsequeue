import pino from 'pino';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, expect, it } from 'vitest';
import type { RateLimit, TenantId } from '../../src/domain/tenant.js';
import {
  closeRedisClient,
  createRedisClient,
  RedisRateLimitStore,
  redisKeys,
  type RedisClient,
} from '../../src/redis/index.js';
import type { BucketRequest } from '../../src/services/rate-limiter.js';
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
const acme = 'acme' as TenantId;
const globex = 'globex' as TenantId;
const initech = 'initech' as TenantId;

const tenantBucket = (limit: RateLimit): BucketRequest => ({ scope: 'tenant', limit });
const queueBucket = (limit: RateLimit): BucketRequest => ({ scope: 'queue', limit });

describeWithRedis('token bucket script (real Redis)', { timeout: 20_000 }, () => {
  const prefix = uniqueRedisPrefix();
  const keys = redisKeys(prefix);
  let admin: Redis;
  const cleanups: (() => Promise<unknown>)[] = [];

  async function store(url = requireRedisUrl()): Promise<RedisRateLimitStore> {
    const c: RedisClient = createRedisClient(
      {
        REDIS_URL: url,
        REDIS_CONNECT_TIMEOUT_MS: 500,
        REDIS_COMMAND_TIMEOUT_MS: 500,
        REDIS_RECONNECT_BASE_MS: 20,
        REDIS_RECONNECT_MAX_MS: 100,
      },
      { logger, name: 'pulsequeue:test:ratelimit' },
    );
    cleanups.push(() => closeRedisClient(c));
    await waitUntil(() => c.status === 'ready');
    return new RedisRateLimitStore({ client: c, keys });
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

  it('a new bucket is full: the burst passes, the next request waits ~1/rate', async () => {
    const s = await store();
    const limit = { perSec: 5, burst: 3 };
    const results = [];
    for (let i = 0; i < 4; i++) results.push(await s.take(acme, 'q', [tenantBucket(limit)]));
    expect(results.map((r) => r.allowed)).toEqual([true, true, true, false]);
    expect(results.map((r) => r.buckets[0]!.remaining)).toEqual([2, 1, 0, 0]);
    const refused = results[3]!;
    expect(refused.retryAfterMs).toBeGreaterThan(100);
    expect(refused.retryAfterMs).toBeLessThanOrEqual(200);
    // The key lives until the bucket would be full again (plus slack), not forever.
    const ttl = await admin.pttl(keys.rateLimitTenant('acme'));
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(600 + 1_000);
  });

  it('refills on the Redis clock, never beyond the burst', async () => {
    const s = await store();
    const limit = { perSec: 50, burst: 2 };
    await s.take(acme, 'q', [tenantBucket(limit)]);
    await s.take(acme, 'q', [tenantBucket(limit)]);
    expect((await s.take(acme, 'q', [tenantBucket(limit)])).allowed).toBe(false);
    await sleep(200); // 10 tokens worth of time, capped at 2
    const after = [];
    for (let i = 0; i < 3; i++)
      after.push((await s.take(acme, 'q', [tenantBucket(limit)])).allowed);
    expect(after).toEqual([true, true, false]);
  });

  it('is all-or-nothing across the tenant and queue buckets', async () => {
    const s = await store();
    const tenant = tenantBucket({ perSec: 1, burst: 10 });
    const queue = queueBucket({ perSec: 1, burst: 1 });
    const first = await s.take(acme, 'emails', [tenant, queue]);
    expect(first.allowed).toBe(true);
    expect(first.buckets.map((b) => b.remaining)).toEqual([9, 0]);
    const second = await s.take(acme, 'emails', [tenant, queue]);
    expect(second.allowed).toBe(false);
    expect(second.buckets.map((b) => b.remaining)).toEqual([9, 0]);
    // The refused request did not spend tenant quota; another queue still gets it.
    const other = await s.take(acme, 'reports', [tenant]);
    expect(other.buckets[0]!.remaining).toBe(8);
  });

  it('keeps tenants and queues apart', async () => {
    const s = await store();
    const limit = { perSec: 1, burst: 1 };
    expect((await s.take(acme, 'q', [tenantBucket(limit)])).allowed).toBe(true);
    expect((await s.take(acme, 'q', [tenantBucket(limit)])).allowed).toBe(false);
    expect((await s.take(globex, 'q', [tenantBucket(limit)])).allowed).toBe(true);
    const big = tenantBucket({ perSec: 100, burst: 100 });
    // Queue buckets: per queue within a tenant.
    expect((await s.take(initech, 'a', [big, queueBucket(limit)])).allowed).toBe(true);
    expect((await s.take(initech, 'b', [big, queueBucket(limit)])).allowed).toBe(true);
    expect((await s.take(initech, 'a', [big, queueBucket(limit)])).allowed).toBe(false);
  });

  it('a lowered burst applies immediately (stored tokens are clamped)', async () => {
    const s = await store();
    await s.take(acme, 'q', [tenantBucket({ perSec: 1, burst: 100 })]);
    const r = await s.take(acme, 'q', [tenantBucket({ perSec: 1, burst: 2 })]);
    expect(r.buckets[0]!.remaining).toBe(1);
  });

  it('never over-admits under concurrency from several connections', async () => {
    const stores = await Promise.all(Array.from({ length: 4 }, () => store()));
    const limit = { perSec: 1, burst: 25 };
    const results = await Promise.all(
      Array.from({ length: 200 }, (_, i) =>
        stores[i % stores.length]!.take(acme, 'q', [tenantBucket(limit)]),
      ),
    );
    expect(results.filter((r) => r.allowed)).toHaveLength(25);
  });

  it('reports unavailable and fails fast while Redis is unreachable', async () => {
    const proxy = await RedisOutageProxy.start();
    cleanups.push(() => proxy.stop());
    const s = await store(proxy.url);
    expect(s.isAvailable()).toBe(true);
    await proxy.cut();
    await waitUntil(() => !s.isAvailable());
    const started = Date.now();
    await expect(s.take(acme, 'q', [tenantBucket({ perSec: 1, burst: 1 })])).rejects.toThrow();
    expect(Date.now() - started).toBeLessThan(600);
  });
});
