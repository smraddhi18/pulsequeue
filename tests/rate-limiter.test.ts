import { describe, expect, it, vi } from 'vitest';
import type { RateLimit, TenantId } from '../src/domain/tenant.js';
import {
  RateLimiter,
  type BucketRequest,
  type RateLimiterOptions,
  type RateLimitStore,
  type TakeResult,
} from '../src/services/rate-limiter.js';

const tenant = 'acme' as TenantId;
const defaults: RateLimit = { perSec: 10, burst: 20 };

/** In-memory token buckets with a manual clock: the same arithmetic as the Lua script. */
class FakeStore implements RateLimitStore {
  available = true;
  fail: Error | null = null;
  readonly calls: { queue: string; buckets: BucketRequest[] }[] = [];
  private readonly levels = new Map<string, { level: number; at: number }>();

  constructor(private readonly clock: { now: number }) {}

  isAvailable(): boolean {
    return this.available;
  }

  async take(t: TenantId, queue: string, buckets: readonly BucketRequest[]): Promise<TakeResult> {
    this.calls.push({ queue, buckets: [...buckets] });
    if (this.fail) throw this.fail;
    const keys = buckets.map((b) => (b.scope === 'tenant' ? t : `${t}/${queue}`));
    const levels = buckets.map((b, i) => {
      const s = this.levels.get(keys[i]!);
      if (!s) return b.limit.burst;
      return Math.min(b.limit.burst, s.level + ((this.clock.now - s.at) * b.limit.perSec) / 1000);
    });
    const allowed = levels.every((l) => l >= 1);
    const retryAfterMs = allowed
      ? 0
      : Math.max(
          ...levels.map((l, i) =>
            l >= 1 ? 0 : Math.ceil(((1 - l) * 1000) / buckets[i]!.limit.perSec),
          ),
        );
    return {
      allowed,
      retryAfterMs,
      buckets: buckets.map((b, i) => {
        const level = allowed ? levels[i]! - 1 : levels[i]!;
        if (allowed) this.levels.set(keys[i]!, { level, at: this.clock.now });
        return {
          scope: b.scope,
          limit: b.limit,
          remaining: Math.floor(level),
          fullInMs: Math.ceil(((b.limit.burst - level) * 1000) / b.limit.perSec),
        };
      }),
    };
  }
}

function setup(overrides: Partial<RateLimiterOptions> = {}, queueLimit: RateLimit | null = null) {
  const clock = { now: 1_000_000 };
  const store = new FakeStore(clock);
  const logger = { warn: vi.fn(), info: vi.fn(), debug: vi.fn(), error: vi.fn() };
  const loadQueueLimit = vi.fn(async () => queueLimit);
  const limiter = new RateLimiter({
    store,
    failMode: 'open',
    defaultTenantLimit: defaults,
    loadQueueLimit,
    settingsCacheTtlMs: 5_000,
    logger: logger as unknown as RateLimiterOptions['logger'],
    now: () => clock.now,
    ...overrides,
  });
  return { limiter, store, clock, logger, loadQueueLimit };
}

describe('RateLimiter', () => {
  it('allows a burst up to the tenant capacity, then limits with a retry hint', async () => {
    const { limiter } = setup();
    for (let i = 0; i < 20; i++) {
      expect((await limiter.consume(tenant, 'q', null)).outcome).toBe('allowed');
    }
    const limited = await limiter.consume(tenant, 'q', null);
    expect(limited).toEqual({
      outcome: 'limited',
      retryAfterMs: 100, // 10/s: the next token in 100 ms
      info: { scope: 'tenant', limit: 20, remaining: 0, resetMs: 2_000 },
    });
  });

  it('refills over time', async () => {
    const { limiter, clock } = setup();
    for (let i = 0; i < 20; i++) await limiter.consume(tenant, 'q', null);
    clock.now += 350; // 3.5 tokens
    const outcomes = [];
    for (let i = 0; i < 4; i++) outcomes.push((await limiter.consume(tenant, 'q', null)).outcome);
    expect(outcomes).toEqual(['allowed', 'allowed', 'allowed', 'limited']);
  });

  it("uses the tenant's own limit over the defaults", async () => {
    const { limiter, store } = setup();
    const own = { perSec: 1, burst: 2 };
    const first = await limiter.consume(tenant, 'q', own);
    expect(first).toMatchObject({ outcome: 'allowed', info: { limit: 2, remaining: 1 } });
    expect(store.calls[0]?.buckets).toEqual([{ scope: 'tenant', limit: own }]);
  });

  it('adds the queue bucket when the queue has a limit and reports the tighter one', async () => {
    const { limiter, store } = setup({}, { perSec: 1, burst: 3 });
    const first = await limiter.consume(tenant, 'q', null);
    expect(store.calls[0]?.buckets.map((b) => b.scope)).toEqual(['tenant', 'queue']);
    expect(first).toMatchObject({
      outcome: 'allowed',
      info: { scope: 'queue', limit: 3, remaining: 2 },
    });
    await limiter.consume(tenant, 'q', null);
    await limiter.consume(tenant, 'q', null);
    expect(await limiter.consume(tenant, 'q', null)).toMatchObject({
      outcome: 'limited',
      retryAfterMs: 1_000,
      info: { scope: 'queue', remaining: 0 },
    });
  });

  it('caches queue limits for the TTL; forgetQueue drops the entry', async () => {
    const { limiter, loadQueueLimit, clock } = setup({}, null);
    await limiter.consume(tenant, 'q', null);
    await limiter.consume(tenant, 'q', null);
    expect(loadQueueLimit).toHaveBeenCalledTimes(1);
    limiter.forgetQueue(tenant, 'q');
    await limiter.consume(tenant, 'q', null);
    expect(loadQueueLimit).toHaveBeenCalledTimes(2);
    clock.now += 5_000;
    await limiter.consume(tenant, 'q', null);
    expect(loadQueueLimit).toHaveBeenCalledTimes(3);
    // Per tenant and queue.
    await limiter.consume('globex' as TenantId, 'q', null);
    expect(loadQueueLimit).toHaveBeenLastCalledWith('globex', 'q');
  });

  it('is skipped entirely without a store', async () => {
    const { limiter, loadQueueLimit } = setup({ store: null });
    expect(limiter.enabled).toBe(false);
    expect(await limiter.consume(tenant, 'q', null)).toEqual({
      outcome: 'skipped',
      reason: 'disabled',
    });
    expect(loadQueueLimit).not.toHaveBeenCalled();
  });

  describe('when the store fails', () => {
    it('open mode lets the request through and logs ratelimit.unavailable (throttled)', async () => {
      const { limiter, store, logger, clock } = setup();
      store.fail = new Error('Command timed out');
      for (let i = 0; i < 5; i++) {
        expect(await limiter.consume(tenant, 'q', null)).toEqual({
          outcome: 'skipped',
          reason: 'unavailable',
        });
      }
      expect(logger.warn).toHaveBeenCalledTimes(1);
      expect(logger.warn.mock.calls[0]?.[0]).toMatchObject({
        event: 'ratelimit.unavailable',
        failMode: 'open',
        cause: 'error',
        err: { message: 'Command timed out' },
      });
      clock.now += 10_000;
      await limiter.consume(tenant, 'q', null);
      expect(logger.warn).toHaveBeenCalledTimes(2);
      expect(logger.warn.mock.calls[1]?.[0]).toMatchObject({ suppressed: 4 });
    });

    it('closed mode refuses', async () => {
      const { limiter, store, logger } = setup({ failMode: 'closed' });
      store.fail = new Error('boom');
      expect(await limiter.consume(tenant, 'q', null)).toEqual({ outcome: 'unavailable' });
      expect(logger.warn.mock.calls[0]?.[0]).toMatchObject({ failMode: 'closed' });
    });

    it('does not call the store while it is disconnected', async () => {
      const { limiter, store, loadQueueLimit } = setup({ failMode: 'closed' });
      store.available = false;
      expect(await limiter.consume(tenant, 'q', null)).toEqual({ outcome: 'unavailable' });
      expect(store.calls).toHaveLength(0);
      expect(loadQueueLimit).not.toHaveBeenCalled();
    });

    it('propagates a failing queue-limit lookup (Postgres is the source of truth)', async () => {
      const { limiter } = setup({ loadQueueLimit: async () => Promise.reject(new Error('pg')) });
      await expect(limiter.consume(tenant, 'q', null)).rejects.toThrow('pg');
    });
  });
});
