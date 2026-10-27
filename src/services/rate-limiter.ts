import type { RateLimit, TenantId } from '../domain/tenant.js';
import { LogThrottle } from '../lib/log-throttle.js';
import type { StructuredLogger } from '../lib/logger.js';
import { TtlCache } from '../lib/ttl-cache.js';

export type RateLimitScope = 'tenant' | 'queue';

/** One bucket to take a token from. */
export interface BucketRequest {
  scope: RateLimitScope;
  limit: RateLimit;
}

/** A bucket after the attempt (or as it is, when the attempt was refused). */
export interface BucketState {
  scope: RateLimitScope;
  limit: RateLimit;
  /** Whole tokens left. */
  remaining: number;
  /** Time until the bucket is full again. */
  fullInMs: number;
}

export interface TakeResult {
  allowed: boolean;
  /** 0 when allowed; otherwise when the refusing bucket(s) hold a token again. */
  retryAfterMs: number;
  /** Same order as the request. */
  buckets: BucketState[];
}

/**
 * Shared bucket storage (Redis in production, a fake in unit tests). `take`
 * is all-or-nothing: a token is taken from every bucket or from none, so a
 * request refused by its queue bucket does not also spend tenant quota.
 * Every method may throw.
 */
export interface RateLimitStore {
  /** Cheap, synchronous connection check. */
  isAvailable(): boolean;
  take(tenantId: TenantId, queue: string, buckets: readonly BucketRequest[]): Promise<TakeResult>;
}

/** What the headers report: the bucket closest to refusing. */
export interface RateLimitInfo {
  scope: RateLimitScope;
  limit: number;
  remaining: number;
  resetMs: number;
}

export type RateLimitDecision =
  | { outcome: 'allowed'; info: RateLimitInfo }
  | { outcome: 'limited'; info: RateLimitInfo; retryAfterMs: number }
  /** Not limited: limiting is off, or the store failed and the mode is open. */
  | { outcome: 'skipped'; reason: 'disabled' | 'unavailable' }
  /** The store failed and the mode is closed: the caller must refuse (503). */
  | { outcome: 'unavailable' };

export interface RateLimiterOptions {
  /** null = rate limiting disabled (RATE_LIMIT_ENABLED=false or no REDIS_URL). */
  store: RateLimitStore | null;
  failMode: 'open' | 'closed';
  /** RATE_LIMIT_TENANT_*: for tenants without their own limit. */
  defaultTenantLimit: RateLimit;
  /** A queue's own limit (null = only the tenant bucket applies). */
  loadQueueLimit: (tenantId: TenantId, queue: string) => Promise<RateLimit | null>;
  /** RATE_LIMIT_SETTINGS_CACHE_TTL_MS. */
  settingsCacheTtlMs: number;
  settingsCacheMaxEntries?: number;
  logger: StructuredLogger;
  now?: () => number;
}

const UNAVAILABLE_LOG_INTERVAL_MS = 10_000;

/**
 * Enqueue rate limiting (M6): token buckets per tenant and, optionally, per
 * queue, kept in Redis so every API instance spends from the same buckets.
 *
 * The policy lives here (which buckets, what to do when Redis fails); the
 * atomic arithmetic lives in the store. Redis trouble never turns into a
 * 500: in open mode (default) the request goes through unlimited and a
 * throttled `ratelimit.unavailable` warning is logged; in closed mode the
 * caller answers 503 so a hard quota is never exceeded.
 */
export class RateLimiter {
  private readonly queueLimits: TtlCache<string, RateLimit | null>;
  private readonly unavailable: LogThrottle;

  constructor(private readonly options: RateLimiterOptions) {
    this.queueLimits = new TtlCache({
      ttlMs: options.settingsCacheTtlMs,
      maxEntries: options.settingsCacheMaxEntries ?? 10_000,
      ...(options.now ? { now: options.now } : {}),
    });
    this.unavailable = new LogThrottle(UNAVAILABLE_LOG_INTERVAL_MS, options.now);
  }

  get enabled(): boolean {
    return this.options.store !== null;
  }

  /**
   * Take one enqueue token for `queue` of `tenantId`. `tenantLimit` is the
   * tenant's own limit from its row (null = the defaults).
   */
  async consume(
    tenantId: TenantId,
    queue: string,
    tenantLimit: RateLimit | null,
  ): Promise<RateLimitDecision> {
    const { store } = this.options;
    if (!store) return { outcome: 'skipped', reason: 'disabled' };
    if (!store.isAvailable()) return this.storeFailed(tenantId, 'not_connected');

    const buckets: BucketRequest[] = [
      { scope: 'tenant', limit: tenantLimit ?? this.options.defaultTenantLimit },
    ];
    const queueLimit = await this.queueLimit(tenantId, queue);
    if (queueLimit) buckets.push({ scope: 'queue', limit: queueLimit });

    let result: TakeResult;
    try {
      result = await store.take(tenantId, queue, buckets);
    } catch (err) {
      return this.storeFailed(tenantId, 'error', err);
    }

    if (result.allowed) return { outcome: 'allowed', info: binding(result.buckets) };
    // Report the bucket that refused (the one that needs the longest wait).
    const refused = result.buckets.filter((b) => b.remaining < 1);
    return {
      outcome: 'limited',
      info: binding(refused.length > 0 ? refused : result.buckets),
      retryAfterMs: result.retryAfterMs,
    };
  }

  /** Forget a cached queue limit (after PUT /queues/:name/settings on this instance). */
  forgetQueue(tenantId: TenantId, queue: string): void {
    this.queueLimits.delete(cacheKey(tenantId, queue));
  }

  private async queueLimit(tenantId: TenantId, queue: string): Promise<RateLimit | null> {
    const key = cacheKey(tenantId, queue);
    const cached = this.queueLimits.get(key);
    if (cached !== undefined) return cached;
    const limit = await this.options.loadQueueLimit(tenantId, queue);
    this.queueLimits.set(key, limit);
    return limit;
  }

  private storeFailed(tenantId: TenantId, cause: string, err?: unknown): RateLimitDecision {
    const { failMode, logger } = this.options;
    const decision = this.unavailable.check('store');
    if (decision.log) {
      logger.warn(
        {
          event: 'ratelimit.unavailable',
          failMode,
          cause,
          tenantId,
          suppressed: decision.suppressed,
          ...(err instanceof Error ? { err: { message: err.message } } : {}),
        },
        failMode === 'open'
          ? 'Rate limit store unavailable: enqueues are not limited'
          : 'Rate limit store unavailable: enqueues are refused',
      );
    }
    return failMode === 'open'
      ? { outcome: 'skipped', reason: 'unavailable' }
      : { outcome: 'unavailable' };
  }
}

function cacheKey(tenantId: TenantId, queue: string): string {
  return `${tenantId}\u0000${queue}`;
}

/** The bucket with the fewest tokens left (ties: the longer reset). */
function binding(buckets: readonly BucketState[]): RateLimitInfo {
  let best = buckets[0];
  if (!best) throw new Error('rate limit result without buckets');
  for (const b of buckets) {
    const waitsLonger = b.remaining === best.remaining && b.fullInMs > best.fullInMs;
    if (b.remaining < best.remaining || waitsLonger) best = b;
  }
  return {
    scope: best.scope,
    limit: best.limit.burst,
    remaining: Math.max(0, best.remaining),
    resetMs: best.fullInMs,
  };
}
