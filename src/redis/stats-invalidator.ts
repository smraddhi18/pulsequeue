import type { StructuredLogger } from '../lib/logger.js';
import { LogThrottle } from '../lib/log-throttle.js';
import { isRedisReady, type RedisClient } from './client.js';
import { KeyedCoalescer } from './coalescer.js';
import type { RedisKeys } from './keys.js';

export interface StatsInvalidatorOptions {
  client: RedisClient;
  keys: RedisKeys;
  /** Dirty flags expire with the entries they refer to (STATS_CACHE_TTL_MS). */
  ttlMs: number;
  /** STATS_INVALIDATION_COALESCE_MS. */
  coalesceMs: number;
  logger: StructuredLogger;
}

const FAILURE_LOG_INTERVAL_MS = 10_000;

/**
 * Marks a queue's cached stats dirty after a write committed. Writers never
 * delete or recompute the entry themselves: "delete after commit" has a
 * classic race (a reader that computed before the commit writes its old
 * result back after the delete), and recompute-on-write would put stats
 * queries on the enqueue hot path. Setting a flag is O(1), idempotent and
 * coalesced per queue; the next reader decides when to recompute.
 *
 * A lost invalidation (Redis down, process crashed between commit and SET)
 * is bounded by the entry TTL.
 */
export class StatsInvalidator {
  private readonly coalescer: KeyedCoalescer;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly failures = new LogThrottle(FAILURE_LOG_INTERVAL_MS);

  constructor(private readonly options: StatsInvalidatorOptions) {
    this.coalescer = new KeyedCoalescer(options.coalesceMs, (queue) => this.markDirty(queue));
  }

  invalidate(queue: string): void {
    this.coalescer.trigger(queue);
  }

  async flush(): Promise<void> {
    this.coalescer.flush();
    await Promise.allSettled([...this.inFlight]);
  }

  private markDirty(queue: string): void {
    const { client, keys, ttlMs, logger } = this.options;
    if (!isRedisReady(client)) {
      this.logFailure(queue, new Error(`Redis is not connected (status ${client.status})`));
      return;
    }
    const promise = client
      .set(keys.statsDirty(queue), '1', 'PX', ttlMs)
      .then(() => {
        logger.debug({ event: 'stats.cache.invalidated', queue }, 'Stats cache invalidated');
      })
      .catch((err: unknown) => this.logFailure(queue, err))
      .finally(() => this.inFlight.delete(promise));
    this.inFlight.add(promise);
  }

  private logFailure(queue: string, err: unknown): void {
    const decision = this.failures.check('invalidate');
    if (!decision.log) return;
    this.options.logger.warn(
      { event: 'stats.cache.invalidate_failed', queue, err, suppressed: decision.suppressed },
      'Could not invalidate cached stats; the entry expires within its TTL',
    );
  }
}
