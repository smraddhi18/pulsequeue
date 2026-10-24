import type { QueueStats, QueueStatsResult } from '../domain/stats.js';
import { LogThrottle } from '../lib/log-throttle.js';
import type { StructuredLogger } from '../lib/logger.js';
import { SingleFlight } from '../lib/single-flight.js';

/** A cached snapshot plus when it was written (writer's wall clock, ms). */
export interface CachedQueueStats {
  stats: QueueStats;
  cachedAt: number;
}

/**
 * Storage for the stats cache (Redis in production, an in-memory fake in
 * unit tests). Every method may throw; StatsCache treats any failure as
 * "cache unavailable" and serves from Postgres.
 */
export interface StatsCacheStore {
  /** Cheap, synchronous connection check: skip the cache entirely while Redis is down. */
  isAvailable(): boolean;
  /** The entry (null when absent/expired/unreadable) and whether the queue changed since. */
  read(queue: string): Promise<{ entry: CachedQueueStats | null; dirty: boolean }>;
  /**
   * Atomically clear the dirty flag. Returns true for exactly one caller per
   * invalidation, cluster-wide: that caller recomputes, the others serve the
   * old entry. This is the cross-instance single-flight.
   */
  claimRefresh(queue: string): Promise<boolean>;
  /**
   * Compare-and-set on the snapshot time: written only if no newer snapshot
   * is stored. Returns false when a newer one won.
   */
  write(queue: string, entry: CachedQueueStats): Promise<boolean>;
}

export interface StatsCacheOptions {
  /** null = caching disabled (every read is a BYPASS). */
  store: StatsCacheStore | null;
  /** STATS_CACHE_MIN_REFRESH_MS: a dirty entry younger than this is still served. */
  minRefreshMs: number;
  logger: StructuredLogger;
  now?: () => number;
}

const ERROR_LOG_INTERVAL_MS = 10_000;

/**
 * Read-through cache policy for queue stats. Redis-agnostic; correctness
 * never depends on the store.
 *
 *   read entry + dirty flag (one round-trip)
 *   entry, clean                       -> HIT
 *   entry, dirty, younger than minRefresh -> STALE (bounds recompute rate)
 *   entry, dirty, older                -> claim (DEL dirty); winner recomputes
 *                                         (MISS), losers serve the entry (STALE)
 *   no entry                           -> recompute (MISS)
 *   store unavailable / errors         -> recompute (BYPASS)
 *
 * Recomputes are also deduplicated in-process (SingleFlight), so a cold key
 * hit by N concurrent requests costs one Postgres query per instance.
 *
 * Staleness bounds: an entry is served at most minRefreshMs after the change
 * that dirtied it, or STATS_CACHE_TTL_MS if the invalidation itself was lost.
 */
export class StatsCache {
  private readonly flights = new SingleFlight<QueueStats>();
  private readonly errors = new LogThrottle(ERROR_LOG_INTERVAL_MS);
  private readonly now: () => number;

  constructor(private readonly options: StatsCacheOptions) {
    this.now = options.now ?? Date.now;
  }

  get enabled(): boolean {
    return this.options.store !== null;
  }

  async get(queue: string, compute: () => Promise<QueueStats>): Promise<QueueStatsResult> {
    const { store, logger } = this.options;
    if (!store || !store.isAvailable()) {
      const stats = await this.flights.run(queue, compute);
      logger.debug(
        { event: 'stats.cache.bypass', queue, reason: store ? 'unavailable' : 'disabled' },
        'Stats served from Postgres (cache bypassed)',
      );
      return { stats, cache: 'bypass' };
    }

    let read: { entry: CachedQueueStats | null; dirty: boolean };
    try {
      read = await store.read(queue);
    } catch (err) {
      this.logError(queue, 'read', err);
      return { stats: await this.flights.run(queue, compute), cache: 'bypass' };
    }

    const { entry, dirty } = read;
    if (entry && !dirty) {
      logger.debug({ event: 'stats.cache.hit', queue }, 'Stats cache hit');
      return { stats: entry.stats, cache: 'hit' };
    }

    if (entry) {
      const ageMs = this.now() - entry.cachedAt;
      if (ageMs < this.options.minRefreshMs) return this.stale(queue, entry, 'min_refresh');
      let claimed: boolean;
      try {
        claimed = await store.claimRefresh(queue);
      } catch (err) {
        this.logError(queue, 'claim', err);
        return this.stale(queue, entry, 'claim_failed');
      }
      if (!claimed) return this.stale(queue, entry, 'refresh_in_progress');
    }
    let fresh = entry !== null; // reaching here with an entry means we won the claim
    if (!entry && dirty) {
      // Clear a leftover dirty flag *before* the snapshot: any change after
      // this point sets it again and dirties the entry we are about to write.
      fresh = await store.claimRefresh(queue).catch(() => false);
    }

    // The caller that cleared the flag must snapshot after the clear: joining
    // a computation that began earlier could cache data that misses the very
    // change that dirtied the entry, and mark it clean. Everyone else joins
    // whatever is in flight (on a cold, dirty key that is the claimer's fresh
    // computation), so N concurrent cold reads still cost one query.
    const stats = await this.flights.run(queue, compute, { fresh });
    try {
      const written = await store.write(queue, { stats, cachedAt: this.now() });
      logger.debug(
        { event: 'stats.cache.miss', queue, written, refresh: entry !== null },
        'Stats computed and cached',
      );
    } catch (err) {
      this.logError(queue, 'write', err);
    }
    return { stats, cache: 'miss' };
  }

  private stale(queue: string, entry: CachedQueueStats, reason: string): QueueStatsResult {
    this.options.logger.debug(
      { event: 'stats.cache.stale', queue, reason, ageMs: this.now() - entry.cachedAt },
      'Serving stale stats',
    );
    return { stats: entry.stats, cache: 'stale' };
  }

  private logError(queue: string, operation: string, err: unknown): void {
    const decision = this.errors.check(operation);
    if (!decision.log) return;
    this.options.logger.warn(
      { event: 'stats.cache.error', queue, operation, err, suppressed: decision.suppressed },
      'Stats cache unavailable; serving from Postgres',
    );
  }
}
