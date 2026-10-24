import type { QueueName } from './job.js';

/**
 * Queue statistics (Milestone 5): a point-in-time snapshot of one queue,
 * computed by Postgres in a single statement and cached in Redis.
 */

/** Statuses reported in stats; the legacy pre-M3 `failed` status can no longer occur. */
export const STATS_STATUSES = ['queued', 'scheduled', 'running', 'succeeded', 'dead'] as const;
export type StatsStatus = (typeof STATS_STATUSES)[number];

export type QueueStatusCounts = Record<StatsStatus, number>;

export interface QueueStats {
  queue: QueueName;
  counts: QueueStatusCounts;
  /** Sum of counts. */
  total: number;
  /** run_at of the oldest runnable job: how long the head of the backlog has been waiting. */
  oldestQueuedAt: Date | null;
  /** Earliest run_at among delayed / retrying jobs. */
  nextScheduledAt: Date | null;
  /** locked_at of the longest-running job (spots stuck handlers before the reaper does). */
  oldestRunningSince: Date | null;
  /**
   * Database clock at the snapshot. Ages in the response are relative to it
   * (so they are consistent within one snapshot), and the cache orders writes
   * by it so an older snapshot never overwrites a newer one.
   */
  computedAt: Date;
}

/**
 * How a stats read was served (also the x-cache response header):
 * - hit: fresh cache entry, no invalidation since it was computed
 * - stale: entry invalidated but younger than the refresh interval, or
 *   another instance is already recomputing it
 * - miss: computed from Postgres and written to the cache
 * - bypass: cache disabled or Redis unavailable; computed from Postgres
 */
export const STATS_CACHE_STATUSES = ['hit', 'stale', 'miss', 'bypass'] as const;
export type StatsCacheStatus = (typeof STATS_CACHE_STATUSES)[number];

export interface QueueStatsResult {
  stats: QueueStats;
  cache: StatsCacheStatus;
}
