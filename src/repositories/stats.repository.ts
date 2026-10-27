import type { QueueName } from '../domain/job.js';
import { STATS_STATUSES, type QueueStats, type QueueStatusCounts } from '../domain/stats.js';
import { runQuery } from '../db/query.js';
import type { TenantId } from '../domain/tenant.js';
import type { Queryable } from '../db/pool.js';

interface StatsRow {
  name: string;
  counts: Record<string, number | string> | null;
  oldest_queued_at: Date | null;
  next_scheduled_at: Date | null;
  oldest_running_since: Date | null;
  computed_at: Date;
}

/**
 * Read-only aggregate queries for GET /queues/:name/stats.
 *
 * One statement, so every number comes from the same MVCC snapshot (counts
 * and ages are mutually consistent) and `now()` is the snapshot's clock.
 * Each aggregate is a correlated subquery so the planner can use the right
 * index for it:
 * - counts: jobs_queue_status_created_idx (queue_id, status, ...), an
 *   index(-only) scan grouped by status;
 * - oldest queued: jobs_ready_idx (partial, status = 'queued');
 * - next scheduled: jobs_scheduled_due_idx (queue_id, run_at), one row;
 * - oldest running: running rows are bounded by total worker concurrency.
 *
 * The count is still O(jobs in the queue): that is the cost the Redis cache
 * amortizes. (Approximate counts from pg_class.reltuples are table-wide, not
 * per queue, so they do not help here.)
 */
export class QueueStatsRepository {
  constructor(private readonly db: Queryable) {}

  /** Snapshot for one queue, or null when the queue does not exist. */
  async compute(tenantId: TenantId, name: QueueName): Promise<QueueStats | null> {
    const rows = await runQuery<StatsRow>(
      this.db,
      `SELECT q.name,
              (SELECT json_object_agg(s.status, s.n)
               FROM (SELECT status::text AS status, count(*) AS n
                     FROM jobs
                     WHERE queue_id = q.id
                     GROUP BY status) s) AS counts,
              (SELECT min(run_at) FROM jobs
               WHERE queue_id = q.id AND status = 'queued') AS oldest_queued_at,
              (SELECT min(run_at) FROM jobs
               WHERE queue_id = q.id AND status = 'scheduled') AS next_scheduled_at,
              (SELECT min(locked_at) FROM jobs
               WHERE queue_id = q.id AND status = 'running') AS oldest_running_since,
              now() AS computed_at
       FROM queues q
       WHERE q.tenant_id = $1 AND q.name = $2`,
      [tenantId, name],
    );
    const row = rows[0];
    if (!row) return null;
    const counts = {} as QueueStatusCounts;
    let total = 0;
    for (const status of STATS_STATUSES) {
      const n = Number(row.counts?.[status] ?? 0);
      counts[status] = n;
      total += n;
    }
    return {
      queue: row.name as QueueName,
      counts,
      total,
      oldestQueuedAt: row.oldest_queued_at,
      nextScheduledAt: row.next_scheduled_at,
      oldestRunningSince: row.oldest_running_since,
      computedAt: row.computed_at,
    };
  }
}
