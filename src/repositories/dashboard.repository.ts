import type { Queryable } from '../db/pool.js';
import { runQuery } from '../db/query.js';
import type {
  DashboardQueuePage,
  FailureRateReport,
  LatencyReport,
  LatencySummary,
  ThroughputSeries,
} from '../domain/dashboard.js';
import type { QueueName } from '../domain/job.js';
import type { TenantId } from '../domain/tenant.js';

/**
 * Read-only aggregates for the dashboard API (Milestone 7).
 *
 * Every statement takes the queue's internal id (resolved inside the
 * caller's tenant by DashboardService, which is where isolation is
 * enforced) and uses the database clock for "now", so windows are
 * consistent across API instances. Index use:
 * - attempt aggregates: job_attempts_queue_finished_idx (queue_id,
 *   finished_at) INCLUDE (status, started_at, ready_at), index-only;
 * - enqueued: jobs_queue_created_idx (queue_id, created_at);
 * - dead-lettered: jobs_dlq_idx (queue_id, dead_lettered_at) WHERE dead;
 * - depth counts: jobs_queue_status_created_idx (queue_id, status, ...).
 */

/** date_bin origin: buckets align to UTC midnight (1m buckets start on :00). */
const BIN_ORIGIN = `timestamptz '2000-01-01 00:00:00+00'`;
const MS = `interval '1 millisecond'`;

interface QueueSummaryRow {
  name: string;
  max_concurrency: number | null;
  queued: number;
  scheduled: number;
  running: number;
  dead: number;
  oldest_queued_at: Date | null;
  computed_at: Date;
}

interface ThroughputRow {
  bucket_start: Date;
  window_end: Date;
  enqueued: number;
  succeeded: number;
  failed: number;
  timed_out: number;
  dead_lettered: number;
}

interface LatencyRow {
  window_start: Date;
  window_end: Date;
  exec_samples: number;
  exec_p: number[] | null;
  exec_max: number | null;
  wait_samples: number;
  wait_p: number[] | null;
  wait_max: number | null;
  truncated: boolean | null;
}

interface FailureRow {
  window_start: Date;
  window_end: Date;
  succeeded: number;
  failed: number;
  timed_out: number;
  dead_lettered: number;
}

export interface LatencyQuery {
  queueId: string;
  queue: QueueName;
  windowMs: number;
  sampleLimit: number;
}

function summary(samples: number, p: number[] | null, max: number | null): LatencySummary {
  return {
    samples,
    p50: p?.[0] ?? null,
    p95: p?.[1] ?? null,
    p99: p?.[2] ?? null,
    max,
  };
}

export class DashboardRepository {
  constructor(private readonly db: Queryable) {}

  /**
   * One page of the tenant's queues (by name) with depth per status. Each
   * count is a correlated index(-only) scan of that queue+status, so the
   * cost is bounded by the page size and the jobs actually in those states,
   * never by succeeded history.
   */
  async listQueues(
    tenantId: TenantId,
    limit: number,
    after: string | null,
  ): Promise<DashboardQueuePage> {
    const rows = await runQuery<QueueSummaryRow>(
      this.db,
      `SELECT q.name,
              q.max_concurrency,
              (SELECT count(*) FROM jobs j WHERE j.queue_id = q.id AND j.status = 'queued')::int AS queued,
              (SELECT count(*) FROM jobs j WHERE j.queue_id = q.id AND j.status = 'scheduled')::int AS scheduled,
              (SELECT count(*) FROM jobs j WHERE j.queue_id = q.id AND j.status = 'running')::int AS running,
              (SELECT count(*) FROM jobs j WHERE j.queue_id = q.id AND j.status = 'dead')::int AS dead,
              (SELECT min(run_at) FROM jobs j
               WHERE j.queue_id = q.id AND j.status = 'queued') AS oldest_queued_at,
              now() AS computed_at
       FROM queues q
       WHERE q.tenant_id = $1 AND ($2::text IS NULL OR q.name > $2)
       ORDER BY q.name
       LIMIT $3`,
      [tenantId, after, limit + 1],
    );
    const page = rows.slice(0, limit);
    return {
      queues: page.map((r) => ({
        queue: r.name as QueueName,
        queued: r.queued,
        scheduled: r.scheduled,
        running: r.running,
        dead: r.dead,
        maxConcurrency: r.max_concurrency,
        oldestQueuedAt: r.oldest_queued_at,
      })),
      nextAfter: rows.length > limit ? (page[page.length - 1]?.name ?? null) : null,
      // Empty page: still report the clock, so clients can tell staleness.
      computedAt: rows[0]?.computed_at ?? (await this.now()),
    };
  }

  /**
   * Per-bucket counts over [to - window, to), where `to` is the end of the
   * bucket containing now(): the last bucket is the current, still-filling
   * one. generate_series supplies every bucket so empty ones come back as
   * zeros instead of gaps. Each source is aggregated once per window (one
   * range scan each) and joined to the series on bucket start.
   */
  async throughput(
    queueId: string,
    queue: QueueName,
    windowMs: number,
    bucketMs: number,
  ): Promise<ThroughputSeries> {
    const rows = await runQuery<ThroughputRow>(
      this.db,
      `WITH bounds AS (
         SELECT date_bin($2::bigint * ${MS}, now(), ${BIN_ORIGIN}) + $2::bigint * ${MS} AS end_at
       ),
       series AS (
         SELECT gs AS bucket_start, b.end_at
         FROM bounds b,
              generate_series(b.end_at - $3::bigint * ${MS},
                              b.end_at - $2::bigint * ${MS},
                              $2::bigint * ${MS}) AS gs
       ),
       enq AS (
         SELECT date_bin($2::bigint * ${MS}, j.created_at, ${BIN_ORIGIN}) AS bucket_start,
                count(*) AS n
         FROM jobs j, bounds b
         WHERE j.queue_id = $1
           AND j.created_at >= b.end_at - $3::bigint * ${MS}
           AND j.created_at < b.end_at
         GROUP BY 1
       ),
       att AS (
         SELECT date_bin($2::bigint * ${MS}, a.finished_at, ${BIN_ORIGIN}) AS bucket_start,
                count(*) FILTER (WHERE a.status = 'succeeded') AS succeeded,
                count(*) FILTER (WHERE a.status = 'failed') AS failed,
                count(*) FILTER (WHERE a.status = 'timed_out') AS timed_out
         FROM job_attempts a, bounds b
         WHERE a.queue_id = $1
           AND a.finished_at IS NOT NULL
           AND a.finished_at >= b.end_at - $3::bigint * ${MS}
           AND a.finished_at < b.end_at
         GROUP BY 1
       ),
       dead AS (
         SELECT date_bin($2::bigint * ${MS}, j.dead_lettered_at, ${BIN_ORIGIN}) AS bucket_start,
                count(*) AS n
         FROM jobs j, bounds b
         WHERE j.queue_id = $1
           AND j.status = 'dead'
           AND j.dead_lettered_at >= b.end_at - $3::bigint * ${MS}
           AND j.dead_lettered_at < b.end_at
         GROUP BY 1
       )
       SELECT s.bucket_start,
              s.end_at AS window_end,
              coalesce(e.n, 0)::int AS enqueued,
              coalesce(a.succeeded, 0)::int AS succeeded,
              coalesce(a.failed, 0)::int AS failed,
              coalesce(a.timed_out, 0)::int AS timed_out,
              coalesce(d.n, 0)::int AS dead_lettered
       FROM series s
       LEFT JOIN enq e USING (bucket_start)
       LEFT JOIN att a USING (bucket_start)
       LEFT JOIN dead d USING (bucket_start)
       ORDER BY s.bucket_start`,
      [queueId, bucketMs, windowMs],
    );
    const first = rows[0];
    const to = first?.window_end ?? new Date();
    return {
      queue,
      from: new Date(to.getTime() - windowMs),
      to,
      bucketMs,
      buckets: rows.map((r) => ({
        start: r.bucket_start,
        enqueued: r.enqueued,
        succeeded: r.succeeded,
        failed: r.failed,
        timedOut: r.timed_out,
        deadLettered: r.dead_lettered,
      })),
    };
  }

  /**
   * p50/p95/p99 + max of execution time and queue wait over attempts that
   * finished in [now - window, now). percentile_cont sorts its input, so the
   * input is capped: the newest `sampleLimit` attempts (one index range scan
   * in finished_at DESC order, stopped by LIMIT). One extra row is read to
   * tell "exactly at the cap" from "truncated". `released` attempts (handed
   * back on shutdown) say nothing about the handler and are excluded.
   */
  async latency(query: LatencyQuery): Promise<LatencyReport> {
    const rows = await runQuery<LatencyRow>(
      this.db,
      `WITH bounds AS (SELECT now() AS end_at),
       s AS (
         SELECT (extract(epoch FROM a.finished_at - a.started_at) * 1000)::float8 AS exec_ms,
                CASE WHEN a.ready_at IS NULL THEN NULL
                     ELSE greatest(0, extract(epoch FROM a.started_at - a.ready_at) * 1000)::float8
                END AS wait_ms,
                row_number() OVER (ORDER BY a.finished_at DESC) AS rn
         FROM job_attempts a, bounds b
         WHERE a.queue_id = $1
           AND a.finished_at IS NOT NULL
           AND a.finished_at >= b.end_at - $2::bigint * ${MS}
           AND a.status IN ('succeeded', 'failed', 'timed_out')
         ORDER BY a.finished_at DESC
         LIMIT $3::int + 1
       )
       SELECT (SELECT end_at FROM bounds) - $2::bigint * ${MS} AS window_start,
              (SELECT end_at FROM bounds) AS window_end,
              count(*) FILTER (WHERE rn <= $3)::int AS exec_samples,
              percentile_cont(ARRAY[0.5, 0.95, 0.99]::float8[]) WITHIN GROUP (ORDER BY exec_ms)
                FILTER (WHERE rn <= $3) AS exec_p,
              (max(exec_ms) FILTER (WHERE rn <= $3))::float8 AS exec_max,
              count(wait_ms) FILTER (WHERE rn <= $3)::int AS wait_samples,
              percentile_cont(ARRAY[0.5, 0.95, 0.99]::float8[]) WITHIN GROUP (ORDER BY wait_ms)
                FILTER (WHERE rn <= $3 AND wait_ms IS NOT NULL) AS wait_p,
              (max(wait_ms) FILTER (WHERE rn <= $3))::float8 AS wait_max,
              bool_or(rn > $3) AS truncated
       FROM s`,
      [query.queueId, query.windowMs, query.sampleLimit],
    );
    const r = rows[0];
    if (!r) throw new Error('latency aggregate returned no row');
    return {
      queue: query.queue,
      from: r.window_start,
      to: r.window_end,
      execution: summary(r.exec_samples, r.exec_p, r.exec_max),
      queueWait: summary(r.wait_samples, r.wait_p, r.wait_max),
      truncated: r.truncated === true,
      sampleLimit: query.sampleLimit,
    };
  }

  /** Attempt outcomes and terminal job outcomes over [now - window, now). */
  async failureRate(
    queueId: string,
    queue: QueueName,
    windowMs: number,
  ): Promise<FailureRateReport> {
    const rows = await runQuery<FailureRow>(
      this.db,
      `WITH bounds AS (SELECT now() AS end_at, now() - $2::bigint * ${MS} AS start_at)
       SELECT b.start_at AS window_start,
              b.end_at AS window_end,
              coalesce(a.succeeded, 0)::int AS succeeded,
              coalesce(a.failed, 0)::int AS failed,
              coalesce(a.timed_out, 0)::int AS timed_out,
              (SELECT count(*) FROM jobs j
               WHERE j.queue_id = $1 AND j.status = 'dead'
                 AND j.dead_lettered_at >= b.start_at)::int AS dead_lettered
       FROM bounds b
       LEFT JOIN LATERAL (
         SELECT count(*) FILTER (WHERE x.status = 'succeeded') AS succeeded,
                count(*) FILTER (WHERE x.status = 'failed') AS failed,
                count(*) FILTER (WHERE x.status = 'timed_out') AS timed_out
         FROM job_attempts x
         WHERE x.queue_id = $1
           AND x.finished_at IS NOT NULL
           AND x.finished_at >= b.start_at
       ) a ON true`,
      [queueId, windowMs],
    );
    const r = rows[0];
    if (!r) throw new Error('failure-rate aggregate returned no row');
    const total = r.succeeded + r.failed + r.timed_out;
    const terminal = r.succeeded + r.dead_lettered;
    return {
      queue,
      from: r.window_start,
      to: r.window_end,
      attempts: { succeeded: r.succeeded, failed: r.failed, timedOut: r.timed_out, total },
      attemptFailureRate: total === 0 ? null : (r.failed + r.timed_out) / total,
      jobs: { succeeded: r.succeeded, deadLettered: r.dead_lettered },
      jobFailureRate: terminal === 0 ? null : r.dead_lettered / terminal,
    };
  }

  private async now(): Promise<Date> {
    const rows = await runQuery<{ now: Date }>(this.db, 'SELECT now() AS now');
    return rows[0]?.now ?? new Date();
  }
}
