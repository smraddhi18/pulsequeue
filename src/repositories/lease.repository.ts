import type { DeadLetterReason } from '../domain/dead-letter.js';
import type { JobId, QueueName } from '../domain/job.js';
import type { FailureDisposition, LeaseRef, LeasedJob, ReclaimedJob } from '../domain/lease.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';

export interface LeaseJobsParams {
  /** Queues this worker has handlers for. */
  queues: readonly string[];
  workerId: string;
  /** Max jobs to lease in this call (the worker's free slots). */
  limit: number;
  leaseMs: number;
}

export interface FailAttemptParams {
  error: string;
  /** How the attempt ended: a thrown error or the per-attempt timeout. */
  attemptStatus: 'failed' | 'timed_out';
  /** Delay before the next attempt, or null to dead-letter the job. */
  retryDelayMs: number | null;
  /** Why the job is dead-lettered when retryDelayMs is null (default attempts_exhausted). */
  deadLetterReason?: DeadLetterReason;
}

export interface ReapExpiredOptions {
  /**
   * Poison-message threshold: dead-letter a job (reason poison_lease_expired)
   * once its lease has expired this many times, even with attempts left.
   * Omit to rely on max_attempts alone.
   */
  poisonLeaseExpiries?: number;
}

interface LeasedRow {
  id: string;
  queue_name: string;
  payload: unknown;
  priority: number;
  attempts: number;
  max_attempts: number;
  lease_token: string;
  locked_by: string;
  lease_expires_at: Date;
  created_at: Date;
  attempt_id: string;
  attempt_number: number;
}

interface ReclaimedRow {
  id: string;
  queue_name: string;
  previous_worker_id: string;
  attempts: number;
  max_attempts: number;
  status: 'queued' | 'dead';
  dead_reason: DeadLetterReason | null;
  lease_expiries: number;
}

/** SET fragment that drops a lease; jobs_running_has_lease requires it whenever status leaves running. */
const CLEAR_LEASE = `locked_by = NULL, lease_token = NULL, locked_at = NULL,
             lease_expires_at = NULL, heartbeat_at = NULL`;

/** Max scheduled -> queued promotions per lease call; see LeaseRepository.promoteDue. */
export const PROMOTE_BATCH_SIZE = 500;

/** Longest error text stored per attempt / job; handlers can throw huge messages. */
export const MAX_ERROR_LENGTH = 4_000;

export function truncateError(message: string): string {
  return message.length > MAX_ERROR_LENGTH ? `${message.slice(0, MAX_ERROR_LENGTH - 1)}…` : message;
}

/**
 * All SQL for the worker side of the queue: lease, heartbeat, complete, fail,
 * release and reap. Every statement is a single round-trip; multi-table
 * changes use data-modifying CTEs, so each is atomic in its own implicit
 * transaction and no row lock is held across a network round-trip or while a
 * handler runs.
 *
 * Every write after the lease is fenced on (id, status = 'running',
 * lease_token): it affects zero rows if the lease was lost, and the caller is
 * told so instead of silently clobbering the current owner.
 */
export class LeaseRepository {
  constructor(private readonly db: Queryable) {}

  /**
   * Lease up to `limit` runnable jobs from `queues`.
   *
   * Two statements. First, due `scheduled` jobs (delayed enqueues, retry
   * backoffs, delayed replays) of these queues are promoted to `queued`
   * (see promoteDue). Then the lease itself: FOR UPDATE SKIP LOCKED makes
   * concurrent workers partition the ready set instead of queueing on each
   * other's row locks: a row another transaction is leasing right now is
   * skipped, not waited for. The candidate scan walks jobs_ready_idx (queued
   * rows only) in (priority DESC, run_at, id) order, so it never reads a job
   * that is not due yet. Each leased job gets a fresh lease token, attempts + 1
   * and a `running` job_attempts row, all in the same statement.
   */
  async lease(params: LeaseJobsParams): Promise<LeasedJob[]> {
    if (params.limit <= 0 || params.queues.length === 0) return [];
    await this.promoteDue(params.queues, Math.max(params.limit, PROMOTE_BATCH_SIZE));
    const rows = await runQuery<LeasedRow>(
      this.db,
      `WITH candidates AS (
         SELECT j.id
         FROM jobs j
         WHERE j.queue_id IN (SELECT q.id FROM queues q WHERE q.name = ANY($1::text[]))
           AND j.status = 'queued'
           -- Defensive: queued rows are due by construction (promotion and
           -- immediate enqueues set run_at <= now()), but a skewed producer
           -- clock must never let a job start early.
           AND j.run_at <= now()
           -- Defensive: a runnable row must have budget left, otherwise the
           -- increment below would violate jobs_attempts_within_max and fail
           -- the whole batch on every poll.
           AND j.attempts < j.max_attempts
         ORDER BY j.priority DESC, j.run_at, j.id
         LIMIT $2
         FOR UPDATE SKIP LOCKED
       ),
       leased AS (
         UPDATE jobs j
         SET status           = 'running',
             attempts         = j.attempts + 1,
             locked_by        = $3,
             lease_token      = gen_random_uuid(),
             locked_at        = now(),
             heartbeat_at     = now(),
             lease_expires_at = now() + ($4::bigint * interval '1 millisecond')
         FROM candidates c
         WHERE j.id = c.id
         RETURNING j.id, j.queue_id, j.payload, j.priority, j.attempts, j.max_attempts,
                   j.lease_token, j.locked_by, j.lease_expires_at, j.run_at, j.created_at
       ),
       attempt AS (
         -- attempt_number counts every execution (released ones too), so it is
         -- derived from the history rather than from jobs.attempts. Safe without
         -- a race: the job row is locked by this statement.
         INSERT INTO job_attempts (job_id, attempt_number, worker_id, status)
         SELECT l.id,
                (SELECT count(*) FROM job_attempts a WHERE a.job_id = l.id) + 1,
                $3,
                'running'
         FROM leased l
         RETURNING id, job_id, attempt_number
       )
       SELECT l.id, q.name AS queue_name, l.payload, l.priority, l.attempts, l.max_attempts,
              l.lease_token, l.locked_by, l.lease_expires_at, l.created_at,
              a.id AS attempt_id, a.attempt_number
       FROM leased l
       JOIN attempt a ON a.job_id = l.id
       JOIN queues q ON q.id = l.queue_id
       ORDER BY l.priority DESC, l.run_at, l.id`,
      [params.queues, params.limit, params.workerId, params.leaseMs],
    );
    return rows.map((row) => ({
      jobId: row.id as JobId,
      queue: row.queue_name as QueueName,
      payload: row.payload,
      priority: row.priority,
      attempt: row.attempts,
      maxAttempts: row.max_attempts,
      attemptId: row.attempt_id,
      attemptNumber: row.attempt_number,
      leaseToken: row.lease_token,
      workerId: row.locked_by,
      leaseExpiresAt: row.lease_expires_at,
      createdAt: row.created_at,
    }));
  }
  /**
   * Move up to `limit` due `scheduled` jobs of `queues` to `queued`, oldest
   * due first; returns how many moved.
   *
   * The scan is a range on jobs_scheduled_due_idx (queue_id, run_at), so it
   * reads only rows that are due: jobs delayed until tomorrow are never
   * touched. SKIP LOCKED lets every worker run this on each poll without
   * blocking another worker promoting the same rows; a row promoted by
   * someone else simply stops matching `status = 'scheduled'`. Bounded so one
   * poll after a long outage cannot turn into one giant transaction; the rest
   * is promoted by the following polls.
   */
  async promoteDue(queues: readonly string[], limit: number = PROMOTE_BATCH_SIZE): Promise<number> {
    if (limit <= 0 || queues.length === 0) return 0;
    const rows = await runQuery<{ promoted: string }>(
      this.db,
      `WITH due AS (
         SELECT j.id
         FROM jobs j
         WHERE j.queue_id IN (SELECT q.id FROM queues q WHERE q.name = ANY($1::text[]))
           AND j.status = 'scheduled'
           AND j.run_at <= now()
         ORDER BY j.run_at, j.id
         LIMIT $2
         FOR UPDATE SKIP LOCKED
       ),
       promoted AS (
         UPDATE jobs j
         SET status = 'queued'
         FROM due d
         WHERE j.id = d.id
         RETURNING j.id
       )
       SELECT count(*)::text AS promoted FROM promoted`,
      [queues, limit],
    );
    return Number(rows[0]?.promoted ?? 0);
  }

  /**
   * Milliseconds until the earliest `scheduled` job of `queues` is due (0 if
   * one is already due), or null when none is scheduled. Measured on the
   * database clock, like run_at itself.
   *
   * Used by workers in push mode (Milestone 5): wake-ups announce new work,
   * but nothing publishes when a delayed job or a retry *becomes* due, so the
   * idle worker sleeps at most until then. One index probe per queue on
   * jobs_scheduled_due_idx (queue_id, run_at): cheap enough to run on every
   * idle cycle.
   */
  async nextDueInMs(queues: readonly string[]): Promise<number | null> {
    if (queues.length === 0) return null;
    const rows = await runQuery<{ due_in_ms: string | null }>(
      this.db,
      // No GREATEST(0, ...) here: GREATEST skips NULLs, so "nothing
      // scheduled" would come back as 0 (due now). Clamped below instead.
      `SELECT ceil(EXTRACT(EPOCH FROM (min(n.run_at) - now())) * 1000)::bigint::text AS due_in_ms
       FROM queues q
       CROSS JOIN LATERAL (
         SELECT j.run_at
         FROM jobs j
         WHERE j.queue_id = q.id AND j.status = 'scheduled'
         ORDER BY j.run_at
         LIMIT 1
       ) n
       WHERE q.name = ANY($1::text[])`,
      [queues],
    );
    const value = rows[0]?.due_in_ms;
    return value === null || value === undefined ? null : Math.max(0, Number(value));
  }

  /**
   * Extend the lease to now() + leaseMs. Returns the new expiry, or null when
   * the lease is gone (reaped and possibly re-leased): the caller must stop.
   */
  async heartbeat(lease: LeaseRef, leaseMs: number): Promise<Date | null> {
    const rows = await runQuery<{ lease_expires_at: Date }>(
      this.db,
      `UPDATE jobs
       SET heartbeat_at = now(),
           lease_expires_at = now() + ($3::bigint * interval '1 millisecond')
       WHERE id = $1 AND status = 'running' AND lease_token = $2
       RETURNING lease_expires_at`,
      [lease.jobId, lease.leaseToken, leaseMs],
    );
    return rows[0]?.lease_expires_at ?? null;
  }

  /** Mark the job and its attempt succeeded. false = lease lost, nothing written. */
  async complete(lease: LeaseRef): Promise<boolean> {
    const rows = await runQuery<{ id: string }>(
      this.db,
      `WITH done AS (
         UPDATE jobs
         SET status = 'succeeded', finished_at = now(), last_error = NULL,
             ${CLEAR_LEASE}
         WHERE id = $1 AND status = 'running' AND lease_token = $2
         RETURNING id
       )
       UPDATE job_attempts a
       SET status = 'succeeded', finished_at = now()
       FROM done
       WHERE a.id = $3 AND a.job_id = done.id
       RETURNING a.id`,
      [lease.jobId, lease.leaseToken, lease.attemptId],
    );
    return rows.length > 0;
  }

  /**
   * Record a failed attempt and either reschedule the job after
   * `retryDelayMs` (status scheduled, run_at on the database clock) or move
   * it to the dead-letter queue (status dead + reason + dead_lettered_at).
   *
   * Dead-lettering happens here, in the same fenced statement that records
   * the attempt, not in a follow-up write: it is atomic with the outcome (no
   * window where a crash leaves the job terminal but outside the DLQ), and a
   * worker whose lease was lost matches zero rows, so it can neither
   * dead-letter nor reschedule a job someone else now owns. null = lease
   * lost, nothing written.
   */
  async fail(lease: LeaseRef, params: FailAttemptParams): Promise<FailureDisposition | null> {
    const error = truncateError(params.error);
    const deadReason =
      params.retryDelayMs === null ? (params.deadLetterReason ?? 'attempts_exhausted') : null;
    const rows = await runQuery<{ status: 'dead' | 'scheduled' }>(
      this.db,
      `WITH updated AS (
         UPDATE jobs
         SET status           = CASE WHEN $4::bigint IS NULL THEN 'dead' ELSE 'scheduled' END::job_status,
             run_at           = CASE WHEN $4::bigint IS NULL THEN run_at
                                     ELSE now() + ($4::bigint * interval '1 millisecond') END,
             finished_at      = CASE WHEN $4::bigint IS NULL THEN now() ELSE NULL END,
             dead_reason      = $7::dead_letter_reason,
             dead_lettered_at = CASE WHEN $4::bigint IS NULL THEN now() ELSE NULL END,
             last_error       = $3,
             ${CLEAR_LEASE}
         WHERE id = $1 AND status = 'running' AND lease_token = $2
         RETURNING id, status
       ),
       attempt AS (
         UPDATE job_attempts a
         SET status = $6::job_attempt_status, error = $3, finished_at = now()
         FROM updated
         WHERE a.id = $5 AND a.job_id = updated.id
       )
       SELECT status FROM updated`,
      [
        lease.jobId,
        lease.leaseToken,
        error,
        params.retryDelayMs,
        lease.attemptId,
        params.attemptStatus,
        deadReason,
      ],
    );
    const status = rows[0]?.status;
    if (status === undefined) return null;
    return status === 'dead' ? 'dead_lettered' : 'retrying';
  }

  /**
   * Hand a job back unfinished (graceful shutdown hit its drain timeout, or a
   * job was leased while stopping). It becomes runnable immediately and the
   * attempt is recorded as `released` and refunded: a deploy is not the job's
   * fault, so it must not burn retry budget. false = lease lost.
   */
  async release(lease: LeaseRef, reason: string): Promise<boolean> {
    const rows = await runQuery<{ id: string }>(
      this.db,
      `WITH released AS (
         UPDATE jobs
         SET status = 'queued', run_at = now(), attempts = attempts - 1,
             ${CLEAR_LEASE}
         WHERE id = $1 AND status = 'running' AND lease_token = $2
         RETURNING id
       )
       UPDATE job_attempts a
       SET status = 'released', error = $4, finished_at = now()
       FROM released
       WHERE a.id = $3 AND a.job_id = released.id
       RETURNING a.id`,
      [lease.jobId, lease.leaseToken, lease.attemptId, truncateError(reason)],
    );
    return rows.length > 0;
  }

  /**
   * Reclaim up to `limit` jobs whose lease expired (worker crashed, hung or
   * partitioned). The attempt is recorded as timed_out and *does* count: a
   * job that keeps killing its worker must eventually stop being retried. A
   * job with attempts left becomes queued again; otherwise it is
   * dead-lettered. Every expiry also bumps lease_expiries, and reaching
   * `poisonLeaseExpiries` dead-letters the job as a poison message even with
   * attempts left (a job that OOMs its worker should not get 25 more tries).
   *
   * SKIP LOCKED lets every worker run a reaper without coordination, and it
   * skips rows a live worker is heartbeating right now; the WHERE is
   * re-checked after locking, so a heartbeat that just landed wins.
   */
  async reapExpired(limit: number, options: ReapExpiredOptions = {}): Promise<ReclaimedJob[]> {
    const rows = await runQuery<ReclaimedRow>(
      this.db,
      `WITH expired AS (
         SELECT id, locked_by,
                CASE
                  WHEN $2::int IS NOT NULL AND lease_expiries + 1 >= $2::int THEN 'poison_lease_expired'
                  WHEN attempts >= max_attempts THEN 'attempts_exhausted'
                END::dead_letter_reason AS dead_reason
         FROM jobs
         WHERE status = 'running' AND lease_expires_at < now()
         ORDER BY lease_expires_at
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       ),
       reclaimed AS (
         UPDATE jobs j
         SET status           = CASE WHEN e.dead_reason IS NOT NULL THEN 'dead' ELSE 'queued' END::job_status,
             run_at           = CASE WHEN e.dead_reason IS NOT NULL THEN j.run_at ELSE now() END,
             finished_at      = CASE WHEN e.dead_reason IS NOT NULL THEN now() ELSE NULL END,
             dead_reason      = e.dead_reason,
             dead_lettered_at = CASE WHEN e.dead_reason IS NOT NULL THEN now() ELSE NULL END,
             lease_expiries   = j.lease_expiries + 1,
             last_error       = 'Lease expired: worker ' || e.locked_by || ' stopped heartbeating',
             ${CLEAR_LEASE}
         FROM expired e
         WHERE j.id = e.id
         RETURNING j.id, j.queue_id, e.locked_by AS previous_worker_id, j.attempts, j.max_attempts,
                   j.status, j.dead_reason, j.lease_expiries
       ),
       attempt_rows AS (
         UPDATE job_attempts a
         SET status = 'timed_out', finished_at = now(),
             error = 'Lease expired: worker ' || r.previous_worker_id || ' stopped heartbeating'
         FROM reclaimed r
         WHERE a.job_id = r.id AND a.status = 'running'
       )
       SELECT r.id, q.name AS queue_name, r.previous_worker_id, r.attempts, r.max_attempts,
              r.status::text AS status, r.dead_reason::text AS dead_reason, r.lease_expiries
       FROM reclaimed r
       JOIN queues q ON q.id = r.queue_id`,
      [limit, options.poisonLeaseExpiries ?? null],
    );
    return rows.map((row) => ({
      jobId: row.id as JobId,
      queue: row.queue_name as QueueName,
      previousWorkerId: row.previous_worker_id,
      attempt: row.attempts,
      maxAttempts: row.max_attempts,
      status: row.status,
      deadLetterReason: row.dead_reason,
      leaseExpiries: row.lease_expiries,
    }));
  }
}
