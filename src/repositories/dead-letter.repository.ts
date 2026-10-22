import type { DeadLetterReason, JobAttempt } from '../domain/dead-letter.js';
import type { JobId } from '../domain/job.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';
import { JOB_COLUMNS, toStoredJob, type JobRow, type StoredJob } from './job.repository.js';

/** A job plus its DLQ columns. dead* fields are null unless status = 'dead'. */
export interface StoredDeadLetterJob extends StoredJob {
  deadReason: DeadLetterReason | null;
  deadLetteredAt: Date | null;
  /** Exact dead_lettered_at text (UTC, microseconds) for keyset cursors; null unless dead. */
  deadLetteredAtCursor: string | null;
  leaseExpiries: number;
  replayCount: number;
  lastReplayedAt: Date | null;
}

export interface ListDeadLettersParams {
  queueId: string;
  reason?: DeadLetterReason;
  limit: number;
  /** Return rows strictly older than this (dead_lettered_at, id) position. */
  after?: { createdAt: string; id: string };
}

export interface ReplayParams {
  jobId: string;
  queueId: string;
  delayMs: number;
}

export interface ReplayBatchParams {
  queueId: string;
  limit: number;
  reason?: DeadLetterReason;
  delayMs: number;
}

export interface PurgeOlderThanParams {
  queueId: string;
  olderThan: Date;
  limit: number;
}

interface DeadLetterRow extends JobRow {
  dead_reason: DeadLetterReason | null;
  dead_lettered_at: Date | null;
  dead_lettered_at_cursor: string | null;
  lease_expiries: number;
  replay_count: number;
  last_replayed_at: Date | null;
}

interface AttemptRow {
  attempt_number: number;
  status: JobAttempt['status'];
  worker_id: string | null;
  error: string | null;
  started_at: Date;
  finished_at: Date | null;
}

const DLQ_COLUMNS = `${JOB_COLUMNS},
  j.dead_reason, j.dead_lettered_at, j.lease_expiries, j.replay_count, j.last_replayed_at,
  to_char(j.dead_lettered_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS dead_lettered_at_cursor`;

/**
 * SET fragment that takes a job out of the DLQ: a fresh retry budget and DLQ
 * metadata cleared (jobs_dead_has_metadata requires both together). The
 * attempt history is untouched; new attempts continue its attempt_number
 * sequence, so history shows "3 failures, replay, then success".
 * $delay is the bind parameter holding the delay in milliseconds.
 */
function replaySet(delay: string): string {
  return `status           = CASE WHEN ${delay}::bigint > 0 THEN 'scheduled' ELSE 'queued' END::job_status,
          run_at           = now() + (${delay}::bigint * interval '1 millisecond'),
          attempts         = 0,
          finished_at      = NULL,
          dead_reason      = NULL,
          dead_lettered_at = NULL,
          lease_expiries   = 0,
          replay_count     = replay_count + 1,
          last_replayed_at = now()`;
}

function toStoredDeadLetter(row: DeadLetterRow): StoredDeadLetterJob {
  return {
    ...toStoredJob(row),
    deadReason: row.dead_reason,
    deadLetteredAt: row.dead_lettered_at,
    deadLetteredAtCursor: row.dead_lettered_at_cursor,
    leaseExpiries: row.lease_expiries,
    replayCount: row.replay_count,
    lastReplayedAt: row.last_replayed_at,
  };
}

/**
 * SQL for the dead-letter queue: inspect, replay and purge. Dead-lettering
 * itself is a worker outcome and lives in LeaseRepository (fenced on the
 * lease token).
 *
 * Every mutation is a single statement guarded by `status = 'dead'` in its
 * WHERE. Under READ COMMITTED a concurrent second replay/purge of the same
 * job blocks on the row lock, then re-evaluates the WHERE against the
 * committed row, sees it is no longer dead and matches nothing. So each
 * dead-lettered job is replayed or purged at most once, without explicit
 * locking or SERIALIZABLE retries.
 */
export class DeadLetterRepository {
  constructor(private readonly db: Queryable) {}

  /** Any job by id with its DLQ columns (callers check status). */
  async findById(id: string): Promise<StoredDeadLetterJob | null> {
    const rows = await runQuery<DeadLetterRow>(
      this.db,
      `SELECT ${DLQ_COLUMNS}
       FROM jobs j
       JOIN queues q ON q.id = j.queue_id
       WHERE j.id = $1`,
      [id],
    );
    return rows[0] ? toStoredDeadLetter(rows[0]) : null;
  }

  /**
   * Keyset page of a queue's DLQ, most recently dead-lettered first, served
   * by the partial jobs_dlq_idx (queue_id, dead_lettered_at DESC, id DESC).
   */
  async listByQueue(params: ListDeadLettersParams): Promise<StoredDeadLetterJob[]> {
    const values: unknown[] = [params.queueId];
    const where = ['j.queue_id = $1', "j.status = 'dead'"];

    if (params.reason !== undefined) {
      values.push(params.reason);
      where.push(`j.dead_reason = $${values.length}::dead_letter_reason`);
    }
    if (params.after !== undefined) {
      values.push(params.after.createdAt, params.after.id);
      where.push(
        `(j.dead_lettered_at, j.id) < ($${values.length - 1}::timestamptz, $${values.length}::uuid)`,
      );
    }
    values.push(params.limit);

    const rows = await runQuery<DeadLetterRow>(
      this.db,
      `SELECT ${DLQ_COLUMNS}
       FROM jobs j
       JOIN queues q ON q.id = j.queue_id
       WHERE ${where.join(' AND ')}
       ORDER BY j.dead_lettered_at DESC, j.id DESC
       LIMIT $${values.length}`,
      values,
    );
    return rows.map(toStoredDeadLetter);
  }

  async listAttempts(jobId: string): Promise<JobAttempt[]> {
    const rows = await runQuery<AttemptRow>(
      this.db,
      `SELECT attempt_number, status::text AS status, worker_id, error, started_at, finished_at
       FROM job_attempts
       WHERE job_id = $1
       ORDER BY attempt_number`,
      [jobId],
    );
    return rows.map((row) => ({
      attemptNumber: row.attempt_number,
      status: row.status,
      workerId: row.worker_id,
      error: row.error,
      startedAt: row.started_at,
      finishedAt: row.finished_at,
    }));
  }

  /**
   * Move one dead-lettered job of `queueId` back to queued (or scheduled
   * after delayMs). null = nothing replayed: the job does not exist in this
   * queue or is no longer dead (e.g. a concurrent replay won).
   */
  async replay(params: ReplayParams): Promise<StoredDeadLetterJob | null> {
    const rows = await runQuery<DeadLetterRow>(
      this.db,
      `WITH replayed AS (
         UPDATE jobs
         SET ${replaySet('$3')}
         WHERE id = $1 AND queue_id = $2 AND status = 'dead'
         RETURNING *
       )
       SELECT ${DLQ_COLUMNS}
       FROM replayed j
       JOIN queues q ON q.id = j.queue_id`,
      [params.jobId, params.queueId, params.delayMs],
    );
    return rows[0] ? toStoredDeadLetter(rows[0]) : null;
  }

  /**
   * Replay up to `limit` dead-lettered jobs, oldest first. SKIP LOCKED lets
   * concurrent bulk replays (or a bulk replay racing single replays/purges)
   * partition the DLQ instead of blocking on, or double-replaying, the same
   * rows. Returns the replayed ids.
   */
  async replayBatch(params: ReplayBatchParams): Promise<JobId[]> {
    const values: unknown[] = [params.queueId, params.limit, params.delayMs];
    let reasonFilter = '';
    if (params.reason !== undefined) {
      values.push(params.reason);
      reasonFilter = `AND dead_reason = $${values.length}::dead_letter_reason`;
    }
    const rows = await runQuery<{ id: string }>(
      this.db,
      `WITH picked AS (
         SELECT id
         FROM jobs
         WHERE queue_id = $1 AND status = 'dead' ${reasonFilter}
         ORDER BY dead_lettered_at, id
         LIMIT $2
         FOR UPDATE SKIP LOCKED
       )
       UPDATE jobs j
       SET ${replaySet('$3')}
       FROM picked p
       WHERE j.id = p.id
       RETURNING j.id`,
      values,
    );
    return rows.map((r) => r.id as JobId);
  }

  /** Delete one dead-lettered job (attempts cascade). false = not dead / not in this queue. */
  async purge(jobId: string, queueId: string): Promise<boolean> {
    const rows = await runQuery<{ id: string }>(
      this.db,
      `DELETE FROM jobs
       WHERE id = $1 AND queue_id = $2 AND status = 'dead'
       RETURNING id`,
      [jobId, queueId],
    );
    return rows.length > 0;
  }

  /**
   * Delete up to `limit` jobs dead-lettered before `olderThan`, oldest first.
   * Bounded so one call cannot hold locks on (and cascade-delete the attempts
   * of) an unbounded number of rows; callers repeat until fewer than `limit`
   * come back.
   */
  async purgeOlderThan(params: PurgeOlderThanParams): Promise<number> {
    const rows = await runQuery<{ purged: number }>(
      this.db,
      `WITH doomed AS (
         SELECT id
         FROM jobs
         WHERE queue_id = $1 AND status = 'dead' AND dead_lettered_at < $2::timestamptz
         ORDER BY dead_lettered_at, id
         LIMIT $3
         FOR UPDATE SKIP LOCKED
       ),
       deleted AS (
         DELETE FROM jobs j
         USING doomed d
         WHERE j.id = d.id
         RETURNING j.id
       )
       SELECT count(*)::int AS purged FROM deleted`,
      [params.queueId, params.olderThan, params.limit],
    );
    return rows[0]?.purged ?? 0;
  }
}
