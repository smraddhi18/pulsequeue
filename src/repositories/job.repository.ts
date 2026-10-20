import type { Job, JobId, JobStatus, QueueName } from '../domain/job.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';

/** Job as stored, including internal columns that never leave the service layer. */
export interface StoredJob extends Job {
  requestFingerprint: string | null;
  /** Exact created_at text (UTC, microseconds) for keyset cursors. */
  createdAtCursor: string;
}

export interface InsertJobParams {
  queueId: string;
  status: Extract<JobStatus, 'queued' | 'scheduled'>;
  priority: number;
  payload: unknown;
  maxAttempts: number;
  delayMs: number;
  idempotencyKey: string | null;
  requestFingerprint: string | null;
}

export interface InsertJobResult {
  job: StoredJob;
  /** false when the idempotency key already existed and nothing was written. */
  inserted: boolean;
}

export interface ListJobsParams {
  queueId: string;
  status?: JobStatus;
  limit: number;
  /** Return rows strictly older than this (created_at, id) position. */
  after?: { createdAt: string; id: string };
}

interface JobRow {
  id: string;
  queue_name: string;
  status: JobStatus;
  priority: number;
  payload: unknown;
  attempts: number;
  max_attempts: number;
  run_at: Date;
  idempotency_key: string | null;
  request_fingerprint: string | null;
  last_error: string | null;
  finished_at: Date | null;
  created_at: Date;
  updated_at: Date;
  created_at_cursor: string;
}

/** Columns selected for every job read; `j` = jobs, `q` = queues. */
const JOB_COLUMNS = `
  j.id, q.name AS queue_name, j.status, j.priority, j.payload, j.attempts,
  j.max_attempts, j.run_at, j.idempotency_key, j.request_fingerprint,
  j.last_error, j.finished_at, j.created_at, j.updated_at,
  to_char(j.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at_cursor`;

function toStoredJob(row: JobRow): StoredJob {
  return {
    id: row.id as JobId,
    queue: row.queue_name as QueueName,
    status: row.status,
    payload: row.payload,
    priority: row.priority,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    runAt: row.run_at,
    idempotencyKey: row.idempotency_key,
    lastError: row.last_error,
    finishedAt: row.finished_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    requestFingerprint: row.request_fingerprint,
    createdAtCursor: row.created_at_cursor,
  };
}

/** Bounded retries for the (rare) case where the conflicting row disappears between statements. */
const MAX_IDEMPOTENT_INSERT_ATTEMPTS = 3;

export class JobRepository {
  constructor(private readonly db: Queryable) {}

  /**
   * Insert a job, or return the existing one for the same (queue, idempotency key).
   *
   * Race-safety comes from the partial unique index plus ON CONFLICT DO NOTHING,
   * never from a pre-check. Under READ COMMITTED, a concurrent loser's
   * statement waits for the winner to commit and then inserts nothing, but its
   * snapshot cannot see the winner's row, so the existing job is read in a
   * separate statement. DO NOTHING (rather than a no-op DO UPDATE) keeps
   * replays read-only: no row lock, no dead tuple, updated_at untouched.
   *
   * run_at is computed from the database clock so API replicas with skewed
   * clocks agree on when a delayed job becomes runnable.
   */
  async insertIdempotent(params: InsertJobParams): Promise<InsertJobResult> {
    for (let attempt = 1; attempt <= MAX_IDEMPOTENT_INSERT_ATTEMPTS; attempt++) {
      const rows = await runQuery<JobRow>(
        this.db,
        `WITH inserted AS (
           INSERT INTO jobs (
             queue_id, status, priority, payload, max_attempts, run_at,
             idempotency_key, request_fingerprint
           )
           VALUES (
             $1, $2, $3, $4::jsonb, $5, now() + ($6::bigint * interval '1 millisecond'),
             $7, $8
           )
           ON CONFLICT (queue_id, idempotency_key) WHERE idempotency_key IS NOT NULL
           DO NOTHING
           RETURNING *
         )
         SELECT ${JOB_COLUMNS}
         FROM inserted j
         JOIN queues q ON q.id = j.queue_id`,
        [
          params.queueId,
          params.status,
          params.priority,
          // Always serialize explicitly: pg would turn a JS array into a
          // Postgres array literal and send a bare string unquoted.
          JSON.stringify(params.payload),
          params.maxAttempts,
          params.delayMs,
          params.idempotencyKey,
          params.requestFingerprint,
        ],
      );

      if (rows[0]) return { job: toStoredJob(rows[0]), inserted: true };

      if (params.idempotencyKey === null) {
        throw new Error('Job insert returned no row without an idempotency key');
      }

      const existing = await this.findByIdempotencyKey(params.queueId, params.idempotencyKey);
      if (existing) return { job: existing, inserted: false };
      // The conflicting job was deleted in between; the key is free again, retry the insert.
    }
    throw new Error('Idempotent job insert did not converge');
  }

  async findById(id: string): Promise<StoredJob | null> {
    const rows = await runQuery<JobRow>(
      this.db,
      `SELECT ${JOB_COLUMNS}
       FROM jobs j
       JOIN queues q ON q.id = j.queue_id
       WHERE j.id = $1`,
      [id],
    );
    return rows[0] ? toStoredJob(rows[0]) : null;
  }

  async findByIdempotencyKey(queueId: string, idempotencyKey: string): Promise<StoredJob | null> {
    const rows = await runQuery<JobRow>(
      this.db,
      `SELECT ${JOB_COLUMNS}
       FROM jobs j
       JOIN queues q ON q.id = j.queue_id
       WHERE j.queue_id = $1 AND j.idempotency_key = $2`,
      [queueId, idempotencyKey],
    );
    return rows[0] ? toStoredJob(rows[0]) : null;
  }

  /**
   * Keyset page, newest first: ORDER BY (created_at, id) DESC with a row-value
   * comparison against the cursor, served by jobs_queue_created_idx or
   * jobs_queue_status_created_idx. Cost is O(limit) at any depth, unlike
   * OFFSET, and concurrent inserts never shift or duplicate rows across pages.
   */
  async listByQueue(params: ListJobsParams): Promise<StoredJob[]> {
    const values: unknown[] = [params.queueId];
    const where = ['j.queue_id = $1'];

    if (params.status !== undefined) {
      values.push(params.status);
      where.push(`j.status = $${values.length}::job_status`);
    }
    if (params.after !== undefined) {
      values.push(params.after.createdAt, params.after.id);
      where.push(
        `(j.created_at, j.id) < ($${values.length - 1}::timestamptz, $${values.length}::uuid)`,
      );
    }
    values.push(params.limit);

    const rows = await runQuery<JobRow>(
      this.db,
      `SELECT ${JOB_COLUMNS}
       FROM jobs j
       JOIN queues q ON q.id = j.queue_id
       WHERE ${where.join(' AND ')}
       ORDER BY j.created_at DESC, j.id DESC
       LIMIT $${values.length}`,
      values,
    );
    return rows.map(toStoredJob);
  }
}
