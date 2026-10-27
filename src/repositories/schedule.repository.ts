import type { JobId, QueueName } from '../domain/job.js';
import type { MisfirePolicy, Schedule, ScheduleId } from '../domain/schedule.js';
import type { TenantId } from '../domain/tenant.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';

/** Schedule as stored, including internal columns that never leave the service layer. */
export interface StoredSchedule extends Schedule {
  queueId: string;
  /** Owner of the schedule's queue (for signals; never in API responses). */
  tenantId: TenantId;
  /** Exact created_at text (UTC, microseconds) for keyset cursors. */
  createdAtCursor: string;
}

export interface InsertScheduleParams {
  queueId: string;
  name: string;
  cronExpression: string;
  timezone: string;
  payload: unknown;
  priority: number;
  maxAttempts: number;
  misfirePolicy: MisfirePolicy;
  /** null creates the schedule paused (enabled = false). */
  nextRunAt: Date | null;
}

export interface ListSchedulesParams {
  queueId: string;
  enabled?: boolean;
  limit: number;
  /** Return rows strictly older than this (created_at, id) position. */
  after?: { createdAt: string; id: string };
}

/** Outcome of one scheduler evaluation of a claimed schedule. */
export interface RecordFireParams {
  id: string;
  /** New next occurrence (always set: evaluation never pauses a schedule). */
  nextRunAt: Date;
  /** Latest occurrence that enqueued a job in this evaluation, if any. */
  lastRunAt: Date | null;
  lastJobId: string | null;
  /** Number of occurrences fired in this evaluation (added to fire_count). */
  fired: number;
  /** Evaluation error, or null to clear the previous one. */
  lastError: string | null;
}

interface ScheduleRow {
  id: string;
  queue_id: string;
  tenant_id: string;
  queue_name: string;
  name: string;
  cron_expression: string;
  timezone: string;
  payload: unknown;
  priority: number;
  max_attempts: number;
  misfire_policy: MisfirePolicy;
  enabled: boolean;
  next_run_at: Date | null;
  last_run_at: Date | null;
  last_job_id: string | null;
  fire_count: string;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
  created_at_cursor: string;
}

/** Columns selected for every schedule read; `s` = schedules, `q` = queues. */
const SCHEDULE_COLUMNS = `
  s.id, s.queue_id::text AS queue_id, q.tenant_id, q.name AS queue_name, s.name, s.cron_expression,
  s.timezone, s.payload, s.priority, s.max_attempts, s.misfire_policy, s.enabled,
  s.next_run_at, s.last_run_at, s.last_job_id, s.fire_count::text AS fire_count,
  s.last_error, s.created_at, s.updated_at,
  to_char(s.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at_cursor`;

/** Tenant scoping for statements on `schedules` keyed by id, with the tenant as $2. */
const OWNED_BY_TENANT_2 = 'queue_id IN (SELECT id FROM queues WHERE tenant_id = $2)';

function toStoredSchedule(row: ScheduleRow): StoredSchedule {
  return {
    id: row.id as ScheduleId,
    queueId: row.queue_id,
    tenantId: row.tenant_id as TenantId,
    queue: row.queue_name as QueueName,
    name: row.name,
    cron: row.cron_expression,
    timezone: row.timezone,
    payload: row.payload,
    priority: row.priority,
    maxAttempts: row.max_attempts,
    misfirePolicy: row.misfire_policy,
    enabled: row.enabled,
    nextRunAt: row.next_run_at,
    lastRunAt: row.last_run_at,
    lastJobId: row.last_job_id as JobId | null,
    fireCount: Number(row.fire_count),
    lastError: row.last_error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    createdAtCursor: row.created_at_cursor,
  };
}

/**
 * Data access for schedules: CRUD for the API, plus the two statements the
 * scheduler tick runs inside its transaction (claimDue, recordFire).
 *
 * State changes are conditional UPDATEs (`WHERE enabled` / `WHERE NOT
 * enabled`), never read-modify-write, and they queue behind a tick's row lock:
 * a schedule paused while a tick is evaluating it is paused right after that
 * tick commits, and the tick can never re-enable it.
 */
export class ScheduleRepository {
  constructor(private readonly db: Queryable) {}

  /** The database clock: the single time source for next_run_at. */
  async now(): Promise<Date> {
    const rows = await runQuery<{ now: Date }>(this.db, 'SELECT now() AS now');
    const row = rows[0];
    if (!row) throw new Error('SELECT now() returned no row');
    return row.now;
  }

  /**
   * Insert a schedule. A duplicate (queue, name) surfaces as ConflictError
   * with details.constraint = 'schedules_queue_name_uq'.
   */
  async create(params: InsertScheduleParams): Promise<StoredSchedule> {
    const rows = await runQuery<ScheduleRow>(
      this.db,
      `WITH inserted AS (
         INSERT INTO schedules (
           queue_id, name, cron_expression, timezone, payload, priority, max_attempts,
           misfire_policy, enabled, next_run_at
         )
         VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8::schedule_misfire_policy, $9, $10)
         RETURNING *
       )
       SELECT ${SCHEDULE_COLUMNS}
       FROM inserted s
       JOIN queues q ON q.id = s.queue_id`,
      [
        params.queueId,
        params.name,
        params.cronExpression,
        params.timezone,
        // Explicit serialization: node-postgres would turn a top-level array
        // into a Postgres array literal, not JSON.
        JSON.stringify(params.payload),
        params.priority,
        params.maxAttempts,
        params.misfirePolicy,
        params.nextRunAt !== null,
        params.nextRunAt,
      ],
    );
    return toStoredSchedule(requireRow(rows));
  }

  /** A schedule of `tenantId` by id; another tenant's schedule is null. */
  async findById(tenantId: TenantId, id: string): Promise<StoredSchedule | null> {
    const rows = await runQuery<ScheduleRow>(
      this.db,
      `SELECT ${SCHEDULE_COLUMNS}
       FROM schedules s
       JOIN queues q ON q.id = s.queue_id
       WHERE s.id = $1 AND q.tenant_id = $2`,
      [id, tenantId],
    );
    return rows[0] ? toStoredSchedule(rows[0]) : null;
  }

  /** Newest first, keyset-paginated on (created_at, id) via schedules_queue_created_idx. */
  async listByQueue(params: ListSchedulesParams): Promise<StoredSchedule[]> {
    const values: unknown[] = [params.queueId];
    const where = ['s.queue_id = $1'];
    if (params.enabled !== undefined) {
      values.push(params.enabled);
      where.push(`s.enabled = $${values.length}`);
    }
    if (params.after !== undefined) {
      values.push(params.after.createdAt, params.after.id);
      where.push(
        `(s.created_at, s.id) < ($${values.length - 1}::timestamptz, $${values.length}::uuid)`,
      );
    }
    values.push(params.limit);

    const rows = await runQuery<ScheduleRow>(
      this.db,
      `SELECT ${SCHEDULE_COLUMNS}
       FROM schedules s
       JOIN queues q ON q.id = s.queue_id
       WHERE ${where.join(' AND ')}
       ORDER BY s.created_at DESC, s.id DESC
       LIMIT $${values.length}`,
      values,
    );
    return rows.map(toStoredSchedule);
  }

  /** Disable an enabled schedule; null if it does not exist or is already paused. */
  async pause(tenantId: TenantId, id: string): Promise<StoredSchedule | null> {
    return this.updateReturning(
      `UPDATE schedules SET enabled = false, next_run_at = NULL
       WHERE id = $1 AND enabled AND ${OWNED_BY_TENANT_2}
       RETURNING *`,
      [id, tenantId],
    );
  }

  /**
   * Enable a paused schedule from `nextRunAt` (computed from now, so the
   * paused period is not replayed) and clear its last error; null if it does
   * not exist or is already enabled.
   */
  async resume(tenantId: TenantId, id: string, nextRunAt: Date): Promise<StoredSchedule | null> {
    return this.updateReturning(
      `UPDATE schedules SET enabled = true, next_run_at = $3, last_error = NULL
       WHERE id = $1 AND NOT enabled AND ${OWNED_BY_TENANT_2}
       RETURNING *`,
      [id, tenantId, nextRunAt],
    );
  }

  /** Delete a schedule (jobs it already enqueued are kept); false if unknown. */
  async delete(tenantId: TenantId, id: string): Promise<boolean> {
    const rows = await runQuery<{ id: string }>(
      this.db,
      `DELETE FROM schedules WHERE id = $1 AND ${OWNED_BY_TENANT_2} RETURNING id`,
      [id, tenantId],
    );
    return rows.length > 0;
  }

  /**
   * Scheduler tick: lock up to `limit` due schedules, most overdue first.
   * Must run inside the tick's transaction; the row locks are held until it
   * commits, so concurrent API pause/delete calls wait for the tick, and
   * SKIP LOCKED means a second ticking scheduler (which leader election
   * should prevent anyway) would take different rows, never the same ones.
   */
  async claimDue(limit: number): Promise<StoredSchedule[]> {
    const rows = await runQuery<ScheduleRow>(
      this.db,
      `SELECT ${SCHEDULE_COLUMNS}
       FROM schedules s
       JOIN queues q ON q.id = s.queue_id
       WHERE s.id IN (
         SELECT id FROM schedules
         WHERE enabled AND next_run_at <= now()
         ORDER BY next_run_at, id
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       )
       ORDER BY s.next_run_at, s.id`,
      [limit],
    );
    return rows.map(toStoredSchedule);
  }

  /**
   * Scheduler tick: store the outcome of evaluating a claimed schedule. The
   * `enabled` guard keeps the enabled <=> next_run_at invariant even if this
   * were ever called without holding the row lock.
   */
  async recordFire(params: RecordFireParams): Promise<boolean> {
    const rows = await runQuery<{ id: string }>(
      this.db,
      `UPDATE schedules
       SET next_run_at = $2,
           last_run_at = coalesce($3, last_run_at),
           last_job_id = coalesce($4::uuid, last_job_id),
           fire_count  = fire_count + $5,
           last_error  = $6
       WHERE id = $1 AND enabled
       RETURNING id`,
      [
        params.id,
        params.nextRunAt,
        params.lastRunAt,
        params.lastJobId,
        params.fired,
        params.lastError,
      ],
    );
    return rows.length > 0;
  }

  private async updateReturning(sql: string, values: unknown[]): Promise<StoredSchedule | null> {
    const rows = await runQuery<ScheduleRow>(
      this.db,
      `WITH updated AS (${sql})
       SELECT ${SCHEDULE_COLUMNS}
       FROM updated s
       JOIN queues q ON q.id = s.queue_id`,
      values,
    );
    return rows[0] ? toStoredSchedule(rows[0]) : null;
  }
}

function requireRow<T>(rows: T[]): T {
  const row = rows[0];
  if (row === undefined) throw new Error('INSERT ... RETURNING returned no row');
  return row;
}
