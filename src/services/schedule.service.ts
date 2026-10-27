import {
  ConflictError,
  NotFoundError,
  PayloadTooLargeError,
  fieldValidationError,
} from '../domain/errors.js';
import { JOB_LIMITS } from '../domain/job.js';
import {
  SCHEDULE_LIMITS,
  type CreateScheduleInput,
  type ListSchedulesInput,
  type Schedule,
  type SchedulePage,
} from '../domain/schedule.js';
import { compileCron, CRON_LIMITS, InvalidCronError, type CompiledCron } from '../lib/cron.js';
import { decodeCursor, encodeCursor } from '../lib/cursor.js';
import type { ScheduleRepository, StoredSchedule } from '../repositories/schedule.repository.js';
import type { QueueStore } from './job.service.js';
import type { TenantId } from '../domain/tenant.js';

/** The slice of ScheduleRepository the service needs; lets tests pass in-memory fakes. */
export type ScheduleStore = Pick<
  ScheduleRepository,
  'now' | 'create' | 'findById' | 'listByQueue' | 'pause' | 'resume' | 'delete'
>;

export interface ScheduleServiceOptions {
  /** Same budget as enqueued jobs: every fire copies the payload into a job. */
  maxPayloadBytes: number;
}

/**
 * Business rules for schedules: cron/time-zone validation, defaults, the
 * payload budget, first occurrence, and idempotent pause/resume. Occurrence
 * times always come from the database clock, the same clock the scheduler
 * compares next_run_at against. No SQL, no HTTP.
 */
export class ScheduleService {
  constructor(
    private readonly queues: QueueStore,
    private readonly schedules: ScheduleStore,
    private readonly options: ScheduleServiceOptions,
  ) {}

  async create(input: CreateScheduleInput): Promise<Schedule> {
    const cron = compile(input.cron, input.timezone ?? CRON_LIMITS.DEFAULT_TIMEZONE);
    const payloadBytes = Buffer.byteLength(JSON.stringify(input.payload), 'utf8');
    if (payloadBytes > this.options.maxPayloadBytes) {
      throw new PayloadTooLargeError('Schedule payload exceeds the size limit', {
        limitBytes: this.options.maxPayloadBytes,
        actualBytes: payloadBytes,
      });
    }

    const queue = await this.queues.ensure(input.tenantId, input.queue);
    const enabled = input.enabled ?? true;
    const nextRunAt = enabled ? cron.next(await this.schedules.now()) : null;
    try {
      const stored = await this.schedules.create({
        queueId: queue.id,
        name: input.name,
        cronExpression: cron.expression,
        timezone: cron.timezone,
        payload: input.payload,
        priority: input.priority ?? JOB_LIMITS.DEFAULT_PRIORITY,
        maxAttempts: input.maxAttempts ?? JOB_LIMITS.DEFAULT_MAX_ATTEMPTS,
        misfirePolicy: input.misfirePolicy ?? SCHEDULE_LIMITS.DEFAULT_MISFIRE_POLICY,
        nextRunAt,
      });
      return toSchedule(stored);
    } catch (err) {
      if (isConstraintConflict(err, 'schedules_queue_name_uq')) {
        throw new ConflictError(
          `A schedule named "${input.name}" already exists in queue "${input.queue}"`,
          { queue: input.queue, name: input.name },
        );
      }
      throw err;
    }
  }

  /** Another tenant's schedule is indistinguishable from a missing one (404). */
  async get(tenantId: TenantId, id: string): Promise<Schedule> {
    return toSchedule(await this.require(tenantId, id));
  }

  async list(input: ListSchedulesInput): Promise<SchedulePage> {
    // Decode first: a malformed cursor is a 400 regardless of the queue.
    const after = input.cursor !== undefined ? decodeCursor(input.cursor) : undefined;
    // Reads never create queues; an unknown queue is a 404, not an empty page.
    const queue = await this.queues.findByName(input.tenantId, input.queue);
    if (!queue) throw new NotFoundError('Queue not found', { queue: input.queue });

    const rows = await this.schedules.listByQueue({
      queueId: queue.id,
      ...(input.enabled !== undefined ? { enabled: input.enabled } : {}),
      limit: input.limit + 1,
      ...(after !== undefined ? { after } : {}),
    });
    const page = rows.slice(0, input.limit);
    const last = page[page.length - 1];
    const nextCursor =
      rows.length > input.limit && last
        ? encodeCursor({ createdAt: last.createdAtCursor, id: last.id })
        : null;
    return { schedules: page.map(toSchedule), nextCursor };
  }

  /** Stop firing. Idempotent: pausing a paused schedule returns it unchanged. */
  async pause(tenantId: TenantId, id: string): Promise<Schedule> {
    const paused = await this.schedules.pause(tenantId, id);
    return toSchedule(paused ?? (await this.require(tenantId, id)));
  }

  /**
   * Fire again from the next occurrence after now: occurrences that fell into
   * the paused period are not replayed (pausing is a deliberate act, not a
   * misfire). Idempotent: resuming an enabled schedule returns it unchanged.
   */
  async resume(tenantId: TenantId, id: string): Promise<Schedule> {
    const current = await this.require(tenantId, id);
    if (current.enabled) return toSchedule(current);
    const cron = compile(current.cron, current.timezone);
    const resumed = await this.schedules.resume(
      tenantId,
      id,
      cron.next(await this.schedules.now()),
    );
    // null: a concurrent resume won (or a delete); report the current state.
    return toSchedule(resumed ?? (await this.require(tenantId, id)));
  }

  /** Remove a schedule; jobs it already enqueued are unaffected. */
  async delete(tenantId: TenantId, id: string): Promise<void> {
    if (!(await this.schedules.delete(tenantId, id))) {
      throw new NotFoundError('Schedule not found', { scheduleId: id });
    }
  }

  private async require(tenantId: TenantId, id: string): Promise<StoredSchedule> {
    const stored = await this.schedules.findById(tenantId, id);
    if (!stored) throw new NotFoundError('Schedule not found', { scheduleId: id });
    return stored;
  }
}

/** Compile or throw a field-level 400 (`cron` or `timezone`). */
function compile(expression: string, timezone: string): CompiledCron {
  try {
    return compileCron(expression, timezone);
  } catch (err) {
    if (err instanceof InvalidCronError) throw fieldValidationError(err.field, err.message);
    throw err;
  }
}

function isConstraintConflict(err: unknown, constraint: string): boolean {
  if (!(err instanceof ConflictError)) return false;
  const details = err.details;
  return (
    typeof details === 'object' &&
    details !== null &&
    (details as { constraint?: unknown }).constraint === constraint
  );
}

/** Strip storage-only fields before a schedule leaves the service layer. */
function toSchedule(stored: StoredSchedule): Schedule {
  const { queueId: _queueId, tenantId: _tenantId, createdAtCursor: _cursor, ...schedule } = stored;
  return schedule;
}
