import type { Queryable } from '../db/pool.js';
import { AppError } from '../domain/errors.js';
import { scheduleJobIdempotencyKey } from '../domain/schedule.js';
import { compileCron, InvalidCronError } from '../lib/cron.js';
import { requestFingerprint } from '../lib/fingerprint.js';
import type { Logger } from '../lib/logger.js';
import { JobRepository } from '../repositories/job.repository.js';
import { truncateError } from '../repositories/lease.repository.js';
import { ScheduleRepository, type StoredSchedule } from '../repositories/schedule.repository.js';
import { planFires } from './misfire.js';
import { uniqueQueueRefs, type QueueRef } from '../domain/signals.js';

/** A schedule whose expression cannot be evaluated is retried this much later. */
export const BROKEN_SCHEDULE_RETRY_MS = 60_000;

export interface TickOptions {
  batchSize: number;
  graceMs: number;
  maxCatchUp: number;
  logger: Logger;
  /**
   * Called after COMMIT with the distinct queues that received new jobs
   * (Milestone 5: wake workers, invalidate stats). Never called for a
   * rolled-back tick; a throwing hook is logged and does not fail the tick.
   */
  onCommitted?: (queues: QueueRef[]) => void;
}

export interface TickResult {
  /** Due schedules claimed (and evaluated) in this tick. */
  claimed: number;
  /** Jobs enqueued. */
  fired: number;
  /** Occurrences whose job already existed (idempotency key hit). */
  duplicates: number;
  /** Late occurrences dropped by misfire policies. */
  skipped: number;
  /** Schedules whose evaluation failed (recorded in last_error). */
  errors: number;
}

type TickEvent =
  | {
      kind: 'fired' | 'duplicate';
      schedule: StoredSchedule;
      jobId: string;
      scheduledFor: Date;
      now: Date;
    }
  | {
      kind: 'skipped';
      schedule: StoredSchedule;
      skipped: number;
      skippedMore: boolean;
      oldestSkipped: Date | null;
    }
  | { kind: 'error'; schedule: StoredSchedule; error: Error; retryAt: Date };

/**
 * One scheduler tick: a single transaction on the leader's session that
 *
 *   1. claims up to `batchSize` due schedules (FOR UPDATE SKIP LOCKED),
 *   2. per schedule, plans the occurrences to fire (misfire policy), inserts
 *      one job per occurrence with the deterministic idempotency key
 *      `schedule:<id>:<occurrence>`, and advances next_run_at,
 *   3. commits.
 *
 * Jobs and the schedule's new next_run_at commit together, so an occurrence
 * is enqueued exactly once even across crashes: either both are written or
 * neither is and the next leader re-evaluates the same occurrence. The
 * idempotency key is the second line of defence: even two concurrent ticks
 * (which leader election and SKIP LOCKED already prevent) cannot enqueue the
 * same occurrence twice.
 *
 * Each schedule is evaluated inside a SAVEPOINT. A schedule-level failure (an
 * expression that no longer parses, a job the database rejects) is rolled
 * back to the savepoint, recorded in last_error and retried later, without
 * affecting the other schedules in the batch. Anything else (connection loss,
 * timeouts) aborts the whole tick and is rethrown.
 *
 * Logs are written only after COMMIT so they never describe rolled-back work.
 */
export async function runTick(db: Queryable, options: TickOptions): Promise<TickResult> {
  const schedules = new ScheduleRepository(db);
  const jobs = new JobRepository(db);
  const events: TickEvent[] = [];

  await db.query('BEGIN');
  let claimed: StoredSchedule[];
  try {
    const now = await schedules.now();
    claimed = await schedules.claimDue(options.batchSize);
    for (const schedule of claimed) {
      await db.query('SAVEPOINT schedule_eval');
      try {
        await evaluate(schedule, now);
      } catch (err) {
        if (!isScheduleError(err)) throw err;
        await db.query('ROLLBACK TO SAVEPOINT schedule_eval');
        const retryAt = retryTime(schedule, now);
        await schedules.recordFire({
          id: schedule.id,
          nextRunAt: retryAt,
          lastRunAt: null,
          lastJobId: null,
          fired: 0,
          lastError: truncateError(err.message),
        });
        events.push({ kind: 'error', schedule, error: err, retryAt });
      }
      await db.query('RELEASE SAVEPOINT schedule_eval');
    }
    await db.query('COMMIT');
  } catch (err) {
    await db.query('ROLLBACK').catch(() => undefined);
    throw err;
  }

  const queues = uniqueQueueRefs(
    events
      .filter((e) => e.kind === 'fired')
      .map((e) => ({ tenantId: e.schedule.tenantId, name: e.schedule.queue })),
  );
  if (queues.length > 0 && options.onCommitted) {
    try {
      options.onCommitted(queues);
    } catch (err) {
      options.logger.warn({ err, queues }, 'Tick onCommitted hook threw');
    }
  }
  return report(events, claimed.length, options.logger);

  async function evaluate(schedule: StoredSchedule, now: Date): Promise<void> {
    // claimDue only returns enabled rows, and enabled implies next_run_at.
    const nextRunAt = schedule.nextRunAt ?? now;
    const cron = compileCron(schedule.cron, schedule.timezone);
    const plan = planFires({
      cron,
      nextRunAt,
      now,
      policy: schedule.misfirePolicy,
      graceMs: options.graceMs,
      maxCatchUp: options.maxCatchUp,
    });
    if (plan.skipped > 0) {
      events.push({
        kind: 'skipped',
        schedule,
        skipped: plan.skipped,
        skippedMore: plan.skippedMore,
        oldestSkipped: plan.oldestSkipped,
      });
    }

    const fingerprint = requestFingerprint({
      payload: schedule.payload,
      priority: schedule.priority,
      delayMs: 0,
      maxAttempts: schedule.maxAttempts,
    });
    let fired = 0;
    let lastRunAt: Date | null = null;
    let lastJobId: string | null = null;
    for (const scheduledFor of plan.fireTimes) {
      const { job, inserted } = await jobs.insertIdempotent({
        queueId: schedule.queueId,
        status: 'queued',
        priority: schedule.priority,
        payload: schedule.payload,
        maxAttempts: schedule.maxAttempts,
        delayMs: 0,
        // Clamped to now() by the insert: the job is runnable immediately.
        runAt: scheduledFor,
        idempotencyKey: scheduleJobIdempotencyKey(schedule.id, scheduledFor),
        requestFingerprint: fingerprint,
      });
      if (inserted) fired++;
      lastRunAt = scheduledFor;
      lastJobId = job.id;
      events.push({
        kind: inserted ? 'fired' : 'duplicate',
        schedule,
        jobId: job.id,
        scheduledFor,
        now,
      });
    }

    await schedules.recordFire({
      id: schedule.id,
      nextRunAt: plan.nextRunAt,
      lastRunAt,
      lastJobId,
      fired,
      lastError: null,
    });
  }
}

/** Errors that belong to one schedule (bad definition or rejected data), not to the tick. */
function isScheduleError(err: unknown): err is Error {
  if (err instanceof InvalidCronError) return true;
  return err instanceof AppError && err.statusCode < 500;
}

/** Skip to the next occurrence; if the expression itself is broken, retry later. */
function retryTime(schedule: StoredSchedule, now: Date): Date {
  try {
    return compileCron(schedule.cron, schedule.timezone).next(now);
  } catch {
    return new Date(now.getTime() + BROKEN_SCHEDULE_RETRY_MS);
  }
}

function report(events: TickEvent[], claimed: number, logger: Logger): TickResult {
  const result: TickResult = { claimed, fired: 0, duplicates: 0, skipped: 0, errors: 0 };
  for (const e of events) {
    const base = {
      scheduleId: e.schedule.id,
      name: e.schedule.name,
      tenantId: e.schedule.tenantId,
      queue: e.schedule.queue,
    };
    switch (e.kind) {
      case 'fired':
      case 'duplicate': {
        const fields = {
          ...base,
          event: e.kind === 'fired' ? 'scheduler.fired' : 'scheduler.duplicate_suppressed',
          jobId: e.jobId,
          scheduledFor: e.scheduledFor.toISOString(),
          lagMs: e.now.getTime() - e.scheduledFor.getTime(),
        };
        if (e.kind === 'fired') {
          result.fired++;
          logger.info(fields, 'Schedule fired');
        } else {
          result.duplicates++;
          logger.warn(fields, 'Schedule occurrence already enqueued; duplicate suppressed');
        }
        break;
      }
      case 'skipped':
        result.skipped += e.skipped;
        logger.warn(
          {
            ...base,
            event: 'scheduler.misfire_skipped',
            policy: e.schedule.misfirePolicy,
            skipped: e.skipped,
            skippedMore: e.skippedMore,
            oldestSkipped: e.oldestSkipped?.toISOString() ?? null,
          },
          'Missed schedule occurrences dropped by misfire policy',
        );
        break;
      case 'error':
        result.errors++;
        logger.error(
          {
            ...base,
            event: 'scheduler.schedule_error',
            err: e.error,
            retryAt: e.retryAt.toISOString(),
          },
          'Schedule evaluation failed',
        );
        break;
    }
  }
  return result;
}
