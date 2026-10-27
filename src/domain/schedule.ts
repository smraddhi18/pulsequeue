/**
 * Recurring schedules (Milestone 4): a cron expression in an IANA time zone
 * plus a job template. The scheduler leader turns each due occurrence into an
 * ordinary job, so everything downstream (leasing, retries, DLQ) is unchanged.
 */
import type { JobId, QueueName } from './job.js';
import type { TenantId } from './tenant.js';

export const MISFIRE_POLICIES = ['skip', 'fire_once', 'catch_up'] as const;

/** What to do with occurrences missed by more than the grace period; see scheduler/misfire.ts. */
export type MisfirePolicy = (typeof MISFIRE_POLICIES)[number];

export const SCHEDULE_LIMITS = {
  NAME_MAX_LENGTH: 128,
  DEFAULT_MISFIRE_POLICY: 'fire_once' as MisfirePolicy,
} as const;

/** Starts alphanumeric, then [A-Za-z0-9._:-], max 128 chars. Mirrors the DB CHECK. */
export const SCHEDULE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

export type ScheduleId = string & { readonly __brand: 'ScheduleId' };

export interface Schedule {
  id: ScheduleId;
  queue: QueueName;
  name: string;
  cron: string;
  timezone: string;
  payload: unknown;
  priority: number;
  maxAttempts: number;
  misfirePolicy: MisfirePolicy;
  enabled: boolean;
  /** Next occurrence to fire; null while paused. */
  nextRunAt: Date | null;
  /** Scheduled time of the latest occurrence that enqueued a job. */
  lastRunAt: Date | null;
  lastJobId: JobId | null;
  fireCount: number;
  /** Why the latest evaluation failed; cleared by the next successful fire. */
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
}

/** Validated create request (HTTP shape already checked by the route). */
export interface CreateScheduleInput {
  tenantId: TenantId;
  queue: QueueName;
  name: string;
  cron: string;
  timezone?: string;
  payload: unknown;
  priority?: number;
  maxAttempts?: number;
  misfirePolicy?: MisfirePolicy;
  /** false creates the schedule paused. Default true. */
  enabled?: boolean;
}

export interface ListSchedulesInput {
  tenantId: TenantId;
  queue: QueueName;
  enabled?: boolean;
  limit: number;
  cursor?: string;
}

export interface SchedulePage {
  schedules: Schedule[];
  nextCursor: string | null;
}

/**
 * Idempotency key of the job enqueued for one occurrence:
 * `schedule:<schedule id>:<occurrence, ISO-8601 UTC>`. Deterministic, so if
 * the same occurrence is ever evaluated twice (two leaders during a
 * partition, a retried tick), the jobs unique index turns the second enqueue
 * into a no-op instead of a duplicate job. 72 chars, well under the key limit.
 */
export function scheduleJobIdempotencyKey(scheduleId: string, occurrence: Date): string {
  return `schedule:${scheduleId}:${occurrence.toISOString()}`;
}
