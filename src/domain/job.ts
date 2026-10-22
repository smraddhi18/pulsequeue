/**
 * Job domain model and the limits that define a valid job.
 * Postgres enforces the structural invariants; these constants are the
 * product policy the API applies on top (they may be tighter than the DB).
 */

export const JOB_STATUSES = [
  'queued',
  'scheduled',
  'running',
  'succeeded',
  'failed',
  'dead',
] as const;

/**
 * - queued:    runnable now
 * - scheduled: waiting for runAt (delayed enqueue or retry backoff)
 * - running:   leased by a worker, lease kept alive by heartbeats
 * - succeeded: terminal success
 * - failed:    legacy (M2). Since Milestone 3 no code path writes it: every
 *              terminal failure is dead-lettered instead (migration 0003
 *              moved existing `failed` rows to `dead`). Kept because Postgres
 *              enum values cannot be dropped safely.
 * - dead:      terminal, in the dead-letter queue with a reason; can be
 *              replayed (back to queued/scheduled) or purged (M3)
 */
export type JobStatus = (typeof JOB_STATUSES)[number];

export const JOB_LIMITS = {
  /** Higher runs first. */
  MIN_PRIORITY: 0,
  MAX_PRIORITY: 100,
  DEFAULT_PRIORITY: 0,
  MIN_MAX_ATTEMPTS: 1,
  MAX_MAX_ATTEMPTS: 25,
  DEFAULT_MAX_ATTEMPTS: 3,
  /** 30 days. Longer horizons belong to the scheduler (M4), not a delay. */
  MAX_DELAY_MS: 30 * 24 * 60 * 60 * 1000,
  IDEMPOTENCY_KEY_MAX_LENGTH: 255,
} as const;

export const PAGINATION_LIMITS = {
  DEFAULT_LIMIT: 50,
  MAX_LIMIT: 200,
} as const;

/** Lowercase, starts alphanumeric, then [a-z0-9._-], max 64 chars. Mirrors the DB CHECK. */
export const QUEUE_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export type JobId = string & { readonly __brand: 'JobId' };
export type QueueName = string & { readonly __brand: 'QueueName' };

export interface Queue {
  id: string;
  name: QueueName;
  createdAt: Date;
}

export interface Job {
  id: JobId;
  queue: QueueName;
  status: JobStatus;
  payload: unknown;
  priority: number;
  attempts: number;
  maxAttempts: number;
  runAt: Date;
  idempotencyKey: string | null;
  lastError: string | null;
  finishedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
}

/** Validated enqueue request (HTTP shape already checked by the route). */
export interface EnqueueJobInput {
  queue: QueueName;
  payload: unknown;
  priority?: number;
  delayMs?: number;
  maxAttempts?: number;
  idempotencyKey?: string;
}

export interface EnqueueJobResult {
  job: Job;
  /** false when an Idempotency-Key matched an existing job and nothing was inserted. */
  created: boolean;
}

export interface ListJobsInput {
  queue: QueueName;
  status?: JobStatus;
  limit: number;
  cursor?: string;
}

export interface JobPage {
  jobs: Job[];
  nextCursor: string | null;
}
