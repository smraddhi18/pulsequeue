import type { Job, JobId, QueueName } from './job.js';

/**
 * Dead-letter queue model (Milestone 3).
 *
 * A dead-lettered job stays in `jobs` with status `dead` plus DLQ metadata,
 * rather than being copied to a separate table: its id, payload, idempotency
 * key and full job_attempts history stay exactly where they were, and a
 * replay is a single fenced UPDATE of the same row (see migration 0003).
 */

export const DEAD_LETTER_REASONS = [
  /** Every counted attempt failed (thrown error, attempt timeout or lease expiry). */
  'attempts_exhausted',
  /** The handler threw a NonRetryableError (or an error flagged `retryable: false`). */
  'non_retryable',
  /** The handler rejected the payload (InvalidPayloadError): it can never succeed as-is. */
  'invalid_payload',
  /**
   * Poison message: the job's lease expired WORKER_POISON_LEASE_EXPIRIES times,
   * i.e. it keeps crashing or hanging its worker. Dead-lettered even with
   * attempts left so one bad job cannot take down worker after worker.
   */
  'poison_lease_expired',
] as const;

export type DeadLetterReason = (typeof DEAD_LETTER_REASONS)[number];

export const DLQ_LIMITS = {
  /** Jobs replayed by one bulk call; keeps the statement and its row locks short. */
  DEFAULT_BULK_REPLAY: 100,
  MAX_BULK_REPLAY: 1_000,
  /** Jobs deleted by one bulk purge call (attempt rows cascade, so keep batches bounded). */
  DEFAULT_BULK_PURGE: 1_000,
  MAX_BULK_PURGE: 10_000,
} as const;

/** DLQ metadata of a job whose status is `dead`. */
export interface DeadLetterInfo {
  reason: DeadLetterReason;
  deadLetteredAt: Date;
  /** Counted attempts used when it was dead-lettered. */
  attempts: number;
  lastError: string | null;
  /** The queue it was dead-lettered from (and is replayed into). */
  queue: QueueName;
  /** Times it was replayed out of the DLQ before (0 = first time dead). */
  replayCount: number;
  lastReplayedAt: Date | null;
}

export interface DeadLetteredJob extends Job {
  status: 'dead';
  deadLetter: DeadLetterInfo;
}

/** One row of job_attempts, oldest first. */
export interface JobAttempt {
  attemptNumber: number;
  status: 'running' | 'succeeded' | 'failed' | 'timed_out' | 'released';
  workerId: string | null;
  error: string | null;
  startedAt: Date;
  finishedAt: Date | null;
}

export interface DeadLetteredJobWithAttempts extends DeadLetteredJob {
  attemptHistory: JobAttempt[];
}

export interface ListDeadLettersInput {
  queue: QueueName;
  reason?: DeadLetterReason;
  limit: number;
  cursor?: string;
}

export interface DeadLetterPage {
  jobs: DeadLetteredJob[];
  nextCursor: string | null;
}

export interface ReplayJobInput {
  queue: QueueName;
  jobId: JobId;
  /** Run after this delay instead of immediately. */
  delayMs?: number;
}

export interface BulkReplayInput {
  queue: QueueName;
  limit: number;
  reason?: DeadLetterReason;
  delayMs?: number;
}

export interface BulkReplayResult {
  replayed: number;
  jobIds: JobId[];
}

export interface PurgeDeadLettersInput {
  queue: QueueName;
  /** Only jobs dead-lettered strictly before this instant. */
  olderThan: Date;
  limit: number;
}

export interface PurgeResult {
  purged: number;
}
