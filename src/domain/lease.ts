import type { DeadLetterReason } from './dead-letter.js';
import type { JobId, QueueName } from './job.js';
import type { QueueRef } from './signals.js';
import type { TenantId } from './tenant.js';

/**
 * Leasing model (Milestone 2).
 *
 * A worker owns a running job through a lease that expires unless it is
 * extended by heartbeats. The lease token is a fencing token: it changes on
 * every lease, and every write a worker makes afterwards is conditional on it,
 * so a worker that lost its lease (expired and re-leased elsewhere) can never
 * overwrite the newer owner's outcome.
 */

/** Identifies one lease of one job; the only handle a worker needs to write back. */
export interface LeaseRef {
  jobId: JobId;
  leaseToken: string;
  attemptId: string;
}

/** A job as handed to a worker: what the handler needs plus the lease. */
export interface LeasedJob extends LeaseRef {
  /** Owner of the job's queue (M6): workers serve every tenant's queue of a name. */
  tenantId: TenantId;
  queue: QueueName;
  /**
   * The queue has a max_concurrency (M6). Finishing such a job frees a slot
   * other workers may be waiting for, so it is announced like new work.
   * Optional: absent means uncapped.
   */
  queueCapped?: boolean;
  payload: unknown;
  priority: number;
  /** Counted attempts including this one (1-based); drives retry decisions. */
  attempt: number;
  maxAttempts: number;
  /** Row number in job_attempts (counts released attempts too). */
  attemptNumber: number;
  workerId: string;
  leaseExpiresAt: Date;
  createdAt: Date;
}

export type AttemptOutcome = 'succeeded' | 'failed' | 'timed_out' | 'released';

/** Where a failed attempt left the job. */
export type FailureDisposition =
  /** Rescheduled after a backoff delay (status = scheduled). */
  | 'retrying'
  /** Terminal: moved to the dead-letter queue (status = dead) in the same statement. */
  | 'dead_lettered';

/** A job whose expired lease the reaper reclaimed. */
export interface ReclaimedJob {
  jobId: JobId;
  tenantId: TenantId;
  queue: QueueName;
  /** The worker that held the expired lease. */
  previousWorkerId: string;
  attempt: number;
  maxAttempts: number;
  /** queued = will run again; dead = dead-lettered (last attempt, or poison). */
  status: 'queued' | 'dead';
  /** Why it was dead-lettered; null when requeued. */
  deadLetterReason: DeadLetterReason | null;
  /** Lease expiries so far, including this one (poison-message counter). */
  leaseExpiries: number;
}

/** The queue a leased or reclaimed job belongs to, for signals. */
export function jobQueueRef(job: { tenantId: TenantId; queue: QueueName }): QueueRef {
  return { tenantId: job.tenantId, name: job.queue };
}
