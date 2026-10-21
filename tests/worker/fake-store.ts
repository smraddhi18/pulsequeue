import pino from 'pino';
import type { JobId, QueueName } from '../../src/domain/job.js';
import type { LeaseRef, LeasedJob } from '../../src/domain/lease.js';
import type {
  FailAttemptParams,
  LeaseJobsParams,
} from '../../src/repositories/lease.repository.js';
import type { LeaseStore } from '../../src/worker/worker.js';

export const silentLogger = pino({ level: 'silent' });

export interface FakeJob {
  id: string;
  queue: string;
  payload: unknown;
  attempts: number;
  maxAttempts: number;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'scheduled';
  leaseToken: string | null;
  retryDelays: number[];
  errors: string[];
  attemptStatuses: string[];
}

/**
 * In-memory LeaseStore with the same contract as LeaseRepository (fencing on
 * lease token), so the worker loop is tested without Postgres.
 */
export class FakeLeaseStore implements LeaseStore {
  readonly jobs = new Map<string, FakeJob>();
  leaseCalls: LeaseJobsParams[] = [];
  heartbeats = 0;
  /** Make the next N lease calls throw. */
  failLeases = 0;
  /** Heartbeat returns null (lease gone) when true. */
  loseLeases = false;
  /** Called at the start of every lease() call (e.g. to trigger stop mid-lease). */
  onLease: (() => void) | null = null;
  maxConcurrentRunning = 0;
  private seq = 0;

  add(queue: string, payload: unknown = {}, maxAttempts = 3): FakeJob {
    const job: FakeJob = {
      id: `job-${++this.seq}`,
      queue,
      payload,
      attempts: 0,
      maxAttempts,
      status: 'queued',
      leaseToken: null,
      retryDelays: [],
      errors: [],
      attemptStatuses: [],
    };
    this.jobs.set(job.id, job);
    return job;
  }

  running(): number {
    return [...this.jobs.values()].filter((j) => j.status === 'running').length;
  }

  async lease(params: LeaseJobsParams): Promise<LeasedJob[]> {
    this.leaseCalls.push(params);
    this.onLease?.();
    if (this.failLeases > 0) {
      this.failLeases--;
      throw new Error('database unavailable');
    }
    const out: LeasedJob[] = [];
    for (const job of this.jobs.values()) {
      if (out.length >= params.limit) break;
      // Retries are runnable immediately in the fake (no clock).
      if (
        (job.status !== 'queued' && job.status !== 'scheduled') ||
        !params.queues.includes(job.queue)
      )
        continue;
      job.status = 'running';
      job.attempts++;
      job.leaseToken = `tok-${++this.seq}`;
      job.attemptStatuses.push('running');
      out.push({
        jobId: job.id as JobId,
        queue: job.queue as QueueName,
        payload: job.payload,
        priority: 0,
        attempt: job.attempts,
        maxAttempts: job.maxAttempts,
        attemptNumber: job.attemptStatuses.length,
        attemptId: `att-${this.seq}`,
        leaseToken: job.leaseToken,
        workerId: params.workerId,
        leaseExpiresAt: new Date(Date.now() + params.leaseMs),
        createdAt: new Date(),
      });
    }
    this.maxConcurrentRunning = Math.max(this.maxConcurrentRunning, this.running());
    return out;
  }

  private owned(lease: LeaseRef): FakeJob | null {
    const job = this.jobs.get(lease.jobId);
    return job && job.status === 'running' && job.leaseToken === lease.leaseToken ? job : null;
  }

  private settleAttempt(job: FakeJob, status: string): void {
    job.attemptStatuses[job.attemptStatuses.length - 1] = status;
    job.leaseToken = null;
  }

  async heartbeat(lease: LeaseRef, leaseMs: number): Promise<Date | null> {
    this.heartbeats++;
    if (this.loseLeases || !this.owned(lease)) return null;
    return new Date(Date.now() + leaseMs);
  }

  async complete(lease: LeaseRef): Promise<boolean> {
    const job = this.owned(lease);
    if (!job) return false;
    job.status = 'succeeded';
    this.settleAttempt(job, 'succeeded');
    return true;
  }

  async fail(lease: LeaseRef, params: FailAttemptParams): Promise<'retrying' | 'failed' | null> {
    const job = this.owned(lease);
    if (!job) return null;
    job.errors.push(params.error);
    this.settleAttempt(job, params.attemptStatus);
    if (params.retryDelayMs === null) {
      job.status = 'failed';
      return 'failed';
    }
    job.retryDelays.push(params.retryDelayMs);
    job.status = 'scheduled';
    return 'retrying';
  }

  async release(lease: LeaseRef): Promise<boolean> {
    const job = this.owned(lease);
    if (!job) return false;
    job.status = 'queued';
    job.attempts--;
    this.settleAttempt(job, 'released');
    return true;
  }
}

export async function waitFor(
  predicate: () => boolean,
  { timeoutMs = 3_000, intervalMs = 5 } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
