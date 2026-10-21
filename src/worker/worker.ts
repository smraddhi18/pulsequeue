import { writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import type { LeasedJob } from '../domain/lease.js';
import type { Logger } from '../lib/logger.js';
import type { LeaseRepository } from '../repositories/lease.repository.js';
import type { BackoffOptions } from './backoff.js';
import {
  HandlerNotFoundError,
  JobTimeoutError,
  LeaseLostError,
  ShutdownAbortError,
  describeError,
} from './errors.js';
import type { HandlerRegistry, JobContext } from './registry.js';
import { decideRetry } from './retry-policy.js';
import { Sleeper, jitter } from './sleeper.js';

/** The slice of LeaseRepository the worker needs; unit tests pass an in-memory fake. */
export type LeaseStore = Pick<
  LeaseRepository,
  'lease' | 'heartbeat' | 'complete' | 'fail' | 'release'
>;

/** A job reached a terminal failure. Milestone 3 moves it to the dead-letter queue from here. */
export interface JobFailedEvent {
  jobId: string;
  queue: string;
  attempt: number;
  maxAttempts: number;
  error: string;
  reason: 'non_retryable' | 'attempts_exhausted';
  source: 'worker' | 'reaper';
}

export type JobFailedHook = (event: JobFailedEvent) => void | Promise<void>;

export interface WorkerOptions {
  store: LeaseStore;
  registry: HandlerRegistry;
  logger: Logger;
  workerId: string;
  /** Queues to lease from; each must have a handler (see HandlerRegistry.resolveQueues). */
  queues: readonly string[];
  concurrency: number;
  pollIntervalMs: number;
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  jobTimeoutMs: number;
  backoff: BackoffOptions;
  /** Dead-letter hook (Milestone 3). Errors thrown here are logged, never fatal. */
  onJobFailed?: JobFailedHook;
  /** Touched (throttled) on every loop iteration; a container healthcheck reads its mtime. */
  livenessFile?: string;
  /** Injectable randomness for deterministic tests (backoff and poll jitter). */
  random?: () => number;
}

export interface StopResult {
  /** true when every in-flight job finished within the drain timeout. */
  drained: boolean;
  /** Jobs that were still running at the timeout; their leases were released. */
  abandoned: number;
}

interface InFlight {
  job: LeasedJob;
  controller: AbortController;
  promise: Promise<void>;
}

/** Cap for the lease-error backoff so a database outage is retried at least every 30s. */
const MAX_ERROR_BACKOFF_MS = 30_000;
const LIVENESS_TOUCH_INTERVAL_MS = 5_000;

/**
 * Leases jobs and runs them with bounded concurrency.
 *
 * Loop: while there are free slots, lease up to `free` jobs in one short
 * statement (FOR UPDATE SKIP LOCKED) and start them without awaiting. A full
 * batch re-polls immediately (backlog); a partial or empty batch sleeps for
 * the jittered poll interval; with no free slot the loop sleeps until a job
 * finishes. Each running job heartbeats its lease and is bounded by a
 * timeout. Outcomes are written back fenced on the lease token.
 *
 * Delivery is at-least-once: a job can run again after a crash, a lost lease
 * or a timeout, so handlers must be idempotent.
 */
export class Worker {
  private readonly sleeper = new Sleeper();
  private readonly inFlight = new Map<string, InFlight>();
  private readonly random: () => number;
  private readonly logger: Logger;
  private loopPromise: Promise<void> | null = null;
  private stopPromise: Promise<StopResult> | null = null;
  private stopping = false;
  private waitingForSlot = false;
  private lastLivenessTouch = 0;

  constructor(private readonly options: WorkerOptions) {
    if (options.queues.length === 0) throw new Error('Worker needs at least one queue');
    if (options.concurrency < 1) throw new Error('Worker concurrency must be >= 1');
    if (options.heartbeatIntervalMs * 2 > options.leaseDurationMs) {
      throw new Error('heartbeatIntervalMs must be at most half of leaseDurationMs');
    }
    this.random = options.random ?? Math.random;
    this.logger = options.logger.child({ workerId: options.workerId });
  }

  get workerId(): string {
    return this.options.workerId;
  }

  /** Jobs currently executing in this process. */
  get activeJobs(): number {
    return this.inFlight.size;
  }

  get isRunning(): boolean {
    return this.loopPromise !== null && !this.stopping;
  }

  start(): void {
    if (this.loopPromise) throw new Error('Worker already started');
    this.logger.info(
      {
        queues: this.options.queues,
        concurrency: this.options.concurrency,
        leaseDurationMs: this.options.leaseDurationMs,
        heartbeatIntervalMs: this.options.heartbeatIntervalMs,
        jobTimeoutMs: this.options.jobTimeoutMs,
      },
      'Worker started',
    );
    this.loopPromise = this.runLoop();
  }

  /**
   * Graceful stop: stop leasing, let in-flight jobs finish for up to
   * `timeoutMs`, then abort the rest and release their leases so another
   * worker picks them up at once (instead of after the lease expires).
   * Idempotent: a second call returns the first call's promise.
   */
  stop(options: { timeoutMs: number }): Promise<StopResult> {
    this.stopPromise ??= this.shutdown(options.timeoutMs);
    return this.stopPromise;
  }

  private async shutdown(timeoutMs: number): Promise<StopResult> {
    this.stopping = true;
    this.sleeper.wake();
    await this.loopPromise;

    const inFlight = [...this.inFlight.values()];
    this.logger.info({ inFlight: inFlight.length, timeoutMs }, 'Stopped leasing, draining jobs');

    const drained = await settleWithin(
      inFlight.map((f) => f.promise),
      timeoutMs,
    );
    let abandoned = 0;
    if (!drained) {
      const remaining = [...this.inFlight.values()];
      abandoned = remaining.length;
      this.logger.warn(
        { abandoned, timeoutMs },
        'Drain timeout reached, aborting jobs and releasing their leases',
      );
      for (const f of remaining) f.controller.abort(new ShutdownAbortError());
      await Promise.allSettled(remaining.map((f) => f.promise));
    }
    this.logger.info({ drained, abandoned }, 'Worker stopped');
    return { drained, abandoned };
  }

  private async runLoop(): Promise<void> {
    const { store, queues, workerId, concurrency, pollIntervalMs, leaseDurationMs } = this.options;
    let consecutiveErrors = 0;

    while (!this.stopping) {
      this.touchLiveness();
      const free = concurrency - this.inFlight.size;
      if (free <= 0) {
        // Woken early by the next job to finish (see dispatch).
        this.waitingForSlot = true;
        await this.sleeper.sleep(pollIntervalMs);
        this.waitingForSlot = false;
        continue;
      }

      let jobs: LeasedJob[];
      try {
        jobs = await store.lease({ queues, workerId, limit: free, leaseMs: leaseDurationMs });
        consecutiveErrors = 0;
      } catch (err) {
        consecutiveErrors++;
        const delay = Math.min(
          pollIntervalMs * 2 ** Math.min(consecutiveErrors - 1, 10),
          MAX_ERROR_BACKOFF_MS,
        );
        this.logger.error({ err, consecutiveErrors, retryInMs: delay }, 'Lease query failed');
        if (!this.stopping) await this.sleeper.sleep(jitter(delay, 0.2, this.random));
        continue;
      }

      if (this.stopping) {
        // stop() raced the lease query: hand these back untouched.
        await Promise.all(
          jobs.map((job) =>
            this.releaseLease(job, this.jobLogger(job), 'Leased while the worker was stopping'),
          ),
        );
        break;
      }

      if (jobs.length > 0) {
        this.logger.debug({ leased: jobs.length, free }, 'Leased jobs');
      }
      for (const job of jobs) this.dispatch(job);

      // A full batch means there is probably more work: poll again at once.
      if (jobs.length < free && !this.stopping) {
        await this.sleeper.sleep(jitter(pollIntervalMs, 0.2, this.random));
      }
    }
  }

  private dispatch(job: LeasedJob): void {
    const controller = new AbortController();
    const promise = this.execute(job, controller)
      .catch((err: unknown) => {
        // execute() handles its own errors; this is a last-resort guard.
        this.jobLogger(job).error({ err }, 'Unexpected error while running job');
      })
      .finally(() => {
        this.inFlight.delete(job.attemptId);
        if (this.waitingForSlot) this.sleeper.wake();
      });
    this.inFlight.set(job.attemptId, { job, controller, promise });
  }

  private async execute(job: LeasedJob, controller: AbortController): Promise<void> {
    const log = this.jobLogger(job);
    const handler = this.options.registry.get(job.queue);
    const startedAt = performance.now();
    log.info('Job attempt started');

    const heartbeat = this.startHeartbeat(job, controller, log);
    const timeout = setTimeout(
      () => controller.abort(new JobTimeoutError(this.options.jobTimeoutMs)),
      this.options.jobTimeoutMs,
    );

    let succeeded = false;
    let thrown: unknown;
    try {
      if (!handler) throw new HandlerNotFoundError(job.queue);
      const ctx: JobContext = {
        job: {
          id: job.jobId,
          queue: job.queue,
          payload: job.payload,
          priority: job.priority,
          attempt: job.attempt,
          maxAttempts: job.maxAttempts,
          createdAt: job.createdAt,
        },
        signal: controller.signal,
        logger: log,
      };
      await runUntilAborted(() => handler(ctx), controller.signal);
      succeeded = true;
    } catch (err) {
      thrown = err;
    } finally {
      clearTimeout(timeout);
      heartbeat.stop();
    }

    const durationMs = Math.round(performance.now() - startedAt);
    const abortReason: unknown = controller.signal.aborted ? controller.signal.reason : undefined;

    if (succeeded) {
      await this.persist(log, 'complete', async () => {
        if (await this.options.store.complete(job)) {
          log.info({ durationMs, outcome: 'succeeded' }, 'Job attempt succeeded');
        } else {
          log.warn({ durationMs }, 'Job finished but its lease was lost; result discarded');
        }
      });
      return;
    }

    if (abortReason instanceof LeaseLostError) {
      log.warn({ durationMs }, 'Lease lost while running; stopped without writing an outcome');
      return;
    }

    if (abortReason instanceof ShutdownAbortError) {
      await this.releaseLease(job, log, abortReason.message);
      return;
    }

    const error = abortReason instanceof JobTimeoutError ? abortReason : thrown;
    const attemptStatus = abortReason instanceof JobTimeoutError ? 'timed_out' : 'failed';
    const message = describeError(error);
    const decision = decideRetry(
      { attempt: job.attempt, maxAttempts: job.maxAttempts, error },
      this.options.backoff,
      this.random,
    );
    const retryDelayMs = decision.kind === 'retry' ? decision.delayMs : null;

    await this.persist(log, 'fail', async () => {
      const disposition = await this.options.store.fail(job, {
        error: message,
        attemptStatus,
        retryDelayMs,
      });
      if (disposition === null) {
        log.warn({ durationMs, err: error }, 'Job failed but its lease was lost; nothing written');
        return;
      }
      if (decision.kind === 'retry') {
        log.warn(
          { durationMs, err: error, outcome: attemptStatus, retryInMs: decision.delayMs },
          'Job attempt failed, retry scheduled',
        );
        return;
      }
      log.error(
        { durationMs, err: error, outcome: attemptStatus, reason: decision.reason },
        'Job failed permanently',
      );
      await this.notifyFailed({
        jobId: job.jobId,
        queue: job.queue,
        attempt: job.attempt,
        maxAttempts: job.maxAttempts,
        error: message,
        reason: decision.reason,
        source: 'worker',
      });
    });
  }

  /**
   * Extend the lease every heartbeatIntervalMs. If the database says the lease
   * is gone, or heartbeats have been failing for a whole lease duration (so
   * the reaper has likely reclaimed it), abort the job with LeaseLostError.
   */
  private startHeartbeat(
    job: LeasedJob,
    controller: AbortController,
    log: Logger,
  ): { stop(): void } {
    const { store, leaseDurationMs, heartbeatIntervalMs } = this.options;
    let lastRenewedAt = Date.now();
    let beating = false;

    const timer = setInterval(() => {
      if (beating || controller.signal.aborted) return;
      beating = true;
      store
        .heartbeat(job, leaseDurationMs)
        .then((expiresAt) => {
          if (expiresAt === null) {
            controller.abort(new LeaseLostError());
            return;
          }
          lastRenewedAt = Date.now();
          log.trace({ leaseExpiresAt: expiresAt }, 'Lease extended');
        })
        .catch((err: unknown) => {
          const sinceRenewalMs = Date.now() - lastRenewedAt;
          log.warn({ err, sinceRenewalMs }, 'Heartbeat failed');
          if (sinceRenewalMs >= leaseDurationMs) controller.abort(new LeaseLostError());
        })
        .finally(() => {
          beating = false;
        });
    }, heartbeatIntervalMs);
    timer.unref();

    return { stop: () => clearInterval(timer) };
  }

  private async releaseLease(job: LeasedJob, log: Logger, reason: string): Promise<void> {
    await this.persist(log, 'release', async () => {
      const released = await this.options.store.release(job, reason);
      if (released) log.info({ reason }, 'Lease released, job requeued');
      else log.warn({ reason }, 'Lease already lost, nothing to release');
    });
  }

  /**
   * Write an outcome. If the write itself fails (database down), the job
   * stays `running` until its lease expires and the reaper requeues it:
   * at-least-once, never lost.
   */
  private async persist(log: Logger, operation: string, fn: () => Promise<void>): Promise<void> {
    try {
      await fn();
    } catch (err) {
      log.error(
        { err, operation },
        'Could not record job outcome; the reaper will reclaim it when the lease expires',
      );
    }
  }

  private async notifyFailed(event: JobFailedEvent): Promise<void> {
    if (!this.options.onJobFailed) return;
    try {
      await this.options.onJobFailed(event);
    } catch (err) {
      this.logger.error({ err, jobId: event.jobId }, 'onJobFailed hook threw');
    }
  }

  private jobLogger(job: LeasedJob): Logger {
    return this.logger.child({
      jobId: job.jobId,
      queue: job.queue,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      attemptId: job.attemptId,
      attemptNumber: job.attemptNumber,
    });
  }

  private touchLiveness(): void {
    const file = this.options.livenessFile;
    if (!file) return;
    const now = Date.now();
    if (now - this.lastLivenessTouch < LIVENESS_TOUCH_INTERVAL_MS) return;
    this.lastLivenessTouch = now;
    writeFile(file, String(now)).catch((err: unknown) => {
      this.logger.warn({ err, file }, 'Could not write liveness file');
    });
  }
}

/**
 * Run `fn` but stop waiting as soon as `signal` aborts (rejecting with its
 * reason). JavaScript cannot kill the handler's promise; a handler that
 * ignores its signal keeps running in the background, which is one more
 * reason handlers must be idempotent and honour ctx.signal.
 */
export function runUntilAborted(fn: () => Promise<void>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason as Error);
  return new Promise<void>((resolve, reject) => {
    const onAbort = () => reject(signal.reason as Error);
    signal.addEventListener('abort', onAbort, { once: true });
    let work: Promise<void>;
    try {
      work = Promise.resolve(fn());
    } catch (err) {
      work = Promise.reject(err as Error);
    }
    work.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}

/** Resolve true if every promise settles within `ms`, false on timeout. */
async function settleWithin(promises: Promise<unknown>[], ms: number): Promise<boolean> {
  if (promises.length === 0) return true;
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<false>((resolve) => {
    timer = setTimeout(() => resolve(false), ms);
  });
  try {
    return await Promise.race([Promise.allSettled(promises).then(() => true as const), timeout]);
  } finally {
    clearTimeout(timer);
  }
}
