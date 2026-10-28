import { writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import type { DeadLetterReason } from '../domain/dead-letter.js';
import { jobQueueRef, type LeasedJob } from '../domain/lease.js';
import { NOOP_QUEUE_SIGNALS, uniqueQueueRefs, type QueueSignals } from '../domain/signals.js';
import type { Logger } from '../lib/logger.js';
import { NOOP_WORKER_METRICS, type WorkerMetrics } from '../observability/metrics.js';
import {
  DISABLED_TRACING,
  recordSpanError,
  SpanKind,
  SpanStatusCode,
  type Span,
  type Tracing,
} from '../observability/tracing.js';
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

/**
 * The slice of LeaseRepository the worker needs; unit tests pass an in-memory
 * fake. nextDueInMs is optional: without it a push-mode worker relies on
 * wake-ups and its fallback poll alone.
 */
export type LeaseStore = Pick<
  LeaseRepository,
  'lease' | 'heartbeat' | 'complete' | 'fail' | 'release'
> &
  Partial<Pick<LeaseRepository, 'nextDueInMs'>>;

/**
 * Where the worker learns whether wake-ups can be trusted right now (the
 * Redis subscriber). The subscriber delivers the wake-ups themselves by
 * calling Worker.wake().
 */
export interface WakeupSource {
  /** true while subscribed: idle polling can slow down to the fallback interval. */
  readonly isListening: boolean;
}

/**
 * A job was dead-lettered. Fired *after* the move committed (the move itself
 * is part of the fenced fail/reap statement), so the hook is a notification
 * point for side effects such as alerts or metrics, never the mechanism that
 * puts the job in the DLQ: a crash or a throwing hook cannot lose the move.
 */
export interface JobFailedEvent {
  jobId: string;
  queue: string;
  attempt: number;
  maxAttempts: number;
  error: string;
  reason: DeadLetterReason;
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
  /** Idle poll interval without wake-ups (no Redis, or Redis down). */
  pollIntervalMs: number;
  /**
   * Push mode (Milestone 5): while `wakeups.isListening`, an idle worker
   * sleeps `fallbackPollIntervalMs` (capped by the next scheduled job's due
   * time) and relies on wake() for new work. Omit for pure polling.
   */
  wakeups?: WakeupSource;
  /** Idle poll interval in push mode; a safety net for lost wake-ups. Default pollIntervalMs. */
  fallbackPollIntervalMs?: number;
  /** Notified after every committed state change (wake-ups, stats invalidation). */
  signals?: QueueSignals;
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  jobTimeoutMs: number;
  backoff: BackoffOptions;
  /** Called after a job is dead-lettered. Errors thrown here are logged, never fatal. */
  onJobFailed?: JobFailedHook;
  /** Touched (throttled) on every loop iteration; a container healthcheck reads its mtime. */
  livenessFile?: string;
  /** Injectable randomness for deterministic tests (backoff and poll jitter). */
  random?: () => number;
  /** M7: Prometheus counters/histograms (no-op when omitted). */
  metrics?: WorkerMetrics;
  /** M7: one CONSUMER span per attempt, child of the job's enqueue span (no-op when omitted). */
  tracing?: Tracing;
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
 * Floor for a push-mode sleep that ends when a scheduled job is due: a job
 * that is due but not leasable yet (its row is locked by another worker's
 * promotion) must not turn the loop into a busy spin.
 */
const MIN_DUE_SLEEP_MS = 25;

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
 * Push mode (Milestone 5): with a listening wake-up source the idle sleep
 * becomes the long fallback interval, cut short by wake() (a wake-up for one
 * of our queues) or by the next scheduled job becoming due. Postgres stays
 * the source of truth: a wake-up only makes the next lease query happen
 * sooner, and a lost one is caught by the fallback poll.
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
  private idleSleeping = false;
  /** Set by wake(); cleared before each lease, so a wake during the lease query triggers a re-poll. */
  private wakeRequested = false;
  private pushMode = false;
  private lastLivenessTouch = 0;
  private readonly signals: QueueSignals;
  private readonly metrics: WorkerMetrics;
  private readonly tracing: Tracing;

  constructor(private readonly options: WorkerOptions) {
    if (options.queues.length === 0) throw new Error('Worker needs at least one queue');
    if (options.concurrency < 1) throw new Error('Worker concurrency must be >= 1');
    if (options.heartbeatIntervalMs * 2 > options.leaseDurationMs) {
      throw new Error('heartbeatIntervalMs must be at most half of leaseDurationMs');
    }
    if (
      options.fallbackPollIntervalMs !== undefined &&
      options.fallbackPollIntervalMs < options.pollIntervalMs
    ) {
      throw new Error('fallbackPollIntervalMs must be at least pollIntervalMs');
    }
    this.random = options.random ?? Math.random;
    this.logger = options.logger.child({ workerId: options.workerId });
    this.signals = options.signals ?? NOOP_QUEUE_SIGNALS;
    this.metrics = options.metrics ?? NOOP_WORKER_METRICS;
    this.tracing = options.tracing ?? DISABLED_TRACING;
  }

  /**
   * New work may be available (a wake-up arrived, or the wake-up source
   * changed state): poll now if idle. A wake while the query is in flight
   * makes the loop poll again right after it; a wake while all slots are busy
   * or during an error backoff is ignored (the loop polls when it can anyway).
   */
  wake(): void {
    if (this.stopping) return;
    this.wakeRequested = true;
    if (this.idleSleeping) this.sleeper.wake();
  }

  /** true while idle polling is slowed down because wake-ups are trusted. */
  get isPushMode(): boolean {
    return this.pushMode;
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
        wakeups: this.options.wakeups !== undefined,
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
      this.wakeRequested = false;
      try {
        jobs = await store.lease({ queues, workerId, limit: free, leaseMs: leaseDurationMs });
        consecutiveErrors = 0;
      } catch (err) {
        consecutiveErrors++;
        this.metrics.leaseError();
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
        for (const queue of uniqueQueueRefs(jobs.map(jobQueueRef))) this.signals.jobsChanged(queue);
      }
      for (const job of jobs) {
        this.metrics.jobLeased(job);
        this.dispatch(job);
      }

      // A full batch means there is probably more work: poll again at once.
      // So does a wake-up that arrived while the query ran: its job may have
      // committed after the query's snapshot.
      if (jobs.length < free && !this.stopping && !this.wakeRequested) {
        await this.idleSleep(pollIntervalMs);
      }
    }
  }

  /**
   * Sleep between polls when there is no backlog. Poll mode: the jittered
   * poll interval. Push mode: the jittered fallback interval, but no longer
   * than until the next scheduled job (delayed enqueue, retry backoff) is
   * due, because nothing publishes a wake-up when time passes.
   */
  private async idleSleep(pollIntervalMs: number): Promise<void> {
    const listening = this.options.wakeups?.isListening ?? false;
    if (listening !== this.pushMode) {
      this.pushMode = listening;
      this.logger.info(
        { event: 'wakeup.mode_changed', mode: listening ? 'push' : 'poll' },
        listening
          ? 'Wake-ups available: polling only as a fallback'
          : 'Wake-ups unavailable: polling',
      );
    }

    let ms: number;
    if (listening) {
      ms = jitter(this.options.fallbackPollIntervalMs ?? pollIntervalMs, 0.2, this.random);
      const dueInMs = await this.nextDueInMs();
      if (dueInMs !== null) ms = Math.min(ms, Math.max(MIN_DUE_SLEEP_MS, dueInMs));
    } else {
      ms = jitter(pollIntervalMs, 0.2, this.random);
    }
    // A wake-up may have arrived during the nextDue query.
    if (this.wakeRequested || this.stopping) return;

    this.idleSleeping = true;
    try {
      await this.sleeper.sleep(ms);
    } finally {
      this.idleSleeping = false;
    }
  }

  /** null when unknown (no store support, query failed) or nothing is scheduled. */
  private async nextDueInMs(): Promise<number | null> {
    const { store, queues } = this.options;
    if (!store.nextDueInMs) return null;
    try {
      return await store.nextDueInMs(queues);
    } catch (err) {
      // Fall back to the plain fallback interval; the next lease query
      // surfaces a real database outage.
      this.logger.warn({ err }, 'Could not read the next due time');
      return null;
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

  /**
   * One attempt inside its CONSUMER span. The span's parent is the enqueue
   * span stored on the row (jobs.traceparent), so attempts appear in the
   * producer's trace; without a stored context (tracing was off at enqueue,
   * scheduler-fired jobs) the attempt starts its own trace.
   */
  private async execute(job: LeasedJob, controller: AbortController): Promise<void> {
    const span = this.tracing.startSpan('job.execute', {
      kind: SpanKind.CONSUMER,
      parent: this.tracing.contextFromTraceparent(job.traceparent),
      attributes: {
        'messaging.system': 'pulsequeue',
        'messaging.operation.type': 'process',
        'messaging.destination.name': job.queue,
        'messaging.message.id': job.jobId,
        'pulsequeue.tenant_id': job.tenantId,
        'pulsequeue.attempt': job.attempt,
        'pulsequeue.max_attempts': job.maxAttempts,
        'pulsequeue.worker_id': this.options.workerId,
      },
    });
    try {
      await this.runAttempt(job, controller, span);
    } catch (err) {
      recordSpanError(span, err);
      throw err;
    } finally {
      span.end();
    }
  }

  private async runAttempt(job: LeasedJob, controller: AbortController, span: Span): Promise<void> {
    const log = this.jobLogger(job, span);
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
          tenantId: job.tenantId,
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

    const elapsedMs = performance.now() - startedAt;
    const durationMs = Math.round(elapsedMs);
    const seconds = elapsedMs / 1000;
    const abortReason: unknown = controller.signal.aborted ? controller.signal.reason : undefined;

    if (succeeded) {
      await this.persist(log, 'complete', async () => {
        if (await this.options.store.complete(job)) {
          this.slotFreed(job);
          this.metrics.jobSucceeded(job, seconds);
          span.setAttribute('pulsequeue.outcome', 'succeeded');
          log.info({ durationMs, outcome: 'succeeded' }, 'Job attempt succeeded');
        } else {
          this.metrics.leaseLost(job, 'complete');
          span.setAttribute('pulsequeue.outcome', 'lease_lost');
          log.warn({ durationMs }, 'Job finished but its lease was lost; result discarded');
        }
      });
      return;
    }

    if (abortReason instanceof LeaseLostError) {
      this.metrics.leaseLost(job, 'running');
      span.setAttribute('pulsequeue.outcome', 'lease_lost');
      span.setStatus({ code: SpanStatusCode.ERROR, message: 'lease lost' });
      log.warn({ durationMs }, 'Lease lost while running; stopped without writing an outcome');
      return;
    }

    if (abortReason instanceof ShutdownAbortError) {
      span.setAttribute('pulsequeue.outcome', 'released');
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
    recordSpanError(span, error);

    await this.persist(log, 'fail', async () => {
      const disposition = await this.options.store.fail(job, {
        error: message,
        attemptStatus,
        retryDelayMs,
        ...(decision.kind === 'fail' ? { deadLetterReason: decision.reason } : {}),
      });
      if (disposition === null) {
        this.metrics.leaseLost(job, 'fail');
        span.setAttribute('pulsequeue.outcome', 'lease_lost');
        log.warn({ durationMs, err: error }, 'Job failed but its lease was lost; nothing written');
        return;
      }
      this.metrics.jobFailed(job, attemptStatus, disposition, seconds);
      span.setAttribute('pulsequeue.outcome', attemptStatus);
      span.setAttribute('pulsequeue.disposition', disposition);
      if (decision.kind === 'retry') {
        // The retry has a later run_at; idle push-mode workers re-read the
        // next due time when woken, so it is not left to the fallback poll.
        this.signals.jobsRunnable(jobQueueRef(job));
        log.warn(
          { durationMs, err: error, outcome: attemptStatus, retryInMs: decision.delayMs },
          'Job attempt failed, retry scheduled',
        );
        return;
      }
      this.slotFreed(job);
      this.metrics.jobDeadLettered(job, decision.reason, 'worker');
      log.error(
        {
          event: 'dlq.dead_lettered',
          durationMs,
          err: error,
          outcome: attemptStatus,
          reason: decision.reason,
          source: 'worker',
        },
        'Job moved to the dead-letter queue',
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

  /**
   * A job reached a terminal state. For a capped queue (M6) that frees a
   * concurrency slot: workers sitting idle because the queue was at its cap
   * are woken like for new work. Otherwise only the stats change.
   */
  private slotFreed(job: LeasedJob): void {
    if (job.queueCapped) this.signals.jobsRunnable(jobQueueRef(job));
    else this.signals.jobsChanged(jobQueueRef(job));
  }

  private async releaseLease(job: LeasedJob, log: Logger, reason: string): Promise<void> {
    await this.persist(log, 'release', async () => {
      const released = await this.options.store.release(job, reason);
      if (released) {
        this.metrics.jobReleased(job);
        this.signals.jobsRunnable(jobQueueRef(job));
        log.info({ reason }, 'Lease released, job requeued');
      } else {
        this.metrics.leaseLost(job, 'release');
        log.warn({ reason }, 'Lease already lost, nothing to release');
      }
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

  /** Every line about one attempt carries the job ids and, when traced, trace_id/span_id. */
  private jobLogger(job: LeasedJob, span?: Span): Logger {
    return this.logger.child({
      jobId: job.jobId,
      tenantId: job.tenantId,
      queue: job.queue,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      attemptId: job.attemptId,
      attemptNumber: job.attemptNumber,
      ...(span !== undefined ? this.tracing.logFields(span) : {}),
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
