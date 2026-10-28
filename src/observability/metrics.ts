import { Counter, Gauge, Histogram, type Registry } from 'prom-client';
import type { DeadLetterReason } from '../domain/dead-letter.js';
import type { FailureDisposition, LeasedJob, ReclaimedJob } from '../domain/lease.js';
import type { RateLimitDecision } from '../services/rate-limiter.js';
import { METRIC_PREFIX } from './registry.js';
import type { SeriesLimiter } from './series-limiter.js';

/**
 * Metric definitions (Milestone 7). Components depend on the small
 * interfaces below, never on prom-client: the worker, reaper and scheduler
 * take an optional `metrics` and default to the NOOP implementations, so unit
 * tests and embedders pay nothing.
 *
 * Naming follows Prometheus conventions: `_total` counters, `_seconds`
 * histograms, base units. Labels are bounded: tenant/queue go through a
 * SeriesLimiter, every other label is a small closed enum.
 */

const HTTP_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];
/** Job handlers range from milliseconds to many minutes (WORKER_JOB_TIMEOUT_MS default 5m). */
const EXECUTION_BUCKETS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900];
/** Queue wait spans "picked up at once" to "backlog of an hour". */
const QUEUE_WAIT_BUCKETS = [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600];
const TICK_BUCKETS = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5];

export type EnqueueResult = 'created' | 'duplicate';

/** What the enqueue route reports (API process). */
export interface EnqueueMetrics {
  jobEnqueued(tenantId: string, queue: string, result: EnqueueResult): void;
  rateLimitDecision(decision: RateLimitDecision): void;
}

export type LeaseLostStage = 'running' | 'complete' | 'fail' | 'release';

/** What the worker loop and the reaper report (worker process). */
export interface WorkerMetrics {
  jobLeased(job: LeasedJob): void;
  jobSucceeded(job: LeasedJob, seconds: number): void;
  jobFailed(
    job: LeasedJob,
    outcome: 'failed' | 'timed_out',
    disposition: FailureDisposition,
    seconds: number,
  ): void;
  jobDeadLettered(
    job: { tenantId: string; queue: string },
    reason: DeadLetterReason,
    source: 'worker' | 'reaper',
  ): void;
  /** The lease was gone when the worker tried to use it (another owner, or reaped). */
  leaseLost(job: LeasedJob, stage: LeaseLostStage): void;
  /** Shutdown handed an unfinished job back (lease released, job requeued). */
  jobReleased(job: LeasedJob): void;
  /** A lease poll failed (database unreachable...). */
  leaseError(): void;
  jobReclaimed(job: ReclaimedJob): void;
}

export interface TickTotals {
  claimed: number;
  fired: number;
  duplicates: number;
  skipped: number;
  errors: number;
}

export type LeadershipEvent = 'acquired' | 'lost' | 'released';

/** What the scheduler loop reports (scheduler process). */
export interface SchedulerMetrics {
  leadershipChanged(event: LeadershipEvent): void;
  tickCompleted(totals: TickTotals, seconds: number): void;
  tickFailed(seconds: number): void;
}

export const NOOP_ENQUEUE_METRICS: EnqueueMetrics = {
  jobEnqueued: () => undefined,
  rateLimitDecision: () => undefined,
};

export const NOOP_WORKER_METRICS: WorkerMetrics = {
  jobLeased: () => undefined,
  jobSucceeded: () => undefined,
  jobFailed: () => undefined,
  jobDeadLettered: () => undefined,
  leaseLost: () => undefined,
  jobReleased: () => undefined,
  leaseError: () => undefined,
  jobReclaimed: () => undefined,
};

export const NOOP_SCHEDULER_METRICS: SchedulerMetrics = {
  leadershipChanged: () => undefined,
  tickCompleted: () => undefined,
  tickFailed: () => undefined,
};

const QUEUE_LABELS = ['tenant', 'queue'] as const;

/** API process metrics: HTTP requests, enqueues, rate-limit decisions. */
export class ApiMetrics implements EnqueueMetrics {
  readonly httpRequestDuration: Histogram<'method' | 'route' | 'status_code'>;
  private readonly enqueued: Counter<'tenant' | 'queue' | 'result'>;
  private readonly rateLimit: Counter<'outcome' | 'scope'>;

  constructor(
    readonly registry: Registry,
    private readonly series: SeriesLimiter,
  ) {
    this.httpRequestDuration = new Histogram({
      name: `${METRIC_PREFIX}http_request_duration_seconds`,
      help: 'HTTP request latency by method, route template and status code',
      labelNames: ['method', 'route', 'status_code'],
      buckets: HTTP_BUCKETS,
      registers: [registry],
    });
    this.enqueued = new Counter({
      name: `${METRIC_PREFIX}jobs_enqueued_total`,
      help: 'Enqueue requests by outcome: created = new job, duplicate = idempotent replay',
      labelNames: [...QUEUE_LABELS, 'result'],
      registers: [registry],
    });
    this.rateLimit = new Counter({
      name: `${METRIC_PREFIX}ratelimit_decisions_total`,
      help: 'Enqueue rate-limit decisions (scope = the bucket that limited, or none)',
      labelNames: ['outcome', 'scope'],
      registers: [registry],
    });
  }

  /** route is the template (/queues/:name/jobs), never the raw URL: bounded cardinality. */
  observeHttp(method: string, route: string, statusCode: number, seconds: number): void {
    this.httpRequestDuration.observe(
      { method, route, status_code: String(statusCode) },
      Math.max(0, seconds),
    );
  }

  jobEnqueued(tenantId: string, queue: string, result: EnqueueResult): void {
    this.enqueued.inc({ ...this.series.labels(tenantId, queue), result });
  }

  rateLimitDecision(decision: RateLimitDecision): void {
    const scope = decision.outcome === 'limited' ? decision.info.scope : 'none';
    this.rateLimit.inc({ outcome: decision.outcome, scope });
  }
}

/** Worker process metrics (worker loop + reaper). */
export class PromWorkerMetrics implements WorkerMetrics {
  private readonly leased: Counter<'tenant' | 'queue'>;
  private readonly completed: Counter<'tenant' | 'queue'>;
  private readonly failed: Counter<'tenant' | 'queue' | 'outcome' | 'disposition'>;
  private readonly deadLettered: Counter<'tenant' | 'queue' | 'reason' | 'source'>;
  private readonly execution: Histogram<'tenant' | 'queue' | 'outcome'>;
  private readonly queueWait: Histogram<'tenant' | 'queue'>;
  private readonly lost: Counter<'stage'>;
  private readonly released: Counter<'tenant' | 'queue'>;
  private readonly leaseErrors: Counter;
  private readonly reclaimed: Counter<'outcome'>;
  private inFlightSource: (() => number) | null = null;

  constructor(
    readonly registry: Registry,
    private readonly series: SeriesLimiter,
  ) {
    const registers = [registry];
    this.leased = new Counter({
      name: `${METRIC_PREFIX}jobs_leased_total`,
      help: 'Job attempts started (leases taken)',
      labelNames: QUEUE_LABELS,
      registers,
    });
    this.completed = new Counter({
      name: `${METRIC_PREFIX}jobs_completed_total`,
      help: 'Job attempts that succeeded (terminal success)',
      labelNames: QUEUE_LABELS,
      registers,
    });
    this.failed = new Counter({
      name: `${METRIC_PREFIX}jobs_failed_total`,
      help: 'Failed job attempts by outcome (failed/timed_out) and disposition (retrying/dead_lettered)',
      labelNames: [...QUEUE_LABELS, 'outcome', 'disposition'],
      registers,
    });
    this.deadLettered = new Counter({
      name: `${METRIC_PREFIX}jobs_dead_lettered_total`,
      help: 'Jobs moved to the dead-letter queue, by reason and by who moved them',
      labelNames: [...QUEUE_LABELS, 'reason', 'source'],
      registers,
    });
    this.execution = new Histogram({
      name: `${METRIC_PREFIX}job_execution_seconds`,
      help: 'Handler run time per attempt, by outcome',
      labelNames: [...QUEUE_LABELS, 'outcome'],
      buckets: EXECUTION_BUCKETS,
      registers,
    });
    this.queueWait = new Histogram({
      name: `${METRIC_PREFIX}job_queue_wait_seconds`,
      help: 'Time from runnable (run_at) to leased, measured on the database clock',
      labelNames: QUEUE_LABELS,
      buckets: QUEUE_WAIT_BUCKETS,
      registers,
    });
    this.lost = new Counter({
      name: `${METRIC_PREFIX}lease_lost_total`,
      help: 'Leases found lost (fencing token no longer current), by where it was noticed',
      labelNames: ['stage'],
      registers,
    });
    this.released = new Counter({
      name: `${METRIC_PREFIX}jobs_released_total`,
      help: 'Unfinished jobs whose lease was released on shutdown (requeued at once)',
      labelNames: QUEUE_LABELS,
      registers,
    });
    this.leaseErrors = new Counter({
      name: `${METRIC_PREFIX}lease_errors_total`,
      help: 'Lease polls that failed (the worker backs off and retries)',
      registers,
    });
    this.reclaimed = new Counter({
      name: `${METRIC_PREFIX}reaper_reclaimed_total`,
      help: 'Expired leases reclaimed by the reaper, by outcome (requeued/dead_lettered)',
      labelNames: ['outcome'],
      registers,
    });
    // Read lazily at scrape time: no bookkeeping on the hot path.
    const inFlight = (): number => this.inFlightSource?.() ?? 0;
    new Gauge({
      name: `${METRIC_PREFIX}worker_jobs_in_flight`,
      help: 'Jobs currently executing in this worker process',
      registers,
      collect() {
        this.set(inFlight());
      },
    });
  }

  /** Where the in-flight gauge reads from (Worker.activeJobs). */
  trackInFlight(source: () => number): void {
    this.inFlightSource = source;
  }

  private labels(job: { tenantId: string; queue: string }) {
    return this.series.labels(job.tenantId, job.queue);
  }

  jobLeased(job: LeasedJob): void {
    const labels = this.labels(job);
    this.leased.inc(labels);
    if (job.queueWaitMs !== undefined) this.queueWait.observe(labels, job.queueWaitMs / 1000);
  }

  jobSucceeded(job: LeasedJob, seconds: number): void {
    const labels = this.labels(job);
    this.completed.inc(labels);
    this.execution.observe({ ...labels, outcome: 'succeeded' }, seconds);
  }

  jobFailed(
    job: LeasedJob,
    outcome: 'failed' | 'timed_out',
    disposition: FailureDisposition,
    seconds: number,
  ): void {
    const labels = this.labels(job);
    this.failed.inc({ ...labels, outcome, disposition });
    this.execution.observe({ ...labels, outcome }, seconds);
  }

  jobDeadLettered(
    job: { tenantId: string; queue: string },
    reason: DeadLetterReason,
    source: 'worker' | 'reaper',
  ): void {
    this.deadLettered.inc({ ...this.labels(job), reason, source });
  }

  leaseLost(_job: LeasedJob, stage: LeaseLostStage): void {
    this.lost.inc({ stage });
  }

  jobReleased(job: LeasedJob): void {
    this.released.inc(this.labels(job));
  }

  leaseError(): void {
    this.leaseErrors.inc();
  }

  jobReclaimed(job: ReclaimedJob): void {
    this.reclaimed.inc({ outcome: job.status === 'queued' ? 'requeued' : 'dead_lettered' });
  }
}

/** Scheduler process metrics. */
export class PromSchedulerMetrics implements SchedulerMetrics {
  private readonly isLeader: Gauge;
  private readonly leadership: Counter<'event'>;
  private readonly ticks: Counter<'result'>;
  private readonly tickDuration: Histogram<'result'>;
  private readonly fires: Counter<'outcome'>;

  constructor(readonly registry: Registry) {
    const registers = [registry];
    this.isLeader = new Gauge({
      name: `${METRIC_PREFIX}scheduler_is_leader`,
      help: '1 while this scheduler holds the leader lock; sum over instances should be 1',
      registers,
    });
    this.isLeader.set(0);
    this.leadership = new Counter({
      name: `${METRIC_PREFIX}scheduler_leadership_changes_total`,
      help: 'Leadership transitions of this instance (acquired/lost/released)',
      labelNames: ['event'],
      registers,
    });
    this.ticks = new Counter({
      name: `${METRIC_PREFIX}scheduler_ticks_total`,
      help: 'Leader ticks by result',
      labelNames: ['result'],
      registers,
    });
    this.tickDuration = new Histogram({
      name: `${METRIC_PREFIX}scheduler_tick_duration_seconds`,
      help: 'Wall time of one leader tick (all its batches)',
      labelNames: ['result'],
      buckets: TICK_BUCKETS,
      registers,
    });
    this.fires = new Counter({
      name: `${METRIC_PREFIX}schedule_occurrences_total`,
      help: 'Schedule occurrences handled by the leader, by outcome',
      labelNames: ['outcome'],
      registers,
    });
  }

  leadershipChanged(event: LeadershipEvent): void {
    this.leadership.inc({ event });
    this.isLeader.set(event === 'acquired' ? 1 : 0);
  }

  tickCompleted(totals: TickTotals, seconds: number): void {
    this.ticks.inc({ result: 'ok' });
    this.tickDuration.observe({ result: 'ok' }, seconds);
    if (totals.fired > 0) this.fires.inc({ outcome: 'fired' }, totals.fired);
    if (totals.duplicates > 0) this.fires.inc({ outcome: 'duplicate' }, totals.duplicates);
    if (totals.skipped > 0) this.fires.inc({ outcome: 'misfire_skipped' }, totals.skipped);
    if (totals.errors > 0) this.fires.inc({ outcome: 'error' }, totals.errors);
  }

  tickFailed(seconds: number): void {
    this.ticks.inc({ result: 'error' });
    this.tickDuration.observe({ result: 'error' }, seconds);
  }
}
