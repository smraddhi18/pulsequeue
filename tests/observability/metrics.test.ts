import { describe, expect, it } from 'vitest';
import type { LeasedJob, ReclaimedJob } from '../../src/domain/lease.js';
import type { JobId, QueueName } from '../../src/domain/job.js';
import type { TenantId } from '../../src/domain/tenant.js';
import {
  ApiMetrics,
  PromSchedulerMetrics,
  PromWorkerMetrics,
} from '../../src/observability/metrics.js';
import { createMetricsRegistry } from '../../src/observability/registry.js';
import { OVERFLOW_LABEL, SeriesLimiter } from '../../src/observability/series-limiter.js';
import type { Registry } from 'prom-client';

/** Value of one sample matching all given labels (undefined when absent). */
async function sample(
  registry: Registry,
  name: string,
  labels: Record<string, string> = {},
): Promise<number | undefined> {
  // Histogram samples are named <base>_bucket/_sum/_count; counters and gauges <base>.
  const base = name.replace(/_(sum|count|bucket)$/, '');
  const metric = registry.getSingleMetric(base) ?? registry.getSingleMetric(name);
  if (!metric) throw new Error(`no metric ${name}`);
  const { name: metricName, values } = await metric.get();
  const hit = (
    values as Array<{ value: number; labels: Record<string, unknown>; metricName?: string }>
  ).find(
    (v) =>
      (v.metricName ?? metricName) === name &&
      Object.entries(labels).every(([k, val]) => String(v.labels[k]) === val),
  );
  return hit?.value;
}

function leased(overrides: Partial<LeasedJob> = {}): LeasedJob {
  return {
    jobId: '1' as JobId,
    leaseToken: 't',
    attemptId: '1',
    tenantId: 'acme' as TenantId,
    queue: 'emails' as QueueName,
    payload: {},
    priority: 0,
    attempt: 1,
    maxAttempts: 3,
    attemptNumber: 1,
    workerId: 'w',
    leaseExpiresAt: new Date(),
    createdAt: new Date(),
    ...overrides,
  };
}

describe('ApiMetrics', () => {
  it('labels HTTP latency by route template and counts enqueues per result', async () => {
    const registry = createMetricsRegistry('api');
    const m = new ApiMetrics(registry, new SeriesLimiter(10));
    m.observeHttp('POST', '/queues/:name/jobs', 201, 0.012);
    m.jobEnqueued('acme', 'emails', 'created');
    m.jobEnqueued('acme', 'emails', 'duplicate');
    m.jobEnqueued('acme', 'emails', 'created');

    expect(
      await sample(registry, 'pulsequeue_http_request_duration_seconds_count', {
        method: 'POST',
        route: '/queues/:name/jobs',
        status_code: '201',
      }),
    ).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_jobs_enqueued_total', {
        tenant: 'acme',
        queue: 'emails',
        result: 'created',
      }),
    ).toBe(2);
    // Default label: every series says which process kind produced it.
    expect(await registry.metrics()).toContain('role="api"');
  });

  it('records rate-limit outcomes with the limiting scope', async () => {
    const registry = createMetricsRegistry('api');
    const m = new ApiMetrics(registry, new SeriesLimiter(10));
    const info = { scope: 'queue' as const, limit: 1, remaining: 0, resetMs: 10 };
    m.rateLimitDecision({ outcome: 'limited', info, retryAfterMs: 10 });
    m.rateLimitDecision({ outcome: 'allowed', info });
    m.rateLimitDecision({ outcome: 'skipped', reason: 'disabled' });
    expect(
      await sample(registry, 'pulsequeue_ratelimit_decisions_total', {
        outcome: 'limited',
        scope: 'queue',
      }),
    ).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_ratelimit_decisions_total', {
        outcome: 'allowed',
        scope: 'none',
      }),
    ).toBe(1);
  });

  it('folds tenant/queue pairs beyond the cap into __other__', async () => {
    const registry = createMetricsRegistry('api');
    const m = new ApiMetrics(registry, new SeriesLimiter(1));
    m.jobEnqueued('acme', 'a', 'created');
    m.jobEnqueued('acme', 'b', 'created');
    m.jobEnqueued('evil', 'c', 'created');
    expect(
      await sample(registry, 'pulsequeue_jobs_enqueued_total', {
        tenant: OVERFLOW_LABEL,
        queue: OVERFLOW_LABEL,
      }),
    ).toBe(2);
  });
});

describe('PromWorkerMetrics', () => {
  it('tracks the attempt lifecycle: lease, wait, success, failure, DLQ', async () => {
    const registry = createMetricsRegistry('worker');
    const m = new PromWorkerMetrics(registry, new SeriesLimiter(10));
    const q = { tenant: 'acme', queue: 'emails' };
    const job = leased({ queueWaitMs: 250 });

    m.jobLeased(job);
    m.jobSucceeded(job, 0.1);
    m.jobLeased(leased());
    m.jobFailed(job, 'failed', 'retrying', 0.2);
    m.jobFailed(job, 'timed_out', 'dead_lettered', 5);
    m.jobDeadLettered(job, 'attempts_exhausted', 'worker');

    expect(await sample(registry, 'pulsequeue_jobs_leased_total', q)).toBe(2);
    // Only leases that carry a wait are observed.
    expect(await sample(registry, 'pulsequeue_job_queue_wait_seconds_count', q)).toBe(1);
    expect(await sample(registry, 'pulsequeue_job_queue_wait_seconds_sum', q)).toBeCloseTo(0.25);
    expect(await sample(registry, 'pulsequeue_jobs_completed_total', q)).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_jobs_failed_total', {
        ...q,
        outcome: 'timed_out',
        disposition: 'dead_lettered',
      }),
    ).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_job_execution_seconds_count', { ...q, outcome: 'failed' }),
    ).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_jobs_dead_lettered_total', {
        ...q,
        reason: 'attempts_exhausted',
        source: 'worker',
      }),
    ).toBe(1);
  });

  it('counts lease problems, releases, reaper outcomes and in-flight jobs', async () => {
    const registry = createMetricsRegistry('worker');
    const m = new PromWorkerMetrics(registry, new SeriesLimiter(10));
    let inFlight = 3;
    m.trackInFlight(() => inFlight);
    m.leaseLost(leased(), 'complete');
    m.leaseError();
    m.jobReleased(leased());
    const reclaimed: ReclaimedJob = {
      jobId: '1' as JobId,
      tenantId: 'acme' as TenantId,
      queue: 'emails' as QueueName,
      previousWorkerId: 'w',
      attempt: 1,
      maxAttempts: 1,
      status: 'dead',
      deadLetterReason: 'attempts_exhausted',
      leaseExpiries: 1,
    };
    m.jobReclaimed(reclaimed);
    m.jobReclaimed({ ...reclaimed, status: 'queued', deadLetterReason: null });

    expect(await sample(registry, 'pulsequeue_lease_lost_total', { stage: 'complete' })).toBe(1);
    expect(await sample(registry, 'pulsequeue_lease_errors_total')).toBe(1);
    expect(await sample(registry, 'pulsequeue_jobs_released_total')).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_reaper_reclaimed_total', { outcome: 'dead_lettered' }),
    ).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_reaper_reclaimed_total', { outcome: 'requeued' }),
    ).toBe(1);
    expect(await sample(registry, 'pulsequeue_worker_jobs_in_flight')).toBe(3);
    inFlight = 0;
    expect(await sample(registry, 'pulsequeue_worker_jobs_in_flight')).toBe(0);
  });
});

describe('PromSchedulerMetrics', () => {
  it('reflects leadership and tick outcomes', async () => {
    const registry = createMetricsRegistry('scheduler');
    const m = new PromSchedulerMetrics(registry);
    expect(await sample(registry, 'pulsequeue_scheduler_is_leader')).toBe(0);
    m.leadershipChanged('acquired');
    expect(await sample(registry, 'pulsequeue_scheduler_is_leader')).toBe(1);
    m.tickCompleted({ claimed: 3, fired: 2, duplicates: 1, skipped: 4, errors: 0 }, 0.01);
    m.tickFailed(0.5);
    m.leadershipChanged('lost');
    expect(await sample(registry, 'pulsequeue_scheduler_is_leader')).toBe(0);
    expect(await sample(registry, 'pulsequeue_scheduler_ticks_total', { result: 'ok' })).toBe(1);
    expect(await sample(registry, 'pulsequeue_scheduler_ticks_total', { result: 'error' })).toBe(1);
    expect(
      await sample(registry, 'pulsequeue_schedule_occurrences_total', { outcome: 'fired' }),
    ).toBe(2);
    expect(
      await sample(registry, 'pulsequeue_schedule_occurrences_total', {
        outcome: 'misfire_skipped',
      }),
    ).toBe(4);
    expect(
      await sample(registry, 'pulsequeue_scheduler_leadership_changes_total', { event: 'lost' }),
    ).toBe(1);
  });
});
