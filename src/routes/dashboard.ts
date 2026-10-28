import type { FastifyInstance } from 'fastify';
import type {
  DashboardQueuePage,
  FailureRateReport,
  LatencyReport,
  LatencySummary,
  ThroughputSeries,
} from '../domain/dashboard.js';
import type { QueueName } from '../domain/job.js';
import { principalOf } from '../plugins/auth.js';
import type { DashboardService } from '../services/dashboard.service.js';
import {
  dashboardQueuesQuerySchema,
  failureRateQuerySchema,
  latencyQuerySchema,
  throughputQuerySchema,
} from './dashboard.schemas.js';
import { queueParamsSchema } from './jobs.schemas.js';

export interface DashboardRoutesOptions {
  dashboardService: DashboardService;
}

/** Latency values are milliseconds rounded to 0.001 (sub-microsecond digits are noise). */
const round = (v: number | null): number | null =>
  v === null ? null : Math.round(v * 1000) / 1000;

function latencyResponse(s: LatencySummary) {
  return {
    samples: s.samples,
    p50: round(s.p50),
    p95: round(s.p95),
    p99: round(s.p99),
    max: round(s.max),
  };
}

export function toQueuesResponse(page: DashboardQueuePage) {
  return {
    data: page.queues.map((q) => ({
      queue: q.queue,
      counts: { queued: q.queued, scheduled: q.scheduled, running: q.running, dead: q.dead },
      maxConcurrency: q.maxConcurrency,
      oldestQueuedAt: q.oldestQueuedAt?.toISOString() ?? null,
      oldestQueuedAgeMs:
        q.oldestQueuedAt === null
          ? null
          : Math.max(0, page.computedAt.getTime() - q.oldestQueuedAt.getTime()),
    })),
    nextAfter: page.nextAfter,
    computedAt: page.computedAt.toISOString(),
  };
}

export function toThroughputResponse(s: ThroughputSeries) {
  const totals = { enqueued: 0, succeeded: 0, failed: 0, timedOut: 0, deadLettered: 0 };
  const buckets = s.buckets.map((b) => {
    totals.enqueued += b.enqueued;
    totals.succeeded += b.succeeded;
    totals.failed += b.failed;
    totals.timedOut += b.timedOut;
    totals.deadLettered += b.deadLettered;
    return {
      start: b.start.toISOString(),
      enqueued: b.enqueued,
      succeeded: b.succeeded,
      failed: b.failed,
      timedOut: b.timedOut,
      deadLettered: b.deadLettered,
    };
  });
  return {
    queue: s.queue,
    from: s.from.toISOString(),
    to: s.to.toISOString(),
    bucketMs: s.bucketMs,
    buckets,
    totals,
  };
}

export function toLatencyResponse(r: LatencyReport) {
  return {
    queue: r.queue,
    from: r.from.toISOString(),
    to: r.to.toISOString(),
    unit: 'ms',
    executionMs: latencyResponse(r.execution),
    queueWaitMs: latencyResponse(r.queueWait),
    truncated: r.truncated,
    sampleLimit: r.sampleLimit,
  };
}

export function toFailureRateResponse(r: FailureRateReport) {
  return {
    queue: r.queue,
    from: r.from.toISOString(),
    to: r.to.toISOString(),
    attempts: r.attempts,
    attemptFailureRate: r.attemptFailureRate,
    jobs: r.jobs,
    jobFailureRate: r.jobFailureRate,
  };
}

/**
 * Dashboard API (Milestone 7). Read role is enough: everything is scoped to
 * the caller's tenant. Responses are computed per request from Postgres
 * (no cache: windows move with now()), with cost bounded by the window,
 * bucket and sample limits validated in dashboard.schemas.ts.
 */
export async function dashboardRoutes(
  app: FastifyInstance,
  opts: DashboardRoutesOptions,
): Promise<void> {
  const { dashboardService } = opts;

  app.get('/dashboard/queues', { config: { access: 'read' } }, async (request, reply) => {
    const query = dashboardQueuesQuerySchema.parse(request.query);
    const page = await dashboardService.listQueues(
      principalOf(request).tenantId,
      query.limit,
      query.after ?? null,
    );
    reply.header('cache-control', 'no-store');
    return reply.status(200).send(toQueuesResponse(page));
  });

  app.get(
    '/dashboard/queues/:name/throughput',
    { config: { access: 'read' } },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const query = throughputQuerySchema.parse(request.query);
      const series = await dashboardService.throughput(
        principalOf(request).tenantId,
        name as QueueName,
        query.window,
        query.bucket,
      );
      reply.header('cache-control', 'no-store');
      return reply.status(200).send(toThroughputResponse(series));
    },
  );

  app.get(
    '/dashboard/queues/:name/latency',
    { config: { access: 'read' } },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const query = latencyQuerySchema.parse(request.query);
      const report = await dashboardService.latency(
        principalOf(request).tenantId,
        name as QueueName,
        query.window,
      );
      reply.header('cache-control', 'no-store');
      return reply.status(200).send(toLatencyResponse(report));
    },
  );

  app.get(
    '/dashboard/queues/:name/failure-rate',
    { config: { access: 'read' } },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const query = failureRateQuerySchema.parse(request.query);
      const report = await dashboardService.failureRate(
        principalOf(request).tenantId,
        name as QueueName,
        query.window,
      );
      reply.header('cache-control', 'no-store');
      return reply.status(200).send(toFailureRateResponse(report));
    },
  );
}
