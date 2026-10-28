import type { FastifyInstance } from 'fastify';
import type { Job, QueueName } from '../domain/job.js';
import type { JobService } from '../services/job.service.js';
import type { RateLimiter } from '../services/rate-limiter.js';
import {
  enqueueJobBodySchema,
  enqueueJobHeadersSchema,
  jobParamsSchema,
  listJobsQuerySchema,
  queueParamsSchema,
} from './jobs.schemas.js';
import { principalOf } from '../plugins/auth.js';
import { NOOP_ENQUEUE_METRICS, type EnqueueMetrics } from '../observability/metrics.js';
import {
  DISABLED_TRACING,
  recordSpanError,
  SpanKind,
  type Tracing,
} from '../observability/tracing.js';
import { enforceRateLimit } from './rate-limit.js';

export interface JobRoutesOptions {
  jobService: JobService;
  rateLimiter: RateLimiter;
  maxPayloadBytes: number;
  /** M7: enqueue + rate-limit counters (no-op when omitted). */
  metrics?: EnqueueMetrics;
  /** M7: PRODUCER span per enqueue, its traceparent stored on the job. */
  tracing?: Tracing;
}

/** Public JSON contract for a job. Built explicitly so storage columns never leak. */
export interface JobResponse {
  id: string;
  queue: string;
  status: Job['status'];
  priority: number;
  payload: unknown;
  attempts: number;
  maxAttempts: number;
  runAt: string;
  idempotencyKey: string | null;
  lastError: string | null;
  finishedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export function toJobResponse(job: Job): JobResponse {
  return {
    id: job.id,
    queue: job.queue,
    status: job.status,
    priority: job.priority,
    payload: job.payload,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    runAt: job.runAt.toISOString(),
    idempotencyKey: job.idempotencyKey,
    lastError: job.lastError,
    finishedAt: job.finishedAt?.toISOString() ?? null,
    createdAt: job.createdAt.toISOString(),
    updatedAt: job.updatedAt.toISOString(),
  };
}

export const IDEMPOTENT_REPLAY_HEADER = 'idempotent-replayed';

/**
 * Thin HTTP adapter: parse + validate input with zod (ZodError -> 400 via the
 * central handler), call JobService, shape the response.
 */
export async function jobRoutes(app: FastifyInstance, opts: JobRoutesOptions): Promise<void> {
  const { jobService, rateLimiter } = opts;
  const metrics = opts.metrics ?? NOOP_ENQUEUE_METRICS;
  const tracing = opts.tracing ?? DISABLED_TRACING;

  app.post(
    '/queues/:name/jobs',
    {
      config: { access: 'enqueue' },
      // Coarse transport cap; the precise limit applies to the parsed payload
      // in JobService. Headroom covers JSON escaping and the envelope fields.
      bodyLimit: opts.maxPayloadBytes * 2 + 64 * 1024,
    },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const headers = enqueueJobHeadersSchema.parse(request.headers);
      const body = enqueueJobBodySchema.parse(request.body);
      const principal = principalOf(request);

      // After validation (a malformed request costs no quota), before any
      // database write. Idempotent replays are counted like new enqueues.
      const decision = await rateLimiter.consume(
        principal.tenantId,
        name,
        principal.tenantRateLimit,
      );
      metrics.rateLimitDecision(decision);
      if (decision.outcome === 'limited') {
        request.log.info(
          {
            event: 'ratelimit.limited',
            keyId: principal.keyId,
            queue: name,
            scope: decision.info.scope,
            retryAfterMs: decision.retryAfterMs,
          },
          'Enqueue rate limited',
        );
      }
      enforceRateLimit(reply, decision);

      // PRODUCER span, child of the request span. Its traceparent goes on the
      // row; each worker attempt starts a CONSUMER span under it, so one
      // trace shows request -> enqueue -> attempt 1..n even hours later.
      const span = tracing.startSpan('pulsequeue.enqueue', {
        kind: SpanKind.PRODUCER,
        ...(request.span !== null ? { parent: tracing.contextWithSpan(request.span) } : {}),
        attributes: {
          'messaging.system': 'pulsequeue',
          'messaging.operation.type': 'send',
          'messaging.destination.name': name,
          'pulsequeue.tenant_id': principal.tenantId,
        },
      });
      const traceparent = tracing.traceparentOf(span);

      let result;
      try {
        result = await jobService.enqueue({
          tenantId: principal.tenantId,
          queue: name as QueueName,
          payload: body.payload,
          ...(body.priority !== undefined ? { priority: body.priority } : {}),
          ...(body.delayMs !== undefined ? { delayMs: body.delayMs } : {}),
          ...(body.runAt !== undefined ? { runAt: body.runAt } : {}),
          ...(body.maxAttempts !== undefined ? { maxAttempts: body.maxAttempts } : {}),
          ...(headers['idempotency-key'] !== undefined
            ? { idempotencyKey: headers['idempotency-key'] }
            : {}),
          ...(traceparent !== null ? { traceparent } : {}),
        });
      } catch (err) {
        recordSpanError(span, err);
        throw err;
      } finally {
        if (result !== undefined) {
          span.setAttribute('messaging.message.id', result.job.id);
          span.setAttribute('pulsequeue.idempotent_replay', !result.created);
        }
        span.end();
      }
      const { job, created } = result;
      metrics.jobEnqueued(principal.tenantId, name, created ? 'created' : 'duplicate');

      if (created) {
        request.log.info(
          {
            jobId: job.id,
            queue: job.queue,
            status: job.status,
            runAt: job.runAt.toISOString(),
          },
          'Job enqueued',
        );
      }

      reply.header('location', `/jobs/${job.id}`);
      if (!created) {
        // Same convention as Stripe: present only when the response is a replay.
        reply.header(IDEMPOTENT_REPLAY_HEADER, 'true');
      }
      return reply.status(created ? 201 : 200).send(toJobResponse(job));
    },
  );

  app.get('/jobs/:id', { config: { access: 'read' } }, async (request, reply) => {
    const { id } = jobParamsSchema.parse(request.params);
    const job = await jobService.getJob(principalOf(request).tenantId, id);
    return reply.status(200).send(toJobResponse(job));
  });

  app.get('/queues/:name/jobs', { config: { access: 'read' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const query = listJobsQuerySchema.parse(request.query);

    const page = await jobService.listJobs({
      tenantId: principalOf(request).tenantId,
      queue: name as QueueName,
      limit: query.limit,
      ...(query.status !== undefined ? { status: query.status } : {}),
      ...(query.cursor !== undefined ? { cursor: query.cursor } : {}),
    });

    return reply.status(200).send({
      data: page.jobs.map(toJobResponse),
      nextCursor: page.nextCursor,
    });
  });
}
