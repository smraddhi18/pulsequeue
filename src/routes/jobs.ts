import type { FastifyInstance } from 'fastify';
import type { Job, QueueName } from '../domain/job.js';
import type { JobService } from '../services/job.service.js';
import {
  enqueueJobBodySchema,
  enqueueJobHeadersSchema,
  jobParamsSchema,
  listJobsQuerySchema,
  queueParamsSchema,
} from './jobs.schemas.js';

export interface JobRoutesOptions {
  jobService: JobService;
  maxPayloadBytes: number;
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
  const { jobService } = opts;

  app.post(
    '/queues/:name/jobs',
    {
      // Coarse transport cap; the precise limit applies to the parsed payload
      // in JobService. Headroom covers JSON escaping and the envelope fields.
      bodyLimit: opts.maxPayloadBytes * 2 + 64 * 1024,
    },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const headers = enqueueJobHeadersSchema.parse(request.headers);
      const body = enqueueJobBodySchema.parse(request.body);

      const { job, created } = await jobService.enqueue({
        queue: name as QueueName,
        payload: body.payload,
        ...(body.priority !== undefined ? { priority: body.priority } : {}),
        ...(body.delayMs !== undefined ? { delayMs: body.delayMs } : {}),
        ...(body.maxAttempts !== undefined ? { maxAttempts: body.maxAttempts } : {}),
        ...(headers['idempotency-key'] !== undefined
          ? { idempotencyKey: headers['idempotency-key'] }
          : {}),
      });

      if (created) {
        request.log.info({ jobId: job.id, queue: job.queue, status: job.status }, 'Job enqueued');
      }

      reply.header('location', `/jobs/${job.id}`);
      if (!created) {
        // Same convention as Stripe: present only when the response is a replay.
        reply.header(IDEMPOTENT_REPLAY_HEADER, 'true');
      }
      return reply.status(created ? 201 : 200).send(toJobResponse(job));
    },
  );

  app.get('/jobs/:id', async (request, reply) => {
    const { id } = jobParamsSchema.parse(request.params);
    const job = await jobService.getJob(id);
    return reply.status(200).send(toJobResponse(job));
  });

  app.get('/queues/:name/jobs', async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const query = listJobsQuerySchema.parse(request.query);

    const page = await jobService.listJobs({
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
