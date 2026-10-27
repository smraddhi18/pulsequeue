import type { FastifyInstance } from 'fastify';
import type {
  DeadLetteredJob,
  DeadLetteredJobWithAttempts,
  JobAttempt,
} from '../domain/dead-letter.js';
import type { JobId, QueueName } from '../domain/job.js';
import type { DlqService } from '../services/dlq.service.js';
import {
  bulkReplayBodySchema,
  dlqJobParamsSchema,
  listDeadLettersQuerySchema,
  purgeDeadLettersQuerySchema,
  replayJobBodySchema,
} from './dlq.schemas.js';
import { queueParamsSchema } from './jobs.schemas.js';
import { toJobResponse, type JobResponse } from './jobs.js';
import { principalOf } from '../plugins/auth.js';

export interface DlqRoutesOptions {
  dlqService: DlqService;
}

export interface DeadLetterResponse extends JobResponse {
  deadLetter: {
    reason: DeadLetteredJob['deadLetter']['reason'];
    deadLetteredAt: string;
    attempts: number;
    lastError: string | null;
    queue: string;
    replayCount: number;
    lastReplayedAt: string | null;
  };
}

export interface JobAttemptResponse {
  attemptNumber: number;
  status: JobAttempt['status'];
  workerId: string | null;
  error: string | null;
  startedAt: string;
  finishedAt: string | null;
}

export function toDeadLetterResponse(job: DeadLetteredJob): DeadLetterResponse {
  const { deadLetter } = job;
  return {
    ...toJobResponse(job),
    deadLetter: {
      reason: deadLetter.reason,
      deadLetteredAt: deadLetter.deadLetteredAt.toISOString(),
      attempts: deadLetter.attempts,
      lastError: deadLetter.lastError,
      queue: deadLetter.queue,
      replayCount: deadLetter.replayCount,
      lastReplayedAt: deadLetter.lastReplayedAt?.toISOString() ?? null,
    },
  };
}

function toAttemptResponse(attempt: JobAttempt): JobAttemptResponse {
  return {
    attemptNumber: attempt.attemptNumber,
    status: attempt.status,
    workerId: attempt.workerId,
    error: attempt.error,
    startedAt: attempt.startedAt.toISOString(),
    finishedAt: attempt.finishedAt?.toISOString() ?? null,
  };
}

function toDeadLetterDetailResponse(
  job: DeadLetteredJobWithAttempts,
): DeadLetterResponse & { attemptHistory: JobAttemptResponse[] } {
  return {
    ...toDeadLetterResponse(job),
    attemptHistory: job.attemptHistory.map(toAttemptResponse),
  };
}

/**
 * Dead-letter queue API: inspect, replay, purge. Thin HTTP adapter like
 * jobRoutes. Every state change emits one structured log line with a stable
 * `event` field (dlq.replayed, dlq.bulk_replayed, dlq.purged,
 * dlq.bulk_purged) carrying the request id, so DLQ operations are auditable
 * from the logs alone.
 */
export async function dlqRoutes(app: FastifyInstance, opts: DlqRoutesOptions): Promise<void> {
  const { dlqService } = opts;

  app.get('/queues/:name/dlq', { config: { access: 'read' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const query = listDeadLettersQuerySchema.parse(request.query);
    const page = await dlqService.list({
      tenantId: principalOf(request).tenantId,
      queue: name as QueueName,
      limit: query.limit,
      ...(query.reason !== undefined ? { reason: query.reason } : {}),
      ...(query.cursor !== undefined ? { cursor: query.cursor } : {}),
    });
    return reply.status(200).send({
      data: page.jobs.map(toDeadLetterResponse),
      nextCursor: page.nextCursor,
    });
  });

  app.get('/dlq/:jobId', { config: { access: 'read' } }, async (request, reply) => {
    const { jobId } = dlqJobParamsSchema.parse(request.params);
    const job = await dlqService.get(principalOf(request).tenantId, jobId);
    return reply.status(200).send(toDeadLetterDetailResponse(job));
  });

  app.post(
    '/queues/:name/dlq/:jobId/replay',
    { config: { access: 'manage' } },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const { jobId } = dlqJobParamsSchema.parse(request.params);
      const body = replayJobBodySchema.parse(request.body);

      const result = await dlqService.replay({
        tenantId: principalOf(request).tenantId,
        queue: name as QueueName,
        jobId: jobId as JobId,
        ...(body.delayMs !== undefined ? { delayMs: body.delayMs } : {}),
      });
      request.log.info(
        {
          event: 'dlq.replayed',
          jobId: result.job.id,
          queue: result.job.queue,
          previousReason: result.previousReason,
          replayCount: result.replayCount,
          status: result.job.status,
          runAt: result.job.runAt.toISOString(),
        },
        'Job replayed from the dead-letter queue',
      );
      reply.header('location', `/jobs/${result.job.id}`);
      return reply.status(200).send(toJobResponse(result.job));
    },
  );

  app.post('/queues/:name/dlq/replay', { config: { access: 'manage' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const body = bulkReplayBodySchema.parse(request.body);

    const result = await dlqService.replayBatch({
      tenantId: principalOf(request).tenantId,
      queue: name as QueueName,
      limit: body.limit,
      ...(body.reason !== undefined ? { reason: body.reason } : {}),
      ...(body.delayMs !== undefined ? { delayMs: body.delayMs } : {}),
    });
    request.log.info(
      {
        event: 'dlq.bulk_replayed',
        queue: name,
        replayed: result.replayed,
        limit: body.limit,
        reason: body.reason ?? null,
      },
      'Dead-lettered jobs replayed',
    );
    return reply.status(200).send(result);
  });

  app.delete(
    '/queues/:name/dlq/:jobId',
    { config: { access: 'manage' } },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const { jobId } = dlqJobParamsSchema.parse(request.params);
      await dlqService.purge(principalOf(request).tenantId, name as QueueName, jobId as JobId);
      request.log.info({ event: 'dlq.purged', jobId, queue: name }, 'Dead-lettered job purged');
      return reply.status(204).send();
    },
  );

  app.delete('/queues/:name/dlq', { config: { access: 'manage' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const query = purgeDeadLettersQuerySchema.parse(request.query);
    const result = await dlqService.purgeOlderThan({
      tenantId: principalOf(request).tenantId,
      queue: name as QueueName,
      olderThan: query.olderThan,
      limit: query.limit,
    });
    request.log.info(
      {
        event: 'dlq.bulk_purged',
        queue: name,
        purged: result.purged,
        olderThan: query.olderThan.toISOString(),
        limit: query.limit,
      },
      'Dead-lettered jobs purged',
    );
    return reply.status(200).send(result);
  });
}
