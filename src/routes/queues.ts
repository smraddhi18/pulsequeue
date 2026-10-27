import type { FastifyInstance } from 'fastify';
import type { QueueName } from '../domain/job.js';
import type { QueueSettings } from '../domain/queue.js';
import type { RateLimit } from '../domain/tenant.js';
import type { QueueService } from '../services/queue.service.js';
import { queueParamsSchema } from './jobs.schemas.js';
import { updateQueueSettingsBodySchema } from './queues.schemas.js';
import { principalOf } from '../plugins/auth.js';

export interface QueueRoutesOptions {
  queueService: QueueService;
}

export interface QueueSettingsResponse {
  queue: string;
  maxConcurrency: number | null;
  rateLimit: RateLimit | null;
  updatedAt: string;
}

export function toQueueSettingsResponse(settings: QueueSettings): QueueSettingsResponse {
  return {
    queue: settings.queue,
    maxConcurrency: settings.maxConcurrency,
    rateLimit: settings.rateLimit,
    updatedAt: settings.updatedAt.toISOString(),
  };
}

export async function queueRoutes(app: FastifyInstance, opts: QueueRoutesOptions): Promise<void> {
  const { queueService } = opts;

  app.get('/queues/:name/settings', { config: { access: 'read' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const settings = await queueService.getSettings(
      principalOf(request).tenantId,
      name as QueueName,
    );
    return reply.status(200).send(toQueueSettingsResponse(settings));
  });

  /** Admin only. Takes effect on the next lease (concurrency) and the next enqueue (rate). */
  app.put('/queues/:name/settings', { config: { access: 'manage' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const body = updateQueueSettingsBodySchema.parse(request.body);
    const principal = principalOf(request);
    const settings = await queueService.updateSettings({
      tenantId: principal.tenantId,
      queue: name as QueueName,
      maxConcurrency: body.maxConcurrency,
      rateLimit: body.rateLimit,
    });
    request.log.info(
      {
        event: 'queue.settings_updated',
        tenantId: principal.tenantId,
        keyId: principal.keyId,
        queue: settings.queue,
        maxConcurrency: settings.maxConcurrency,
        rateLimit: settings.rateLimit,
      },
      'Queue settings updated',
    );
    return reply.status(200).send(toQueueSettingsResponse(settings));
  });
}
