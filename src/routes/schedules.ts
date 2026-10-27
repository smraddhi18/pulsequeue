import type { FastifyInstance } from 'fastify';
import type { QueueName } from '../domain/job.js';
import type { Schedule } from '../domain/schedule.js';
import type { ScheduleService } from '../services/schedule.service.js';
import { queueParamsSchema } from './jobs.schemas.js';
import {
  createScheduleBodySchema,
  listSchedulesQuerySchema,
  scheduleParamsSchema,
} from './schedules.schemas.js';
import { principalOf } from '../plugins/auth.js';

export interface ScheduleRoutesOptions {
  scheduleService: ScheduleService;
  maxPayloadBytes: number;
}

/** Public JSON contract for a schedule. Built explicitly so storage columns never leak. */
export interface ScheduleResponse {
  id: string;
  queue: string;
  name: string;
  cron: string;
  timezone: string;
  payload: unknown;
  priority: number;
  maxAttempts: number;
  misfirePolicy: Schedule['misfirePolicy'];
  enabled: boolean;
  nextRunAt: string | null;
  lastRunAt: string | null;
  lastJobId: string | null;
  fireCount: number;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

export function toScheduleResponse(s: Schedule): ScheduleResponse {
  return {
    id: s.id,
    queue: s.queue,
    name: s.name,
    cron: s.cron,
    timezone: s.timezone,
    payload: s.payload,
    priority: s.priority,
    maxAttempts: s.maxAttempts,
    misfirePolicy: s.misfirePolicy,
    enabled: s.enabled,
    nextRunAt: s.nextRunAt?.toISOString() ?? null,
    lastRunAt: s.lastRunAt?.toISOString() ?? null,
    lastJobId: s.lastJobId,
    fireCount: s.fireCount,
    lastError: s.lastError,
    createdAt: s.createdAt.toISOString(),
    updatedAt: s.updatedAt.toISOString(),
  };
}

/**
 * Schedules API. Collection routes live under the queue they enqueue into;
 * item routes are addressed by id, like /jobs/:id.
 */
export async function scheduleRoutes(
  app: FastifyInstance,
  opts: ScheduleRoutesOptions,
): Promise<void> {
  const { scheduleService } = opts;

  app.post(
    '/queues/:name/schedules',
    { config: { access: 'manage' }, bodyLimit: opts.maxPayloadBytes * 2 + 64 * 1024 },
    async (request, reply) => {
      const { name } = queueParamsSchema.parse(request.params);
      const body = createScheduleBodySchema.parse(request.body);

      const schedule = await scheduleService.create({
        tenantId: principalOf(request).tenantId,
        queue: name as QueueName,
        name: body.name,
        cron: body.cron,
        payload: body.payload,
        ...(body.timezone !== undefined ? { timezone: body.timezone } : {}),
        ...(body.priority !== undefined ? { priority: body.priority } : {}),
        ...(body.maxAttempts !== undefined ? { maxAttempts: body.maxAttempts } : {}),
        ...(body.misfirePolicy !== undefined ? { misfirePolicy: body.misfirePolicy } : {}),
        ...(body.enabled !== undefined ? { enabled: body.enabled } : {}),
      });

      request.log.info(
        {
          event: 'schedule.created',
          scheduleId: schedule.id,
          queue: schedule.queue,
          name: schedule.name,
          cron: schedule.cron,
          timezone: schedule.timezone,
          nextRunAt: schedule.nextRunAt?.toISOString() ?? null,
        },
        'Schedule created',
      );
      reply.header('location', `/schedules/${schedule.id}`);
      return reply.status(201).send(toScheduleResponse(schedule));
    },
  );

  app.get('/queues/:name/schedules', { config: { access: 'read' } }, async (request, reply) => {
    const { name } = queueParamsSchema.parse(request.params);
    const query = listSchedulesQuerySchema.parse(request.query);
    const page = await scheduleService.list({
      tenantId: principalOf(request).tenantId,
      queue: name as QueueName,
      limit: query.limit,
      ...(query.enabled !== undefined ? { enabled: query.enabled } : {}),
      ...(query.cursor !== undefined ? { cursor: query.cursor } : {}),
    });
    return reply.status(200).send({
      data: page.schedules.map(toScheduleResponse),
      nextCursor: page.nextCursor,
    });
  });

  app.get('/schedules/:id', { config: { access: 'read' } }, async (request, reply) => {
    const { id } = scheduleParamsSchema.parse(request.params);
    return reply
      .status(200)
      .send(toScheduleResponse(await scheduleService.get(principalOf(request).tenantId, id)));
  });

  app.post('/schedules/:id/pause', { config: { access: 'manage' } }, async (request, reply) => {
    const { id } = scheduleParamsSchema.parse(request.params);
    const schedule = await scheduleService.pause(principalOf(request).tenantId, id);
    request.log.info(
      { event: 'schedule.paused', scheduleId: id, queue: schedule.queue, name: schedule.name },
      'Schedule paused',
    );
    return reply.status(200).send(toScheduleResponse(schedule));
  });

  app.post('/schedules/:id/resume', { config: { access: 'manage' } }, async (request, reply) => {
    const { id } = scheduleParamsSchema.parse(request.params);
    const schedule = await scheduleService.resume(principalOf(request).tenantId, id);
    request.log.info(
      {
        event: 'schedule.resumed',
        scheduleId: id,
        queue: schedule.queue,
        name: schedule.name,
        nextRunAt: schedule.nextRunAt?.toISOString() ?? null,
      },
      'Schedule resumed',
    );
    return reply.status(200).send(toScheduleResponse(schedule));
  });

  app.delete('/schedules/:id', { config: { access: 'manage' } }, async (request, reply) => {
    const { id } = scheduleParamsSchema.parse(request.params);
    await scheduleService.delete(principalOf(request).tenantId, id);
    request.log.info({ event: 'schedule.deleted', scheduleId: id }, 'Schedule deleted');
    return reply.status(204).send();
  });
}
