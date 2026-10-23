import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';
import type { DlqService } from '../services/dlq.service.js';
import type { JobService } from '../services/job.service.js';
import type { ScheduleService } from '../services/schedule.service.js';
import { dlqRoutes } from './dlq.js';
import { healthRoutes } from './health.js';
import { jobRoutes } from './jobs.js';
import { readinessRoutes } from './readiness.js';
import { scheduleRoutes } from './schedules.js';

export interface RouteDeps {
  healthService: HealthService;
  jobService: JobService;
  dlqService: DlqService;
  scheduleService: ScheduleService;
  maxPayloadBytes: number;
}

export async function registerRoutes(app: FastifyInstance, deps: RouteDeps): Promise<void> {
  await app.register(healthRoutes, { healthService: deps.healthService });
  await app.register(readinessRoutes, { healthService: deps.healthService });
  await app.register(jobRoutes, {
    jobService: deps.jobService,
    maxPayloadBytes: deps.maxPayloadBytes,
  });
  await app.register(dlqRoutes, { dlqService: deps.dlqService });
  await app.register(scheduleRoutes, {
    scheduleService: deps.scheduleService,
    maxPayloadBytes: deps.maxPayloadBytes,
  });
}
