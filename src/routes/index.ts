import type { FastifyInstance } from 'fastify';
import type { ApiKeyService } from '../services/api-key.service.js';
import type { HealthService } from '../services/health.service.js';
import type { DlqService } from '../services/dlq.service.js';
import type { JobService } from '../services/job.service.js';
import type { QueueService } from '../services/queue.service.js';
import type { RateLimiter } from '../services/rate-limiter.js';
import type { ScheduleService } from '../services/schedule.service.js';
import type { StatsService } from '../services/stats.service.js';
import { apiKeyRoutes } from './api-keys.js';
import { dlqRoutes } from './dlq.js';
import { healthRoutes } from './health.js';
import { jobRoutes } from './jobs.js';
import { queueRoutes } from './queues.js';
import { readinessRoutes } from './readiness.js';
import { scheduleRoutes } from './schedules.js';
import { statsRoutes } from './stats.js';

export interface RouteDeps {
  healthService: HealthService;
  apiKeyService: ApiKeyService;
  jobService: JobService;
  queueService: QueueService;
  dlqService: DlqService;
  scheduleService: ScheduleService;
  statsService: StatsService;
  rateLimiter: RateLimiter;
  maxPayloadBytes: number;
}

export async function registerRoutes(app: FastifyInstance, deps: RouteDeps): Promise<void> {
  await app.register(healthRoutes, { healthService: deps.healthService });
  await app.register(readinessRoutes, { healthService: deps.healthService });
  await app.register(jobRoutes, {
    jobService: deps.jobService,
    rateLimiter: deps.rateLimiter,
    maxPayloadBytes: deps.maxPayloadBytes,
  });
  await app.register(dlqRoutes, { dlqService: deps.dlqService });
  await app.register(scheduleRoutes, {
    scheduleService: deps.scheduleService,
    maxPayloadBytes: deps.maxPayloadBytes,
  });
  await app.register(queueRoutes, { queueService: deps.queueService });
  await app.register(statsRoutes, { statsService: deps.statsService });
  await app.register(apiKeyRoutes, { apiKeyService: deps.apiKeyService });
}
