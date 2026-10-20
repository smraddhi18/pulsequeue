import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';
import type { JobService } from '../services/job.service.js';
import { healthRoutes } from './health.js';
import { jobRoutes } from './jobs.js';
import { readinessRoutes } from './readiness.js';

export interface RouteDeps {
  healthService: HealthService;
  jobService: JobService;
  maxPayloadBytes: number;
}

export async function registerRoutes(app: FastifyInstance, deps: RouteDeps): Promise<void> {
  await app.register(healthRoutes, { healthService: deps.healthService });
  await app.register(readinessRoutes, { healthService: deps.healthService });
  await app.register(jobRoutes, {
    jobService: deps.jobService,
    maxPayloadBytes: deps.maxPayloadBytes,
  });
}
