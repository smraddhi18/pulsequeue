import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';
import { healthRoutes } from './health.js';
import { readinessRoutes } from './readiness.js';

export async function registerRoutes(
  app: FastifyInstance,
  deps: { healthService: HealthService },
): Promise<void> {
  await app.register(healthRoutes, { healthService: deps.healthService });
  await app.register(readinessRoutes, { healthService: deps.healthService });
}
