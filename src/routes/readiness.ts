import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';

export async function readinessRoutes(
  app: FastifyInstance,
  opts: { healthService: HealthService },
): Promise<void> {
  const { healthService } = opts;

  /**
   * Readiness probe — safe to receive traffic.
   * M0: app booted. Later: Postgres + Redis connectivity.
   */
  app.get('/ready', async (_request, reply) => {
    const body = healthService.getReadiness();
    const statusCode = body.status === 'ready' ? 200 : 503;
    return reply.status(statusCode).send(body);
  });
}
