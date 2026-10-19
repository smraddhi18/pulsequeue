import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';

export async function healthRoutes(
  app: FastifyInstance,
  opts: { healthService: HealthService },
): Promise<void> {
  const { healthService } = opts;

  /**
   * Liveness probe — process is up. Kubernetes / load balancers use this;
   * it must not depend on Postgres or Redis.
   */
  app.get('/health', async (_request, reply) => {
    const body = healthService.getLiveness();
    return reply.status(200).send(body);
  });
}
