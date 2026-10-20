import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';

export async function readinessRoutes(
  app: FastifyInstance,
  opts: { healthService: HealthService },
): Promise<void> {
  const { healthService } = opts;

  /**
   * Readiness probe — safe to receive traffic. 200 only when every dependency
   * probe (Postgres from M1) passes; 503 otherwise so the load balancer stops
   * routing here while the process stays alive (liveness is separate).
   */
  app.get('/ready', async (_request, reply) => {
    const body = await healthService.getReadiness();
    const statusCode = body.status === 'ready' ? 200 : 503;
    return reply.status(statusCode).send(body);
  });
}
