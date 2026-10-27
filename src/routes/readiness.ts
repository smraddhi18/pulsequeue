import type { FastifyInstance } from 'fastify';
import type { HealthService } from '../services/health.service.js';

export async function readinessRoutes(
  app: FastifyInstance,
  opts: { healthService: HealthService },
): Promise<void> {
  const { healthService } = opts;

  /**
   * Readiness probe — safe to receive traffic. 503 when a required dependency
   * (Postgres) fails, so the load balancer stops routing here while the
   * process stays alive (liveness is separate). An optional dependency
   * (Redis) failing reports `degraded` with 200: the instance still serves
   * every request correctly, only slower (see HealthService).
   */
  app.get('/ready', { config: { access: 'public' } }, async (_request, reply) => {
    const body = await healthService.getReadiness();
    const statusCode = body.status === 'not_ready' ? 503 : 200;
    return reply.status(statusCode).send(body);
  });
}
