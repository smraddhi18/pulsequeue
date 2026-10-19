import Fastify, { type FastifyInstance } from 'fastify';
import type { Env } from './config/env.js';
import { registerRequestId } from './plugins/request-id.js';
import { registerErrorHandler } from './plugins/error-handler.js';
import { registerRoutes } from './routes/index.js';
import { HealthService } from './services/health.service.js';

export interface BuildAppOptions {
  env: Env;
  /** Pass false to silence logs in tests. */
  logger?: boolean;
}

/**
 * Build the Fastify application (no listen). Used by server.ts and tests.
 * Keeps wiring in one place so routes stay thin and free of business logic.
 */
export async function buildApp(options: BuildAppOptions): Promise<FastifyInstance> {
  const { env } = options;

  const app = Fastify({
    logger:
      options.logger === false
        ? false
        : {
            level: env.LOG_LEVEL,
            base: {
              service: 'pulsequeue',
              env: env.NODE_ENV,
            },
            timestamp: () => `,"time":"${new Date().toISOString()}"`,
          },
    trustProxy: true,
  });

  const healthService = new HealthService();

  await registerRequestId(app);
  await registerErrorHandler(app);
  await registerRoutes(app, { healthService });

  return app;
}
