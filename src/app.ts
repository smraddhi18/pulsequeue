import { hostname } from 'node:os';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Env } from './config/env.js';
import { createPool, pingDatabase, type Pool } from './db/pool.js';
import { registerRequestId } from './plugins/request-id.js';
import { registerErrorHandler } from './plugins/error-handler.js';
import { createRedisServices } from './redis/index.js';
import { DeadLetterRepository } from './repositories/dead-letter.repository.js';
import { JobRepository } from './repositories/job.repository.js';
import { QueueRepository } from './repositories/queue.repository.js';
import { ScheduleRepository } from './repositories/schedule.repository.js';
import { QueueStatsRepository } from './repositories/stats.repository.js';
import { registerRoutes } from './routes/index.js';
import { DlqService } from './services/dlq.service.js';
import { HealthService } from './services/health.service.js';
import { JobService } from './services/job.service.js';
import { ScheduleService } from './services/schedule.service.js';
import { StatsCache } from './services/stats-cache.js';
import { StatsService } from './services/stats.service.js';

export interface BuildAppOptions {
  env: Env;
  /** Pass false to silence logs in tests. */
  logger?: boolean;
  /**
   * Inject an existing pool (tests). When omitted, the app creates one from
   * env and owns it: it is closed in the onClose hook. An injected pool is
   * left open for its owner to close.
   */
  pool?: Pool;
  /** Write JSON log lines here instead of stdout (tests assert on structured events). */
  logStream?: { write(line: string): void };
}

/**
 * Build the Fastify application (no listen). Used by server.ts and tests.
 * This is the composition root: config -> pool + Redis -> repositories ->
 * services -> routes. Redis (optional) is always owned by the app and closed
 * in onClose after in-flight requests finished: pending wake-ups and
 * invalidations are flushed first, then the connection quits.
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
            ...(options.logStream ? { stream: options.logStream } : {}),
          },
    trustProxy: true,
  });

  const ownsPool = options.pool === undefined;
  const pool = options.pool ?? createPool(env, { logger: app.log });
  if (ownsPool) {
    // Runs after the server stops accepting connections and in-flight
    // requests finish, so no query is cut off mid-flight.
    app.addHook('onClose', async () => {
      await pool.end();
      app.log.info('Postgres pool closed');
    });
  }

  const redis = createRedisServices(env, {
    logger: app.log,
    role: 'api',
    id: `${hostname()}-${process.pid}`,
  });
  app.addHook('onClose', async () => {
    await redis.close();
    if (redis.client) app.log.info({ event: 'redis.closed' }, 'Redis connection closed');
  });

  const healthService = new HealthService({
    probes: { postgres: () => pingDatabase(pool) },
    // Redis is optional: failing it reports degraded, not not_ready.
    ...(redis.probe ? { optionalProbes: { redis: redis.probe } } : {}),
    timeoutMs: env.READINESS_TIMEOUT_MS,
    onProbeFailure: (name, err) => app.log.warn({ err, check: name }, 'Readiness check failed'),
  });
  const queueRepository = new QueueRepository(pool);
  const jobService = new JobService(queueRepository, new JobRepository(pool), {
    maxPayloadBytes: env.JOB_MAX_PAYLOAD_BYTES,
    signals: redis.signals,
  });
  const dlqService = new DlqService(queueRepository, new DeadLetterRepository(pool), {
    signals: redis.signals,
  });
  const scheduleService = new ScheduleService(queueRepository, new ScheduleRepository(pool), {
    maxPayloadBytes: env.JOB_MAX_PAYLOAD_BYTES,
  });
  const statsService = new StatsService(
    new QueueStatsRepository(pool),
    new StatsCache({
      store: redis.statsStore,
      minRefreshMs: env.STATS_CACHE_MIN_REFRESH_MS,
      logger: app.log,
    }),
  );

  await registerRequestId(app);
  await registerErrorHandler(app);
  await registerRoutes(app, {
    healthService,
    jobService,
    dlqService,
    scheduleService,
    statsService,
    maxPayloadBytes: env.JOB_MAX_PAYLOAD_BYTES,
  });

  return app;
}
