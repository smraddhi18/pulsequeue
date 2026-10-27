import { hostname } from 'node:os';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Env } from './config/env.js';
import type { QueueName } from './domain/job.js';
import { createPool, pingDatabase, type Pool } from './db/pool.js';
import { registerAuth } from './plugins/auth.js';
import { registerRequestId } from './plugins/request-id.js';
import { registerErrorHandler } from './plugins/error-handler.js';
import { createRedisServices } from './redis/index.js';
import { ApiKeyRepository } from './repositories/api-key.repository.js';
import { DeadLetterRepository } from './repositories/dead-letter.repository.js';
import { JobRepository } from './repositories/job.repository.js';
import { QueueRepository } from './repositories/queue.repository.js';
import { ScheduleRepository } from './repositories/schedule.repository.js';
import { QueueStatsRepository } from './repositories/stats.repository.js';
import { registerRoutes } from './routes/index.js';
import { ApiKeyService } from './services/api-key.service.js';
import { DlqService } from './services/dlq.service.js';
import { HealthService } from './services/health.service.js';
import { JobService } from './services/job.service.js';
import { QueueService } from './services/queue.service.js';
import { RateLimiter } from './services/rate-limiter.js';
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
 * services -> auth -> routes. Every route except /health and /ready requires
 * an API key (see plugins/auth.ts). Redis (optional) is always owned by the app and closed
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
    rateLimit: env.RATE_LIMIT_ENABLED,
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
  const apiKeyService = new ApiKeyService(new ApiKeyRepository(pool), {
    cacheTtlMs: env.AUTH_CACHE_TTL_MS,
    cacheMaxEntries: env.AUTH_CACHE_MAX_ENTRIES,
    logger: app.log,
  });
  const queueRepository = new QueueRepository(pool);
  const jobService = new JobService(queueRepository, new JobRepository(pool), {
    maxPayloadBytes: env.JOB_MAX_PAYLOAD_BYTES,
    signals: redis.signals,
  });
  // Rate limiting needs shared buckets: off without Redis (fail-open by design).
  const rateLimiter = new RateLimiter({
    store: redis.rateLimitStore,
    failMode: env.RATE_LIMIT_FAIL_MODE,
    defaultTenantLimit: {
      perSec: env.RATE_LIMIT_TENANT_PER_SEC,
      burst: env.RATE_LIMIT_TENANT_BURST,
    },
    loadQueueLimit: async (tenantId, queue) =>
      (await queueRepository.findSettings(tenantId, queue as QueueName))?.rateLimit ?? null,
    settingsCacheTtlMs: env.RATE_LIMIT_SETTINGS_CACHE_TTL_MS,
    logger: app.log,
  });
  if (env.RATE_LIMIT_ENABLED && !rateLimiter.enabled) {
    app.log.warn(
      { event: 'ratelimit.disabled', reason: 'no_redis' },
      'RATE_LIMIT_ENABLED but REDIS_URL is unset: enqueues are not rate limited',
    );
  }
  const queueService = new QueueService(queueRepository, {
    onSettingsChanged: (settings, tenantId) => rateLimiter.forgetQueue(tenantId, settings.queue),
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
  // Before the routes: its onRoute hook checks that each one declares an access policy.
  await registerAuth(app, { apiKeyService });
  await registerRoutes(app, {
    healthService,
    apiKeyService,
    jobService,
    queueService,
    dlqService,
    scheduleService,
    statsService,
    rateLimiter,
    maxPayloadBytes: env.JOB_MAX_PAYLOAD_BYTES,
  });

  return app;
}
