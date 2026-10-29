import { hostname } from 'node:os';
import Fastify, { type FastifyInstance } from 'fastify';
import type { Registry } from 'prom-client';
import type { Env } from './config/env.js';
import type { QueueName } from './domain/job.js';
import { createPool, pingDatabase, type Pool } from './db/pool.js';
import { ApiMetrics } from './observability/metrics.js';
import { MetricsServer } from './observability/metrics-server.js';
import { QueueDepthCollector } from './observability/queue-depth.js';
import { createMetricsRegistry, enableDefaultMetrics } from './observability/registry.js';
import { SeriesLimiter } from './observability/series-limiter.js';
import { createTracing, tracingConfigFromEnv, type Tracing } from './observability/tracing.js';
import { registerAuth } from './plugins/auth.js';
import { registerHttpObservability } from './plugins/observability.js';
import { registerRequestId } from './plugins/request-id.js';
import { registerErrorHandler } from './plugins/error-handler.js';
import { createRedisServices } from './redis/index.js';
import { ApiKeyRepository } from './repositories/api-key.repository.js';
import { DashboardRepository } from './repositories/dashboard.repository.js';
import { DeadLetterRepository } from './repositories/dead-letter.repository.js';
import { JobRepository } from './repositories/job.repository.js';
import { QueueRepository } from './repositories/queue.repository.js';
import { ScheduleRepository } from './repositories/schedule.repository.js';
import { QueueStatsRepository } from './repositories/stats.repository.js';
import { registerRoutes } from './routes/index.js';
import { ApiKeyService } from './services/api-key.service.js';
import { DashboardService } from './services/dashboard.service.js';
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
  /**
   * Tracing to use (tests inject one with an in-memory exporter). When
   * omitted it is built from env (off unless TRACING_ENABLED) and owned by
   * the app: flushed and shut down in onClose.
   */
  tracing?: Tracing;
  /** Process metrics (CPU, heap, event loop). Default: on when METRICS_PORT is set, outside tests. */
  defaultMetrics?: boolean;
}

/** Per-app observability handles (tests read the registry and the metrics port). */
export interface AppObservability {
  registry: Registry;
  metrics: ApiMetrics;
  /** null unless METRICS_PORT is set. */
  metricsServer: MetricsServer | null;
  tracing: Tracing;
}

declare module 'fastify' {
  interface FastifyInstance {
    observability: AppObservability;
    /** Graceful shutdown step 1 (Milestone 8): /ready turns 503, traffic keeps being served. */
    beginDraining(): void;
  }
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
    // Milestone 8: bound every request (slowloris -> 408) and keep idle
    // keep-alive sockets open longer than the load balancer's idle timeout.
    requestTimeout: env.HTTP_REQUEST_TIMEOUT_MS,
    keepAliveTimeout: env.HTTP_KEEP_ALIVE_TIMEOUT_MS,
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
  const dashboardService = new DashboardService(queueRepository, new DashboardRepository(pool), {
    latencySampleLimit: env.DASHBOARD_LATENCY_MAX_SAMPLES,
  });

  // --- Observability (M7) -------------------------------------------------
  const registry = createMetricsRegistry('api');
  if (options.defaultMetrics ?? (env.METRICS_PORT !== undefined && env.NODE_ENV !== 'test')) {
    enableDefaultMetrics(registry);
  }
  const metrics = new ApiMetrics(registry, new SeriesLimiter(env.METRICS_MAX_QUEUE_SERIES));
  if (env.METRICS_QUEUE_DEPTH_ENABLED) {
    new QueueDepthCollector({
      pool,
      registry,
      logger: app.log,
      maxQueues: env.METRICS_MAX_QUEUE_SERIES,
      cacheMs: env.METRICS_QUEUE_DEPTH_CACHE_MS,
      timeoutMs: env.METRICS_QUEUE_DEPTH_TIMEOUT_MS,
    });
  }
  const tracing = options.tracing ?? createTracing(tracingConfigFromEnv(env, 'pulsequeue-api'));
  if (options.tracing === undefined && tracing.enabled) {
    // onClose runs after in-flight requests finished, so their spans have
    // ended; shutdown flushes the batch processor before the process exits.
    app.addHook('onClose', async () => {
      await tracing.shutdown();
    });
  }
  const metricsServer =
    env.METRICS_PORT !== undefined
      ? new MetricsServer({
          registry,
          host: env.METRICS_HOST,
          port: env.METRICS_PORT,
          token: env.METRICS_TOKEN,
          logger: app.log,
        })
      : null;
  if (metricsServer !== null) {
    await metricsServer.start();
    app.addHook('onClose', async () => {
      await metricsServer.stop();
    });
  }
  app.decorate('observability', { registry, metrics, metricsServer, tracing });
  app.decorate('beginDraining', () => {
    if (!healthService.isDraining) app.log.info({ event: 'shutdown.draining' }, 'Draining');
    healthService.beginDraining();
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
  // After request-id, before auth: trace ids join the request logger first.
  await registerHttpObservability(app, { metrics, tracing });
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
    dashboardService,
    rateLimiter,
    maxPayloadBytes: env.JOB_MAX_PAYLOAD_BYTES,
    enqueueMetrics: metrics,
    tracing,
  });

  return app;
}
