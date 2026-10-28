import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';
import type { SchedulerEnv } from '../config/env.js';
import type { Logger } from '../lib/logger.js';
import type { Registry } from 'prom-client';
import { PromSchedulerMetrics } from '../observability/metrics.js';
import { MetricsServer } from '../observability/metrics-server.js';
import { createMetricsRegistry, enableDefaultMetrics } from '../observability/registry.js';
import { createRedisServices, type RedisServices } from '../redis/index.js';
import { PgAdvisoryLeaderLock, type LeaderLock } from './leader-lock.js';
import { Scheduler } from './scheduler.js';
import { runTick } from './tick.js';

export interface SchedulerRuntime {
  scheduler: Scheduler;
  schedulerId: string;
  /** Publishes wake-ups/invalidations for queues a tick fired into. */
  redis: RedisServices;
  /** M7: scheduler metrics and their /metrics listener (null without METRICS_PORT). */
  observability: {
    registry: Registry;
    metrics: PromSchedulerMetrics;
    metricsServer: MetricsServer | null;
  };
  start(): void;
  /**
   * Finish the in-flight tick (bounded by SCHEDULER_SHUTDOWN_TIMEOUT_MS),
   * release leadership, then flush pending signals and close Redis.
   */
  stop(): Promise<void>;
}

export interface CreateSchedulerRuntimeOptions {
  logger: Logger;
  /** Inject a lock (tests); otherwise a PgAdvisoryLeaderLock on DATABASE_URL. */
  lock?: LeaderLock;
  /** Extra libpq startup options for the leader session, e.g. `-c search_path=...`. */
  pgOptions?: string;
  /** Process metrics (CPU, heap, event loop). Default: on when METRICS_PORT is set, outside tests. */
  defaultMetrics?: boolean;
}

export function defaultSchedulerId(): string {
  return `${hostname()}-${process.pid}-${randomBytes(3).toString('hex')}`;
}

/**
 * Composition root for the scheduler process: config -> leader lock (one
 * dedicated session) -> tick -> Scheduler loop. The scheduler needs no pool:
 * all of its queries run on the lock-holding session. Redis (optional) only
 * carries post-commit signals for the queues each tick fired into.
 */
export function createSchedulerRuntime(
  env: SchedulerEnv,
  options: CreateSchedulerRuntimeOptions,
): SchedulerRuntime {
  const schedulerId = env.SCHEDULER_ID ?? defaultSchedulerId();
  const logger = options.logger.child({ component: 'scheduler', schedulerId });

  const lock =
    options.lock ??
    new PgAdvisoryLeaderLock({
      connectionString: env.DATABASE_URL,
      logger,
      connectionTimeoutMs: env.PG_CONNECTION_TIMEOUT_MS,
      statementTimeoutMs: env.PG_STATEMENT_TIMEOUT_MS,
      ...(options.pgOptions !== undefined ? { options: options.pgOptions } : {}),
    });

  const redis = createRedisServices(env, { logger, role: 'scheduler', id: schedulerId });

  // Tracing is not wired here: fired jobs start their own trace in the
  // worker (see README, "Observability"). Metrics cover the leader loop.
  const registry = createMetricsRegistry('scheduler');
  if (options.defaultMetrics ?? (env.METRICS_PORT !== undefined && env.NODE_ENV !== 'test')) {
    enableDefaultMetrics(registry);
  }
  const metrics = new PromSchedulerMetrics(registry);
  const metricsServer =
    env.METRICS_PORT !== undefined
      ? new MetricsServer({
          registry,
          host: env.METRICS_HOST,
          port: env.METRICS_PORT,
          token: env.METRICS_TOKEN,
          logger,
        })
      : null;

  const scheduler = new Scheduler({
    lock,
    metrics,
    logger,
    schedulerId,
    tick: (db) =>
      runTick(db, {
        batchSize: env.SCHEDULER_BATCH_SIZE,
        graceMs: env.SCHEDULER_MISFIRE_GRACE_MS,
        maxCatchUp: env.SCHEDULER_MAX_CATCH_UP,
        logger,
        onCommitted: (queues) => {
          for (const queue of queues) redis.signals.jobsRunnable(queue);
        },
      }),
    tickIntervalMs: env.SCHEDULER_TICK_INTERVAL_MS,
    leaderRetryIntervalMs: env.SCHEDULER_LEADER_RETRY_INTERVAL_MS,
    batchSize: env.SCHEDULER_BATCH_SIZE,
    ...(env.SCHEDULER_LIVENESS_FILE ? { livenessFile: env.SCHEDULER_LIVENESS_FILE } : {}),
  });

  return {
    scheduler,
    schedulerId,
    redis,
    observability: { registry, metrics, metricsServer },
    start: () => {
      scheduler.start();
      metricsServer?.start().catch((err: unknown) => {
        logger.error({ event: 'metrics.listen_failed', err }, 'Metrics endpoint failed to start');
      });
    },
    stop: async () => {
      try {
        await scheduler.stop({ timeoutMs: env.SCHEDULER_SHUTDOWN_TIMEOUT_MS });
      } finally {
        await redis.close();
        await metricsServer?.stop();
      }
    },
  };
}
