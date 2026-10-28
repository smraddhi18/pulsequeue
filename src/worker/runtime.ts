import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';
import type { WorkerEnv } from '../config/env.js';
import { createPool, type Pool } from '../db/pool.js';
import type { Logger } from '../lib/logger.js';
import {
  createRedisServices,
  createWakeupSubscriber,
  type RedisServices,
  type WakeupSubscriber,
} from '../redis/index.js';
import { PromWorkerMetrics } from '../observability/metrics.js';
import { MetricsServer } from '../observability/metrics-server.js';
import { createMetricsRegistry, enableDefaultMetrics } from '../observability/registry.js';
import { SeriesLimiter } from '../observability/series-limiter.js';
import { createTracing, tracingConfigFromEnv, type Tracing } from '../observability/tracing.js';
import type { Registry } from 'prom-client';
import { LeaseRepository } from '../repositories/lease.repository.js';
import { Reaper } from './reaper.js';
import type { HandlerRegistry } from './registry.js';
import { Worker, type JobFailedHook, type StopResult } from './worker.js';

export interface WorkerRuntime {
  worker: Worker;
  reaper: Reaper;
  queues: string[];
  /** Command connection (signals) and, with wake-ups enabled, the subscriber. */
  redis: RedisServices;
  subscriber: WakeupSubscriber | null;
  /** M7: this process's metrics, its /metrics listener (null without METRICS_PORT) and tracing. */
  observability: {
    registry: Registry;
    metrics: PromWorkerMetrics;
    metricsServer: MetricsServer | null;
    tracing: Tracing;
  };
  /** Starts the worker, reaper and subscriber; the metrics listener starts in the background. */
  start(): void;
  /**
   * Stop listening for wake-ups, stop the reaper, drain the worker, flush
   * pending signals and close Redis, then close the pool if the runtime owns it.
   */
  stop(): Promise<StopResult>;
}

export interface CreateWorkerRuntimeOptions {
  registry: HandlerRegistry;
  logger: Logger;
  /** Inject a pool (tests); otherwise one is created from env and closed on stop. */
  pool?: Pool;
  onJobFailed?: JobFailedHook;
  /** Inject tracing (tests); otherwise built from env and shut down on stop. */
  tracing?: Tracing;
  /** Process metrics (CPU, heap, event loop). Default: on when METRICS_PORT is set, outside tests. */
  defaultMetrics?: boolean;
}

/** host-pid-random: unique per process, readable in logs and jobs.locked_by. */
export function defaultWorkerId(): string {
  return `${hostname()}-${process.pid}-${randomBytes(3).toString('hex')}`;
}

/**
 * Composition root for the worker process:
 * config -> pool + Redis -> LeaseRepository -> Worker + Reaper, with the
 * wake-up subscriber driving Worker.wake().
 */
export function createWorkerRuntime(
  env: WorkerEnv,
  options: CreateWorkerRuntimeOptions,
): WorkerRuntime {
  const workerId = env.WORKER_ID ?? defaultWorkerId();
  const logger = options.logger.child({ component: 'worker' });
  const queues = options.registry.resolveQueues(env.WORKER_QUEUES);

  const ownsPool = options.pool === undefined;
  const pool = options.pool ?? createPool(env, { logger });
  if (ownsPool && env.PG_POOL_MAX < 2) {
    logger.warn(
      { PG_POOL_MAX: env.PG_POOL_MAX },
      'PG_POOL_MAX < 2: heartbeats will queue behind lease/complete queries',
    );
  }

  const redis = createRedisServices(env, { logger, role: 'worker', id: workerId });
  // The subscriber and the worker reference each other (wake() vs
  // isListening); callbacks only fire after start(), when this is set.
  const wakeTarget: { worker?: Worker } = {};
  const subscriber = createWakeupSubscriber(env, {
    logger: logger.child({ workerId, subcomponent: 'wakeups' }),
    id: workerId,
    queues,
    onWake: () => wakeTarget.worker?.wake(),
    // Re-subscribed: poll once to catch anything published during the gap.
    // Lost: wake so the idle sleep is re-armed with the short poll interval.
    onListeningChange: () => wakeTarget.worker?.wake(),
  });

  const registry = createMetricsRegistry('worker');
  if (options.defaultMetrics ?? (env.METRICS_PORT !== undefined && env.NODE_ENV !== 'test')) {
    enableDefaultMetrics(registry);
  }
  const metrics = new PromWorkerMetrics(registry, new SeriesLimiter(env.METRICS_MAX_QUEUE_SERIES));
  const ownsTracing = options.tracing === undefined;
  const tracing = options.tracing ?? createTracing(tracingConfigFromEnv(env, 'pulsequeue-worker'));
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

  const store = new LeaseRepository(pool);
  const worker = new Worker({
    store,
    registry: options.registry,
    logger,
    workerId,
    queues,
    concurrency: env.WORKER_CONCURRENCY,
    pollIntervalMs: env.WORKER_POLL_INTERVAL_MS,
    ...(subscriber
      ? { wakeups: subscriber, fallbackPollIntervalMs: env.WORKER_WAKEUP_FALLBACK_POLL_MS }
      : {}),
    signals: redis.signals,
    leaseDurationMs: env.WORKER_LEASE_DURATION_MS,
    heartbeatIntervalMs: env.WORKER_HEARTBEAT_INTERVAL_MS,
    jobTimeoutMs: env.WORKER_JOB_TIMEOUT_MS,
    backoff: { baseMs: env.RETRY_BACKOFF_BASE_MS, maxMs: env.RETRY_BACKOFF_MAX_MS },
    ...(options.onJobFailed ? { onJobFailed: options.onJobFailed } : {}),
    ...(env.WORKER_LIVENESS_FILE ? { livenessFile: env.WORKER_LIVENESS_FILE } : {}),
    metrics,
    tracing,
  });
  metrics.trackInFlight(() => worker.activeJobs);
  const reaper = new Reaper({
    store,
    logger: logger.child({ workerId, subcomponent: 'reaper' }),
    intervalMs: env.WORKER_REAPER_INTERVAL_MS,
    poisonLeaseExpiries: env.WORKER_POISON_LEASE_EXPIRIES,
    signals: redis.signals,
    ...(options.onJobFailed ? { onJobFailed: options.onJobFailed } : {}),
    metrics,
  });

  wakeTarget.worker = worker;
  let stopPromise: Promise<StopResult> | null = null;
  return {
    worker,
    reaper,
    queues,
    redis,
    subscriber,
    observability: { registry, metrics, metricsServer, tracing },
    start() {
      worker.start();
      reaper.start();
      subscriber?.start();
      // A metrics port that cannot bind must not stop job processing.
      metricsServer?.start().catch((err: unknown) => {
        logger.error({ event: 'metrics.listen_failed', err }, 'Metrics endpoint failed to start');
      });
    },
    stop() {
      stopPromise ??= (async () => {
        await subscriber?.close();
        await reaper.stop();
        const result = await worker.stop({ timeoutMs: env.WORKER_SHUTDOWN_TIMEOUT_MS });
        // After the drain: released leases still publish their wake-ups.
        await redis.close();
        await metricsServer?.stop();
        // Flush the last attempts' spans.
        if (ownsTracing) await tracing.shutdown();
        if (ownsPool) {
          await pool.end();
          logger.info('Postgres pool closed');
        }
        return result;
      })();
      return stopPromise;
    },
  };
}
