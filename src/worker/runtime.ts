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
  });
  const reaper = new Reaper({
    store,
    logger: logger.child({ workerId, subcomponent: 'reaper' }),
    intervalMs: env.WORKER_REAPER_INTERVAL_MS,
    poisonLeaseExpiries: env.WORKER_POISON_LEASE_EXPIRIES,
    signals: redis.signals,
    ...(options.onJobFailed ? { onJobFailed: options.onJobFailed } : {}),
  });

  wakeTarget.worker = worker;
  let stopPromise: Promise<StopResult> | null = null;
  return {
    worker,
    reaper,
    queues,
    redis,
    subscriber,
    start() {
      worker.start();
      reaper.start();
      subscriber?.start();
    },
    stop() {
      stopPromise ??= (async () => {
        await subscriber?.close();
        await reaper.stop();
        const result = await worker.stop({ timeoutMs: env.WORKER_SHUTDOWN_TIMEOUT_MS });
        // After the drain: released leases still publish their wake-ups.
        await redis.close();
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
