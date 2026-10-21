import { randomBytes } from 'node:crypto';
import { hostname } from 'node:os';
import type { WorkerEnv } from '../config/env.js';
import { createPool, type Pool } from '../db/pool.js';
import type { Logger } from '../lib/logger.js';
import { LeaseRepository } from '../repositories/lease.repository.js';
import { Reaper } from './reaper.js';
import type { HandlerRegistry } from './registry.js';
import { Worker, type JobFailedHook, type StopResult } from './worker.js';

export interface WorkerRuntime {
  worker: Worker;
  reaper: Reaper;
  queues: string[];
  start(): void;
  /** Stop the reaper, drain the worker, then close the pool if the runtime owns it. */
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
 * config -> pool -> LeaseRepository -> Worker + Reaper.
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

  const store = new LeaseRepository(pool);
  const worker = new Worker({
    store,
    registry: options.registry,
    logger,
    workerId,
    queues,
    concurrency: env.WORKER_CONCURRENCY,
    pollIntervalMs: env.WORKER_POLL_INTERVAL_MS,
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
    ...(options.onJobFailed ? { onJobFailed: options.onJobFailed } : {}),
  });

  let stopPromise: Promise<StopResult> | null = null;
  return {
    worker,
    reaper,
    queues,
    start() {
      worker.start();
      reaper.start();
    },
    stop() {
      stopPromise ??= (async () => {
        await reaper.stop();
        const result = await worker.stop({ timeoutMs: env.WORKER_SHUTDOWN_TIMEOUT_MS });
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
