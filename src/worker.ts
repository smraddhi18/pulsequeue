import { loadWorkerEnv } from './config/env.js';
import { createLogger } from './lib/logger.js';
import { registerBuiltinHandlers } from './worker/handlers/index.js';
import { HandlerRegistry } from './worker/registry.js';
import { createWorkerRuntime } from './worker/runtime.js';

/** Extra time on top of the drain budget for releasing leases and closing the pool. */
const SHUTDOWN_GRACE_MS = 5_000;

/**
 * Worker process entry (`npm run worker`). Leases jobs from Postgres, runs
 * them through the handler registry, heartbeats leases and reaps expired
 * ones. SIGTERM/SIGINT: stop leasing, drain in-flight jobs for up to
 * WORKER_SHUTDOWN_TIMEOUT_MS, release leases of anything unfinished, close
 * the pool, exit 0. Wake-up subscription stops first; pending Redis signals
 * are flushed after the drain.
 */
async function main(): Promise<void> {
  const env = loadWorkerEnv();
  const logger = createLogger(env);
  const registry = registerBuiltinHandlers(new HandlerRegistry());

  const runtime = createWorkerRuntime(env, {
    registry,
    logger,
    // Dead-lettering itself happens inside the fenced fail/reap statement and
    // is logged there (event: dlq.dead_lettered). Pass onJobFailed here to add
    // side effects such as paging or a webhook.
  });

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'Shutting down worker');

    const forceExit = setTimeout(() => {
      logger.error('Graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, env.WORKER_SHUTDOWN_TIMEOUT_MS + SHUTDOWN_GRACE_MS);
    forceExit.unref();

    try {
      const result = await runtime.stop();
      logger.info(result, 'Worker shut down cleanly');
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'Error during worker shutdown');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  runtime.start();
}

main().catch((err: unknown) => {
  // Last resort: config may have failed before the logger exists.
  console.error(err);
  process.exit(1);
});
