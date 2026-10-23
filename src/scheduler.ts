import { loadSchedulerEnv } from './config/env.js';
import { createLogger } from './lib/logger.js';
import { createSchedulerRuntime } from './scheduler/runtime.js';

/** Extra time on top of the tick budget for releasing the lock and closing the session. */
const SHUTDOWN_GRACE_MS = 5_000;

/**
 * Scheduler process entry (`npm run scheduler`). Run two or more replicas:
 * they elect a leader through a Postgres advisory lock and only the leader
 * turns due schedules into jobs. SIGTERM/SIGINT: finish the in-flight tick
 * (up to SCHEDULER_SHUTDOWN_TIMEOUT_MS), release leadership, exit 0.
 */
async function main(): Promise<void> {
  const env = loadSchedulerEnv();
  const logger = createLogger(env);
  const runtime = createSchedulerRuntime(env, { logger });

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'Shutting down scheduler');

    const forceExit = setTimeout(() => {
      logger.error('Graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, env.SCHEDULER_SHUTDOWN_TIMEOUT_MS + SHUTDOWN_GRACE_MS);
    forceExit.unref();

    try {
      await runtime.stop();
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'Error during scheduler shutdown');
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
