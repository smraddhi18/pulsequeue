import { loadEnv } from './config/env.js';
import { buildApp } from './app.js';
import { installFatalHandlers } from './lib/process-handlers.js';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function main(): Promise<void> {
  const env = loadEnv();
  const app = await buildApp({ env });
  // Milestone 8: an unhandled rejection / uncaught exception is logged as one
  // fatal JSON line and the process exits 1 (see process-handlers.ts).
  installFatalHandlers(app.log);

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return; // a second SIGTERM/SIGINT must not race the first close
    shuttingDown = true;
    app.log.info({ signal, drainDelayMs: env.SHUTDOWN_DRAIN_DELAY_MS }, 'Shutting down');

    const forceExitAfterMs = env.SHUTDOWN_DRAIN_DELAY_MS + env.SHUTDOWN_TIMEOUT_MS;
    const forceExit = setTimeout(() => {
      app.log.error({ timeoutMs: forceExitAfterMs }, 'Graceful shutdown timed out, forcing exit');
      process.exit(1);
    }, forceExitAfterMs);
    forceExit.unref();

    try {
      // 1. /ready -> 503 while we keep serving, so load balancers drain us
      //    before connections start being refused.
      app.beginDraining();
      if (env.SHUTDOWN_DRAIN_DELAY_MS > 0) await sleep(env.SHUTDOWN_DRAIN_DELAY_MS);
      // 2. Stop accepting connections, finish in-flight requests, then run
      //    onClose hooks (Redis flush + quit, Postgres pool.end()).
      await app.close();
      app.log.info('Server closed');
      process.exit(0);
    } catch (err) {
      app.log.error({ err }, 'Error during shutdown');
      process.exit(1);
    }
  };

  process.on('SIGINT', () => void shutdown('SIGINT'));
  process.on('SIGTERM', () => void shutdown('SIGTERM'));

  try {
    await app.listen({ host: env.HOST, port: env.PORT });
    app.log.info({ host: env.HOST, port: env.PORT }, 'PulseQueue listening');
  } catch (err) {
    app.log.error({ err }, 'Failed to start server');
    process.exit(1);
  }
}

main().catch((err: unknown) => {
  // Last-resort: config may have failed before the logger exists.
  console.error(err);
  process.exit(1);
});
