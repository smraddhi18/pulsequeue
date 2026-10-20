import { loadEnv } from './config/env.js';
import { buildApp } from './app.js';

/** Upper bound for draining in-flight requests and closing the pool. */
const SHUTDOWN_TIMEOUT_MS = 10_000;

async function main(): Promise<void> {
  const env = loadEnv();
  const app = await buildApp({ env });

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return; // a second SIGTERM/SIGINT must not race the first close
    shuttingDown = true;
    app.log.info({ signal }, 'Shutting down');

    const forceExit = setTimeout(() => {
      app.log.error(
        { timeoutMs: SHUTDOWN_TIMEOUT_MS },
        'Graceful shutdown timed out, forcing exit',
      );
      process.exit(1);
    }, SHUTDOWN_TIMEOUT_MS);
    forceExit.unref();

    try {
      // Stops accepting connections, drains in-flight requests, then runs
      // onClose hooks (Postgres pool.end()).
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
