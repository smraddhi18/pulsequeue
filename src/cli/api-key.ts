import { loadDatabaseEnv } from '../config/env.js';
import { createPool } from '../db/pool.js';
import { createLogger } from '../lib/logger.js';
import { runApiKeyCli } from './api-key-cli.js';

/**
 * `npm run api-key -- <command> ...` (compiled) / `npm run api-key:dev -- ...` (tsx).
 * In Docker: `docker compose run --rm api node dist/cli/api-key.js create ...`.
 */
async function main(): Promise<number> {
  const env = loadDatabaseEnv();
  const logger = createLogger({ NODE_ENV: 'production', LOG_LEVEL: 'warn' }).child({
    component: 'api-key-cli',
  });
  const pool = createPool(env, { logger });
  try {
    return await runApiKeyCli(process.argv.slice(2), pool, process);
  } finally {
    await pool.end();
  }
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (err: unknown) => {
    console.error(err);
    process.exitCode = 1;
  },
);
