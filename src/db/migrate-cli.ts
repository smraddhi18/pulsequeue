import { loadDatabaseEnv } from '../config/env.js';
import { createLogger } from '../lib/logger.js';
import { runMigrations } from './migrate.js';
import { createPool } from './pool.js';

/**
 * `npm run migrate` (compiled) / `npm run migrate:dev` (tsx).
 * Runs as a one-shot step before the API starts (docker-compose `migrate`
 * service, CI, or a deploy job); exits non-zero on failure.
 */
async function main(): Promise<void> {
  const env = loadDatabaseEnv();
  const nodeEnv = process.env.NODE_ENV;
  const logger = createLogger({
    NODE_ENV: nodeEnv === 'production' || nodeEnv === 'test' ? nodeEnv : 'development',
    LOG_LEVEL: 'info',
  }).child({ component: 'migrate' });
  const pool = createPool(env, { logger });

  try {
    const result = await runMigrations(pool, { logger });
    logger.info(
      { applied: result.applied, alreadyApplied: result.alreadyApplied },
      result.applied.length > 0 ? 'Migrations complete' : 'Schema up to date',
    );
  } finally {
    await pool.end();
  }
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
