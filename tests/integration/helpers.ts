import { randomBytes } from 'node:crypto';
import pg from 'pg';
import { describe } from 'vitest';
import { loadDatabaseEnv, loadEnv, type Env } from '../../src/config/env.js';
import { runMigrations, type RunMigrationsOptions } from '../../src/db/migrate.js';
import { createPool, type Pool } from '../../src/db/pool.js';

/**
 * Integration tests run against a real Postgres when TEST_DATABASE_URL (or
 * DATABASE_URL) is set and are skipped otherwise, so `npm test` stays green on
 * a laptop without a database.
 *
 * Isolation: every suite gets its own freshly migrated schema (search_path is
 * set per connection), so suites can run in parallel and never touch data in
 * `public`, even if the URL points at a development database.
 */
export const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

export const describeWithDb: ReturnType<typeof describe.skipIf> =
  describe.skipIf(!TEST_DATABASE_URL);

export interface TestDatabase {
  url: string;
  schema: string;
  pool: Pool;
  /** Drop the schema and close pools. */
  destroy(): Promise<void>;
}

export async function createTestDatabase(
  options: { migrate?: boolean; migrations?: RunMigrationsOptions } = {},
): Promise<TestDatabase> {
  const url = requireUrl();
  const schema = `pq_test_${randomBytes(6).toString('hex')}`;

  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  try {
    await admin.query(`CREATE SCHEMA ${schema}`);
  } finally {
    await admin.end();
  }

  const pool = createPool(loadDatabaseEnv({ DATABASE_URL: url, PG_POOL_MAX: '10' }), {
    options: `-c search_path=${schema}`,
  });

  if (options.migrate !== false) {
    await runMigrations(pool, options.migrations);
  }

  return {
    url,
    schema,
    pool,
    async destroy() {
      await pool.end();
      const cleanup = new pg.Client({ connectionString: url });
      await cleanup.connect();
      try {
        await cleanup.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      } finally {
        await cleanup.end();
      }
    },
  };
}

/**
 * An extra pool on the same isolated schema, standing in for another process
 * (e.g. one per simulated worker). The caller must end() it.
 */
export function createSchemaPool(db: TestDatabase, max = 4): Pool {
  return createPool(loadDatabaseEnv({ DATABASE_URL: db.url, PG_POOL_MAX: String(max) }), {
    options: `-c search_path=${db.schema}`,
  });
}

export function testEnv(overrides: Record<string, string> = {}): Env {
  return loadEnv({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: requireUrl(),
    REDIS_URL: 'redis://localhost:6379',
    ...overrides,
  });
}

function requireUrl(): string {
  if (!TEST_DATABASE_URL) {
    throw new Error('TEST_DATABASE_URL or DATABASE_URL must be set for integration tests');
  }
  return TEST_DATABASE_URL;
}
