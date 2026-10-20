import pg from 'pg';
import type { DatabaseEnv } from '../config/env.js';
import type { Logger } from '../lib/logger.js';

export type Pool = pg.Pool;
export type PoolClient = pg.PoolClient;
export type QueryResultRow = pg.QueryResultRow;

/**
 * Anything that can run a parameterized query: the pool itself or a checked-out
 * client inside a transaction. Repositories accept this so callers decide the
 * transaction boundary.
 */
export interface Queryable {
  query<R extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: unknown[],
  ): Promise<pg.QueryResult<R>>;
}

export interface CreatePoolOptions {
  /** Extra libpq startup options, e.g. `-c search_path=...` (tests use a per-suite schema). */
  options?: string;
  logger?: Pick<Logger, 'error'>;
}

/**
 * One pg Pool per process. Timeouts are explicit so a sick database turns into
 * fast 503s instead of requests piling up behind a hung connection.
 */
export function createPool(env: DatabaseEnv, opts: CreatePoolOptions = {}): Pool {
  const pool = new pg.Pool({
    connectionString: env.DATABASE_URL,
    max: env.PG_POOL_MAX,
    idleTimeoutMillis: env.PG_IDLE_TIMEOUT_MS,
    connectionTimeoutMillis: env.PG_CONNECTION_TIMEOUT_MS,
    statement_timeout: env.PG_STATEMENT_TIMEOUT_MS,
    application_name: 'pulsequeue',
    ...(opts.options !== undefined ? { options: opts.options } : {}),
  });

  // An idle client losing its connection (failover, server restart) emits
  // 'error' on the pool; without a listener Node would crash the process.
  pool.on('error', (err) => {
    opts.logger?.error({ err }, 'Idle Postgres client error');
  });

  return pool;
}

/** Cheap round-trip used by the readiness probe. */
export async function pingDatabase(db: Queryable): Promise<void> {
  await db.query('SELECT 1');
}
