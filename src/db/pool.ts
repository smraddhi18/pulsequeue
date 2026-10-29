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
  /**
   * `false` disables the client-side query timeout. Only for the migration
   * runner, whose statements may legitimately run for a long time.
   */
  queryTimeoutMs?: number | false;
}

/** Slack between the server-side statement_timeout and the client-side backstop. */
export const QUERY_TIMEOUT_SLACK_MS = 5_000;

/** Effective client-side query timeout: explicit, or statement_timeout + slack. */
export function resolveQueryTimeoutMs(env: DatabaseEnv): number {
  return env.PG_QUERY_TIMEOUT_MS ?? env.PG_STATEMENT_TIMEOUT_MS + QUERY_TIMEOUT_SLACK_MS;
}

/**
 * One pg Pool per process. Timeouts are explicit so a sick database turns into
 * fast 503s instead of requests piling up behind a hung connection:
 *
 * - connectionTimeoutMillis: waiting for a free client or a new connection.
 * - statement_timeout (server) and query_timeout (client backstop for a
 *   silently dropped network, where the server-side timer cannot help).
 * - idle_in_transaction_session_timeout: a stalled transaction cannot hold
 *   row locks forever.
 * - TCP keepalive: a dead peer on an idle connection is noticed by the OS.
 */
export function createPool(env: DatabaseEnv, opts: CreatePoolOptions = {}): Pool {
  const queryTimeoutMs =
    opts.queryTimeoutMs === undefined ? resolveQueryTimeoutMs(env) : opts.queryTimeoutMs;
  const pool = new pg.Pool({
    connectionString: env.DATABASE_URL,
    max: env.PG_POOL_MAX,
    idleTimeoutMillis: env.PG_IDLE_TIMEOUT_MS,
    connectionTimeoutMillis: env.PG_CONNECTION_TIMEOUT_MS,
    statement_timeout: env.PG_STATEMENT_TIMEOUT_MS,
    idle_in_transaction_session_timeout: env.PG_IDLE_IN_TRANSACTION_TIMEOUT_MS,
    ...(queryTimeoutMs !== false ? { query_timeout: queryTimeoutMs } : {}),
    keepAlive: true,
    keepAliveInitialDelayMillis: 10_000,
    application_name: 'pulsequeue',
    ...(opts.options !== undefined ? { options: opts.options } : {}),
  });

  // An idle client losing its connection (failover, server restart) emits
  // 'error' on the pool; without a listener Node would crash the process.
  pool.on('error', (err) => {
    opts.logger?.error({ err }, 'Idle Postgres client error');
  });

  // pg-pool only listens for errors on *idle* clients: it removes that
  // listener on checkout. A checked-out client whose connection dies between
  // two queries (a transaction awaiting something, a server restart,
  // pg_terminate_backend, idle_in_transaction_session_timeout) then emits
  // 'error' with no listener, which is an uncaught exception that kills the
  // process (found by the Milestone 8 chaos tests). Every client gets a
  // permanent listener; the client is left unqueryable, so the owner's next
  // query fails normally and release() discards the connection.
  const checkedOut = new WeakSet<pg.PoolClient>();
  pool.on('acquire', (client) => checkedOut.add(client));
  pool.on('release', (_err, client) => checkedOut.delete(client));
  pool.on('connect', (client) => {
    client.on('error', (err) => {
      // Idle clients are reported once, by the pool 'error' listener above.
      if (!checkedOut.has(client)) return;
      opts.logger?.error({ err }, 'Postgres connection lost while checked out');
    });
  });

  return pool;
}

/** Cheap round-trip used by the readiness probe. */
export async function pingDatabase(db: Queryable): Promise<void> {
  await db.query('SELECT 1');
}
