import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { loadDatabaseEnv } from '../../src/config/env.js';
import { mapDatabaseError } from '../../src/db/errors.js';
import { createPool, type Pool } from '../../src/db/pool.js';
import { ServiceUnavailableError } from '../../src/domain/errors.js';
import {
  createTestDatabase,
  describeWithDb,
  PgOutageProxy,
  waitUntil,
  type TestDatabase,
} from './helpers.js';

/**
 * Milestone 8: what happens to a pool when the connection under it dies or
 * the network goes silent. These run against real Postgres; the dangerous
 * cases used to crash the whole process.
 */
describeWithDb('Postgres pool resilience', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let proxy: PgOutageProxy;
  const pools: Pool[] = [];

  function pool(overrides: Record<string, string> = {}, queryTimeoutMs?: false) {
    const logger = { error: vi.fn() };
    const p = createPool(
      loadDatabaseEnv({ DATABASE_URL: db.url, PG_POOL_MAX: '2', ...overrides }),
      {
        options: `-c search_path=${db.schema}`,
        logger,
        ...(queryTimeoutMs === false ? { queryTimeoutMs } : {}),
      },
    );
    pools.push(p);
    return { pool: p, logger };
  }

  beforeAll(async () => {
    db = await createTestDatabase();
    proxy = await PgOutageProxy.start();
  });

  afterAll(async () => {
    await proxy?.stop();
    await Promise.all(pools.map((p) => p.end().catch(() => undefined)));
    await db?.destroy();
  });

  it('survives the backend of a checked-out client being terminated', async () => {
    // Before Milestone 8 this was an uncaught 'error' event: pg-pool removes
    // its error listener on checkout, so the process died.
    const { pool: p, logger } = pool();
    const client = await p.connect();
    const { rows } = await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
    await db.pool.query('SELECT pg_terminate_backend($1)', [rows[0]?.pid]);

    await waitUntil(() => logger.error.mock.calls.length > 0, { timeoutMs: 5_000 });
    expect(logger.error.mock.calls[0]?.[1]).toBe('Postgres connection lost while checked out');

    // The owner's next query fails normally and maps to a 503...
    const err = await client.query('SELECT 1').catch((e: unknown) => e);
    expect(mapDatabaseError(err)).toBeInstanceOf(ServiceUnavailableError);
    // ...and release() discards the broken connection: the pool keeps working.
    client.release();
    await expect(p.query('SELECT 1 AS ok')).resolves.toMatchObject({ rows: [{ ok: 1 }] });
  });

  it('ends a stalled transaction with idle_in_transaction_session_timeout', async () => {
    const { pool: p, logger } = pool({ PG_IDLE_IN_TRANSACTION_TIMEOUT_MS: '300' });
    const client = await p.connect();
    await client.query('BEGIN');
    await client.query('SELECT 1');
    // The process "stalls" inside the transaction; the server ends the session.
    await waitUntil(() => logger.error.mock.calls.length > 0, { timeoutMs: 5_000 });
    const err = await client.query('SELECT 1').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(Error);
    client.release();
    await expect(p.query('SELECT 1 AS ok')).resolves.toMatchObject({ rows: [{ ok: 1 }] });
  });

  it('fails a query on a silently dropped network with the client-side timeout', async () => {
    const { pool: p } = pool({
      DATABASE_URL: proxy.urlFor(db),
      PG_STATEMENT_TIMEOUT_MS: '200',
      PG_QUERY_TIMEOUT_MS: '600',
    });
    await p.query('SELECT 1');

    proxy.freeze();
    try {
      const started = Date.now();
      const err = await p.query('SELECT 1').catch((e: unknown) => e);
      const elapsed = Date.now() - started;
      // statement_timeout (server side) cannot fire: the server never sees the query.
      expect((err as Error).message).toBe('Query read timeout');
      expect(mapDatabaseError(err)).toBeInstanceOf(ServiceUnavailableError);
      expect(elapsed).toBeGreaterThanOrEqual(550);
      expect(elapsed).toBeLessThan(3_000);
    } finally {
      await proxy.restore();
    }
    // pool.query released the timed-out client with its error: a fresh one works.
    await expect(p.query('SELECT 2 AS ok')).resolves.toMatchObject({ rows: [{ ok: 2 }] });
  });

  it('without the client-side timeout the same query just hangs (why it exists)', async () => {
    const { pool: p } = pool(
      { DATABASE_URL: proxy.urlFor(db), PG_STATEMENT_TIMEOUT_MS: '200' },
      false,
    );
    await p.query('SELECT 1');
    proxy.freeze();
    try {
      const outcome = await Promise.race([
        p.query('SELECT 1').then(
          () => 'resolved',
          () => 'rejected',
        ),
        new Promise((r) => setTimeout(() => r('still waiting'), 1_500)),
      ]);
      expect(outcome).toBe('still waiting');
    } finally {
      // Frozen bytes are delivered on restore, so the hung query completes.
      await proxy.restore();
    }
  });
});
