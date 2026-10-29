import { describe, expect, it } from 'vitest';
import { loadDatabaseEnv } from '../src/config/env.js';
import { createPool, resolveQueryTimeoutMs } from '../src/db/pool.js';

const URL = 'postgres://pulsequeue:pulsequeue@127.0.0.1:1/pulsequeue';

/** pg-pool keeps the client config on `options`; nothing here connects. */
function optionsOf(pool: ReturnType<typeof createPool>): Record<string, unknown> {
  return (pool as unknown as { options: Record<string, unknown> }).options;
}

describe('createPool hardening (Milestone 8)', () => {
  it('derives the client-side query timeout from statement_timeout plus slack', () => {
    expect(resolveQueryTimeoutMs(loadDatabaseEnv({ DATABASE_URL: URL }))).toBe(15_000);
    expect(
      resolveQueryTimeoutMs(
        loadDatabaseEnv({ DATABASE_URL: URL, PG_STATEMENT_TIMEOUT_MS: '2000' }),
      ),
    ).toBe(7_000);
    expect(
      resolveQueryTimeoutMs(loadDatabaseEnv({ DATABASE_URL: URL, PG_QUERY_TIMEOUT_MS: '900' })),
    ).toBe(900);
  });

  it('configures every timeout and TCP keepalive on the pool', async () => {
    const pool = createPool(
      loadDatabaseEnv({ DATABASE_URL: URL, PG_IDLE_IN_TRANSACTION_TIMEOUT_MS: '4000' }),
    );
    try {
      expect(optionsOf(pool)).toMatchObject({
        statement_timeout: 10_000,
        query_timeout: 15_000,
        idle_in_transaction_session_timeout: 4_000,
        keepAlive: true,
        application_name: 'pulsequeue',
      });
    } finally {
      await pool.end();
    }
  });

  it('lets the migration runner turn the client-side timeout off', async () => {
    const pool = createPool(loadDatabaseEnv({ DATABASE_URL: URL }), { queryTimeoutMs: false });
    try {
      expect(optionsOf(pool)).not.toHaveProperty('query_timeout');
    } finally {
      await pool.end();
    }
  });
});
