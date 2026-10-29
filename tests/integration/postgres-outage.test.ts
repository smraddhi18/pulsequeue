import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';
import {
  createTestDatabase,
  describeWithDb,
  PgOutageProxy,
  testEnv,
  waitUntil,
  type TestDatabase,
} from './helpers.js';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Milestone 8 chaos: Postgres goes away under a running API and a running
 * worker process, then comes back. A TCP proxy sits between them and the
 * database, so the outage is scoped to this suite (parallel suites keep
 * their connections):
 *
 * - cut(): Postgres crashed / restarted (connections reset, new ones refused).
 * - freeze(): silent network partition (nothing answers, nothing is reset).
 *
 * Expected: the API answers fast 503s (never hangs), /ready reports
 * not_ready, nothing crashes, and once Postgres is back the API is ready
 * again and the worker resumes on its own, with every job succeeding once.
 */
describeWithDb('Postgres outage (API + worker process)', { timeout: 90_000 }, () => {
  let db: TestDatabase;
  let proxy: PgOutageProxy;
  let app: FastifyInstance;
  let inject: Inject;
  let worker: ChildProcess;
  const workerOutput: string[] = [];

  const enqueue = (durationMs: number) =>
    inject({
      method: 'POST',
      url: '/queues/demo/jobs',
      payload: { payload: { durationMs }, maxAttempts: 10 },
    });

  async function allSucceeded(ids: string[]): Promise<boolean> {
    const { rows } = await db.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM jobs WHERE id = ANY($1::uuid[]) AND status = 'succeeded'`,
      [ids],
    );
    return rows[0]?.n === ids.length;
  }

  async function expectExactlyOneSuccess(ids: string[]): Promise<void> {
    const { rows } = await db.pool.query<{ job_id: string; n: number }>(
      `SELECT job_id, count(*) FILTER (WHERE status = 'succeeded')::int AS n
       FROM job_attempts WHERE job_id = ANY($1::uuid[]) GROUP BY job_id`,
      [ids],
    );
    expect(rows).toHaveLength(ids.length);
    for (const r of rows) expect(r.n, `job ${r.job_id}`).toBe(1);
  }

  beforeAll(async () => {
    db = await createTestDatabase();
    proxy = await PgOutageProxy.start();
    const key = await createTestApiKey(db.pool);

    app = await buildApp({
      env: testEnv({
        DATABASE_URL: proxy.urlFor(db),
        PG_POOL_MAX: '4',
        PG_CONNECTION_TIMEOUT_MS: '1000',
        PG_STATEMENT_TIMEOUT_MS: '1000',
        PG_QUERY_TIMEOUT_MS: '1500',
        READINESS_TIMEOUT_MS: '1000',
      }),
      logger: false,
    });
    await app.ready();
    inject = authedInject(app, key.token);

    const { REDIS_URL: _unused, ...inherited } = process.env;
    worker = spawn(process.execPath, ['--import', 'tsx', 'src/worker.ts'], {
      cwd: ROOT,
      env: {
        ...inherited,
        NODE_ENV: 'test',
        LOG_LEVEL: 'info',
        DATABASE_URL: proxy.urlFor(db),
        WORKER_ID: 'outage-worker',
        WORKER_QUEUES: 'demo',
        WORKER_POLL_INTERVAL_MS: '50',
        WORKER_LEASE_DURATION_MS: '2000',
        WORKER_HEARTBEAT_INTERVAL_MS: '500',
        WORKER_REAPER_INTERVAL_MS: '300',
        WORKER_SHUTDOWN_TIMEOUT_MS: '3000',
        PG_CONNECTION_TIMEOUT_MS: '1000',
        PG_STATEMENT_TIMEOUT_MS: '1000',
        PG_QUERY_TIMEOUT_MS: '1500',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    worker.stdout?.on('data', (d: Buffer) => workerOutput.push(d.toString()));
    worker.stderr?.on('data', (d: Buffer) => workerOutput.push(d.toString()));
  });

  afterAll(async () => {
    worker?.kill('SIGKILL');
    await app?.close();
    await proxy?.stop();
    await db?.destroy();
  });

  it('serves 503s during a Postgres crash and recovers on its own afterwards', async () => {
    const before: string[] = [];
    for (let i = 0; i < 8; i++) {
      const res = await enqueue(300);
      expect(res.statusCode).toBe(201);
      before.push(res.json<{ id: string }>().id);
    }
    // Let the worker get some of them in flight, then crash Postgres.
    await waitUntil(
      async () => {
        const { rows } = await db.pool.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM jobs WHERE status = 'running'`,
        );
        return (rows[0]?.n ?? 0) > 0;
      },
      { timeoutMs: 20_000, intervalMs: 20 },
    );
    await proxy.cut();

    const ready = await app.inject({ method: 'GET', url: '/ready' });
    expect(ready.statusCode).toBe(503);
    expect(ready.json()).toMatchObject({ status: 'not_ready', checks: { postgres: 'fail' } });

    const started = Date.now();
    const during = await enqueue(300);
    expect(during.statusCode).toBe(503);
    expect(during.json()).toMatchObject({ error: { code: expect.any(String) } });
    expect(Date.now() - started).toBeLessThan(2_000); // refused connections fail fast

    // Hold the outage long enough for heartbeats to fail and leases to lapse.
    await sleep(2_500);
    expect(worker.exitCode, workerOutput.join('')).toBeNull();

    await proxy.restore();
    await waitUntil(
      async () => (await app.inject({ method: 'GET', url: '/ready' })).statusCode === 200,
      { timeoutMs: 10_000, intervalMs: 100 },
    );

    const after: string[] = [];
    for (let i = 0; i < 4; i++) {
      const res = await enqueue(100);
      expect(res.statusCode).toBe(201);
      after.push(res.json<{ id: string }>().id);
    }

    const ids = [...before, ...after];
    await waitUntil(() => allSucceeded(ids), { timeoutMs: 45_000, intervalMs: 100 }).catch(() => {
      throw new Error(`jobs did not all succeed after recovery:\n${workerOutput.join('')}`);
    });
    await expectExactlyOneSuccess(ids);

    const logs = workerOutput.join('');
    expect(logs).toContain('Lease query failed'); // it noticed...
    expect(worker.exitCode).toBeNull(); // ...and survived
  });

  it('answers within the query timeout during a silent partition, then recovers', async () => {
    proxy.freeze();
    try {
      const started = Date.now();
      const [ready, during] = await Promise.all([
        app.inject({ method: 'GET', url: '/ready' }),
        enqueue(100),
      ]);
      const elapsed = Date.now() - started;
      expect(ready.statusCode).toBe(503);
      expect(during.statusCode).toBe(503);
      // Bounded by PG_QUERY_TIMEOUT_MS / PG_CONNECTION_TIMEOUT_MS, not by TCP.
      expect(elapsed).toBeLessThan(5_000);
      expect(worker.exitCode).toBeNull();
    } finally {
      await proxy.restore();
    }

    await waitUntil(
      async () => (await app.inject({ method: 'GET', url: '/ready' })).statusCode === 200,
      { timeoutMs: 15_000, intervalMs: 100 },
    );
    const res = await enqueue(50);
    expect(res.statusCode).toBe(201);
    const id = res.json<{ id: string }>().id;
    await waitUntil(() => allSucceeded([id]), { timeoutMs: 45_000, intervalMs: 100 }).catch(() => {
      throw new Error(`job did not succeed after the partition:\n${workerOutput.join('')}`);
    });
    await expectExactlyOneSuccess([id]);
  });
});
