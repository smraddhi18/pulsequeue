import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import pino from 'pino';
import { loadWorkerEnv } from '../../src/config/env.js';
import type { Pool } from '../../src/db/pool.js';
import type { QueueName } from '../../src/domain/job.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import { NonRetryableError } from '../../src/worker/errors.js';
import { Reaper } from '../../src/worker/reaper.js';
import { HandlerRegistry, type JobHandler } from '../../src/worker/registry.js';
import { createWorkerRuntime } from '../../src/worker/runtime.js';
import { Worker, type JobFailedEvent, type WorkerOptions } from '../../src/worker/worker.js';
import {
  createSchemaPool,
  createTestDatabase,
  describeWithDb,
  type TestDatabase,
} from './helpers.js';
import { DEFAULT_TENANT_ID } from '../../src/domain/tenant.js';

const logger = pino({ level: 'silent' });
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  { timeoutMs = 10_000, intervalMs = 20 } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await sleep(intervalMs);
  }
}

describeWithDb('worker (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let jobs: JobService;
  const cleanups: (() => Promise<unknown>)[] = [];

  beforeAll(async () => {
    db = await createTestDatabase();
    jobs = new JobService(new QueueRepository(db.pool), new JobRepository(db.pool), {
      maxPayloadBytes: 10_000,
    });
  });

  afterAll(async () => {
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
  });

  afterEach(async () => {
    // Stop workers before pools: LIFO.
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  });

  function startWorker(
    handlers: Record<string, JobHandler>,
    overrides: Partial<WorkerOptions> & { pool?: Pool } = {},
  ): Worker {
    const registry = new HandlerRegistry();
    for (const [q, h] of Object.entries(handlers)) registry.register(q, h);
    const { pool, ...rest } = overrides;
    const worker = new Worker({
      store: new LeaseRepository(pool ?? db.pool),
      registry,
      logger,
      workerId: `w-${Math.random().toString(16).slice(2, 8)}`,
      queues: registry.queues(),
      concurrency: 4,
      pollIntervalMs: 20,
      leaseDurationMs: 5_000,
      heartbeatIntervalMs: 500,
      jobTimeoutMs: 5_000,
      backoff: { baseMs: 10, maxMs: 40 },
      ...rest,
    });
    worker.start();
    cleanups.push(() => worker.stop({ timeoutMs: 2_000 }));
    return worker;
  }

  async function enqueue(
    queue: string,
    payload: unknown = {},
    maxAttempts?: number,
  ): Promise<string> {
    const { job } = await jobs.enqueue({
      tenantId: DEFAULT_TENANT_ID,
      queue: queue as QueueName,
      payload,
      ...(maxAttempts !== undefined ? { maxAttempts } : {}),
    });
    return job.id;
  }

  async function job(id: string) {
    const { rows } = await db.pool.query<{
      status: string;
      attempts: number;
      last_error: string | null;
      finished_at: Date | null;
      locked_by: string | null;
    }>('SELECT status, attempts, last_error, finished_at, locked_by FROM jobs WHERE id = $1', [id]);
    return rows[0]!;
  }

  async function attemptHistory(id: string) {
    const { rows } = await db.pool.query<{
      attempt_number: number;
      status: string;
      error: string | null;
      worker_id: string;
    }>(
      'SELECT attempt_number, status, error, worker_id FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number',
      [id],
    );
    return rows;
  }

  async function statusIs(id: string, status: string): Promise<boolean> {
    return (await job(id)).status === status;
  }

  it('many workers in parallel: every job runs exactly once, never concurrently with itself', async () => {
    const N = 400;
    const WORKERS = 8;
    await db.pool.query(
      `INSERT INTO queues (name) VALUES ('load');
       INSERT INTO jobs (queue_id, payload, priority)
       SELECT q.id, jsonb_build_object('n', g), (g % 5)::smallint
       FROM queues q, generate_series(1, ${N}) g WHERE q.name = 'load'`,
    );

    const runs = new Map<string, number>();
    const inProgress = new Set<string>();
    let overlaps = 0;
    const handler: JobHandler = async (ctx) => {
      if (inProgress.has(ctx.job.id)) overlaps++;
      inProgress.add(ctx.job.id);
      runs.set(ctx.job.id, (runs.get(ctx.job.id) ?? 0) + 1);
      await sleep(Math.floor(Math.random() * 4));
      inProgress.delete(ctx.job.id);
    };

    const workers: Worker[] = [];
    for (let i = 0; i < WORKERS; i++) {
      // One pool per worker, like separate processes.
      const pool = createSchemaPool(db, 4);
      cleanups.push(() => pool.end());
      workers.push(
        startWorker(
          { load: handler },
          { pool, workerId: `load-${i}`, concurrency: 5, pollIntervalMs: 10 },
        ),
      );
    }

    await waitFor(async () => {
      const { rows } = await db.pool.query<{ n: string }>(
        "SELECT count(*) AS n FROM jobs WHERE status = 'succeeded'",
      );
      return Number(rows[0]?.n) === N;
    });

    expect(runs.size).toBe(N);
    expect([...runs.values()].every((n) => n === 1)).toBe(true);
    expect(overlaps).toBe(0);

    const { rows } = await db.pool.query<{
      attempts_rows: string;
      bad_attempts: string;
      workers_used: string;
      max_attempts_per_job: number;
    }>(
      `SELECT count(*) AS attempts_rows,
              count(*) FILTER (WHERE status <> 'succeeded' OR attempt_number <> 1) AS bad_attempts,
              count(DISTINCT worker_id) AS workers_used,
              (SELECT max(attempts) FROM jobs) AS max_attempts_per_job
       FROM job_attempts`,
    );
    expect(Number(rows[0]?.attempts_rows)).toBe(N);
    expect(Number(rows[0]?.bad_attempts)).toBe(0);
    expect(rows[0]?.max_attempts_per_job).toBe(1);
    expect(Number(rows[0]?.workers_used)).toBeGreaterThan(1);
  });

  it('retries with backoff, then succeeds; history records every attempt', async () => {
    const id = await enqueue('flaky', {}, 5);
    startWorker({
      flaky: async (ctx) => {
        if (ctx.job.attempt < 3) throw new Error(`transient ${ctx.job.attempt}`);
      },
    });
    await waitFor(() => statusIs(id, 'succeeded'));
    expect(await job(id)).toMatchObject({ attempts: 3, last_error: null, locked_by: null });
    expect((await attemptHistory(id)).map((a) => [a.attempt_number, a.status, a.error])).toEqual([
      [1, 'failed', 'transient 1'],
      [2, 'failed', 'transient 2'],
      [3, 'succeeded', null],
    ]);
  });

  it('exhausts max attempts into the DLQ and fires the dead-letter hook once', async () => {
    const id = await enqueue('broken', {}, 3);
    const events: JobFailedEvent[] = [];
    startWorker(
      {
        broken: async () => {
          throw new Error('always broken');
        },
      },
      { onJobFailed: (e) => void events.push(e) },
    );
    await waitFor(() => statusIs(id, 'dead'));
    const row = await job(id);
    expect(row).toMatchObject({ attempts: 3, last_error: 'always broken' });
    expect(row.finished_at).not.toBeNull();
    expect((await attemptHistory(id)).map((a) => a.status)).toEqual(['failed', 'failed', 'failed']);
    await sleep(100);
    expect(events).toEqual([
      expect.objectContaining({ jobId: id, attempt: 3, reason: 'attempts_exhausted' }),
    ]);
  });

  it('dead-letters immediately on NonRetryableError', async () => {
    const id = await enqueue('strict', {}, 5);
    startWorker({
      strict: async () => {
        throw new NonRetryableError('invalid payload');
      },
    });
    await waitFor(() => statusIs(id, 'dead'));
    expect(await job(id)).toMatchObject({
      attempts: 1,
      last_error: 'NonRetryableError: invalid payload',
    });
  });

  it('records a timed-out attempt and retries it', async () => {
    const id = await enqueue('slow', {}, 2);
    startWorker(
      {
        slow: async (ctx) => {
          if (ctx.job.attempt === 1) await sleep(1_000);
        },
      },
      { jobTimeoutMs: 100 },
    );
    await waitFor(() => statusIs(id, 'succeeded'));
    expect((await attemptHistory(id)).map((a) => a.status)).toEqual(['timed_out', 'succeeded']);
  });

  it("reclaims a crashed worker's expired lease; another worker finishes the job", async () => {
    const id = await enqueue('crashy', {}, 3);
    // "Crashed" worker: leases with a short lease and never heartbeats or finishes.
    const repo = new LeaseRepository(db.pool);
    const [orphan] = await repo.lease({
      queues: ['crashy'],
      workerId: 'crashed',
      limit: 1,
      leaseMs: 150,
    });
    expect(orphan?.jobId).toBe(id);

    const reaper = new Reaper({ store: repo, logger, intervalMs: 50 });
    reaper.start();
    cleanups.push(() => reaper.stop());
    startWorker({ crashy: async () => undefined }, { workerId: 'rescuer' });

    await waitFor(() => statusIs(id, 'succeeded'));
    expect(await job(id)).toMatchObject({ attempts: 2 });
    expect((await attemptHistory(id)).map((a) => [a.status, a.worker_id])).toEqual([
      ['timed_out', 'crashed'],
      ['succeeded', 'rescuer'],
    ]);
    // The crashed worker comes back and tries to report: fenced out.
    expect(await repo.complete(orphan!)).toBe(false);
    expect((await job(id)).status).toBe('succeeded');
  });

  it('heartbeats keep a long job leased past its lease duration (no reclaim, one attempt)', async () => {
    const id = await enqueue('long');
    const repo = new LeaseRepository(db.pool);
    const reaper = new Reaper({ store: repo, logger, intervalMs: 25 });
    reaper.start();
    cleanups.push(() => reaper.stop());
    startWorker({ long: () => sleep(900) }, { leaseDurationMs: 300, heartbeatIntervalMs: 60 });
    await waitFor(() => statusIs(id, 'succeeded'));
    expect((await attemptHistory(id)).map((a) => a.status)).toEqual(['succeeded']);
  });

  it('graceful stop drains the in-flight job and leases nothing new', async () => {
    const first = await enqueue('drain');
    let started = false;
    const worker = startWorker(
      {
        drain: async () => {
          started = true;
          await sleep(300);
        },
      },
      { concurrency: 1 },
    );
    await waitFor(() => started);
    const second = await enqueue('drain');

    const result = await worker.stop({ timeoutMs: 5_000 });
    expect(result).toEqual({ drained: true, abandoned: 0 });
    expect((await job(first)).status).toBe('succeeded');
    await sleep(100);
    expect(await job(second)).toMatchObject({ status: 'queued', attempts: 0 });
  });

  it('drain timeout releases unfinished leases so the next worker picks them up at once', async () => {
    const id = await enqueue('stuck', {}, 1);
    let started = false;
    const worker = startWorker({
      stuck: async () => {
        started = true;
        await sleep(5_000); // ignores ctx.signal on purpose
      },
    });
    await waitFor(() => started);

    const t0 = Date.now();
    const result = await worker.stop({ timeoutMs: 100 });
    expect(Date.now() - t0).toBeLessThan(2_000);
    expect(result).toEqual({ drained: false, abandoned: 1 });
    expect(await job(id)).toMatchObject({ status: 'queued', attempts: 0, locked_by: null });

    // max_attempts = 1, yet it still runs: the released attempt was refunded.
    startWorker({ stuck: async () => undefined });
    await waitFor(() => statusIs(id, 'succeeded'));
    expect((await attemptHistory(id)).map((a) => [a.attempt_number, a.status])).toEqual([
      [1, 'released'],
      [2, 'succeeded'],
    ]);
  });

  it('runtime: env-driven composition with queue filtering; stop() closes its own pool', async () => {
    const env = loadWorkerEnv({
      DATABASE_URL: db.url,
      LOG_LEVEL: 'silent',
      WORKER_QUEUES: 'alpha',
      WORKER_POLL_INTERVAL_MS: '20',
      WORKER_CONCURRENCY: '2',
      WORKER_SHUTDOWN_TIMEOUT_MS: '1000',
      PG_POOL_MAX: '3',
    });
    const registry = new HandlerRegistry()
      .register('alpha', async () => undefined)
      .register('beta', async () => undefined);
    const pool = createSchemaPool(db, 3);
    const runtime = createWorkerRuntime(env, { registry, logger, pool });
    cleanups.push(async () => {
      await runtime.stop();
      await pool.end();
    });
    expect(runtime.queues).toEqual(['alpha']);
    const a = await enqueue('alpha');
    const b = await enqueue('beta');
    runtime.start();
    await waitFor(() => statusIs(a, 'succeeded'));
    expect((await job(b)).status).toBe('queued');
    await expect(runtime.stop()).resolves.toEqual({ drained: true, abandoned: 0 });

    expect(() =>
      createWorkerRuntime(loadWorkerEnv({ DATABASE_URL: db.url, WORKER_QUEUES: 'gamma' }), {
        registry,
        logger,
        pool,
      }),
    ).toThrow(/gamma/);
  });
});
