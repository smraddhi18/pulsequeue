import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, expect, it } from 'vitest';
import type { QueueName } from '../../src/domain/job.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import { buildApp } from '../../src/app.js';
import {
  createAdminRedis,
  createTestDatabase,
  deleteRedisPrefix,
  describeWithDb,
  describeWithRedis,
  redisTestEnv,
  requireRedisUrl,
  uniqueRedisPrefix,
  type TestDatabase,
} from './helpers.js';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * End-to-end: the real `src/worker.ts` entry as a child process. Proves the
 * signal wiring, not just the Worker class: SIGTERM mid-job lets the job
 * finish, then the process exits 0.
 */
describeWithDb('worker process (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let child: ChildProcess | undefined;

  beforeAll(async () => {
    db = await createTestDatabase();
  });

  afterAll(async () => {
    child?.kill('SIGKILL');
    await db?.destroy();
  });

  it('drains the running job on SIGTERM and exits 0', async () => {
    const jobs = new JobService(new QueueRepository(db.pool), new JobRepository(db.pool), {
      maxPayloadBytes: 10_000,
    });
    const { job } = await jobs.enqueue({
      queue: 'demo' as QueueName,
      payload: { durationMs: 800 },
    });

    // Point the child at this suite's schema through libpq startup options.
    const url = new URL(db.url);
    url.searchParams.set('options', `-c search_path=${db.schema}`);
    const output: string[] = [];
    child = spawn(process.execPath, ['--import', 'tsx', 'src/worker.ts'], {
      cwd: ROOT,
      env: {
        ...process.env,
        NODE_ENV: 'test',
        LOG_LEVEL: 'info',
        DATABASE_URL: url.toString(),
        WORKER_QUEUES: 'demo',
        WORKER_POLL_INTERVAL_MS: '50',
        WORKER_SHUTDOWN_TIMEOUT_MS: '5000',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout?.on('data', (d: Buffer) => output.push(d.toString()));
    child.stderr?.on('data', (d: Buffer) => output.push(d.toString()));
    const exited = new Promise<number | null>((resolve) =>
      child?.on('exit', (code) => resolve(code)),
    );

    const deadline = Date.now() + 15_000;
    for (;;) {
      const { rows } = await db.pool.query<{ status: string }>(
        'SELECT status FROM jobs WHERE id = $1',
        [job.id],
      );
      if (rows[0]?.status === 'running') break;
      if (Date.now() > deadline) throw new Error(`job never started:\n${output.join('')}`);
      await sleep(25);
    }

    child.kill('SIGTERM');
    const code = await exited;
    const logs = output.join('');
    expect(code, logs).toBe(0);
    expect(logs).toContain('Stopped leasing, draining jobs');
    expect(logs).toContain('Worker shut down cleanly');

    const { rows } = await db.pool.query<{ status: string; attempts: number }>(
      'SELECT status, attempts FROM jobs WHERE id = $1',
      [job.id],
    );
    expect(rows[0]).toEqual({ status: 'succeeded', attempts: 1 });
  });
});

/**
 * Milestone 5 end-to-end: the real worker process subscribes to wake-ups,
 * an enqueue through the real API wakes it long before its fallback poll,
 * and SIGTERM still exits 0 with Redis connected.
 */
describeWithRedis('worker process with Redis wake-ups', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let child: ChildProcess | undefined;
  const prefix = uniqueRedisPrefix();

  beforeAll(async () => {
    db = await createTestDatabase();
  });

  afterAll(async () => {
    child?.kill('SIGKILL');
    const admin = createAdminRedis();
    await deleteRedisPrefix(admin, prefix);
    await admin.quit();
    await db?.destroy();
  });

  it('is woken by an API enqueue, then shuts down cleanly', async () => {
    const app = await buildApp({ env: redisTestEnv(prefix), logger: false, pool: db.pool });
    try {
      await app.ready();
      const url = new URL(db.url);
      url.searchParams.set('options', `-c search_path=${db.schema}`);
      const output: string[] = [];
      child = spawn(process.execPath, ['--import', 'tsx', 'src/worker.ts'], {
        cwd: ROOT,
        env: {
          ...process.env,
          NODE_ENV: 'test',
          LOG_LEVEL: 'info',
          DATABASE_URL: url.toString(),
          REDIS_URL: requireRedisUrl(),
          REDIS_KEY_PREFIX: prefix,
          WORKER_QUEUES: 'demo',
          WORKER_POLL_INTERVAL_MS: '5000',
          WORKER_WAKEUP_FALLBACK_POLL_MS: '60000',
          WORKER_SHUTDOWN_TIMEOUT_MS: '5000',
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      child.stdout?.on('data', (d: Buffer) => output.push(d.toString()));
      child.stderr?.on('data', (d: Buffer) => output.push(d.toString()));
      const exited = new Promise<number | null>((resolve) =>
        child?.on('exit', (code) => resolve(code)),
      );

      const deadline = Date.now() + 15_000;
      while (!output.join('').includes('"event":"wakeup.subscribed"')) {
        if (Date.now() > deadline) throw new Error(`never subscribed:\n${output.join('')}`);
        await sleep(25);
      }
      await sleep(300); // let the catch-up poll after subscribing return

      const enqueuedAt = Date.now();
      const res = await app.inject({
        method: 'POST',
        url: '/queues/demo/jobs',
        payload: { payload: { durationMs: 1 } },
      });
      expect(res.statusCode).toBe(201);
      const id = res.json<{ id: string }>().id;
      for (;;) {
        const { rows } = await db.pool.query<{ status: string }>(
          'SELECT status FROM jobs WHERE id = $1',
          [id],
        );
        if (rows[0]?.status === 'succeeded') break;
        if (Date.now() - enqueuedAt > 4_000) {
          throw new Error(`job not picked up before the poll interval:\n${output.join('')}`);
        }
        await sleep(20);
      }
      expect(Date.now() - enqueuedAt).toBeLessThan(2_000);

      child.kill('SIGTERM');
      const code = await exited;
      const logs = output.join('');
      expect(code, logs).toBe(0);
      expect(logs).toContain('"event":"wakeup.mode_changed"');
      expect(logs).toContain('Worker shut down cleanly');
    } finally {
      await app.close();
    }
  });
});
