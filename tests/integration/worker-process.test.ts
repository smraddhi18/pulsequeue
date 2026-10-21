import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, expect, it } from 'vitest';
import type { QueueName } from '../../src/domain/job.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

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
