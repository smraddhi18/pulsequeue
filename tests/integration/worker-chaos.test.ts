import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, afterEach, beforeAll, expect, it } from 'vitest';
import type { JobId, QueueName } from '../../src/domain/job.js';
import { DEFAULT_TENANT_ID } from '../../src/domain/tenant.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import {
  createTestDatabase,
  describeWithDb,
  schemaUrl,
  waitUntil,
  type TestDatabase,
} from './helpers.js';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));

/** Short leases so a crash is detected in ~2 s instead of the 30 s default. */
const CHAOS_WORKER_ENV = {
  WORKER_QUEUES: 'demo',
  WORKER_CONCURRENCY: '4',
  WORKER_POLL_INTERVAL_MS: '50',
  WORKER_LEASE_DURATION_MS: '1500',
  WORKER_HEARTBEAT_INTERVAL_MS: '500',
  WORKER_REAPER_INTERVAL_MS: '250',
  WORKER_SHUTDOWN_TIMEOUT_MS: '3000',
};

interface WorkerProcess {
  id: string;
  child: ChildProcess;
  output: string[];
  exited: Promise<number | null>;
}

interface AttemptRow {
  job_id: string;
  attempt_number: number;
  status: string;
  worker_id: string | null;
  finished_at: Date | null;
}

/**
 * Milestone 8 chaos: real worker processes (src/worker.ts) killed or frozen
 * mid-job. The guarantee under test is "exactly one recorded success per job":
 * execution is at-least-once (a killed attempt is re-run), but the lease
 * fencing makes sure only the current lease holder can write an outcome.
 */
describeWithDb('worker chaos (Postgres)', { timeout: 90_000 }, () => {
  let db: TestDatabase;
  let jobs: JobService;
  const workers: WorkerProcess[] = [];

  function startWorker(id: string): WorkerProcess {
    const output: string[] = [];
    // Postgres only: wake-ups are an optimisation, polling is what is tested.
    const { REDIS_URL: _unused, ...inherited } = process.env;
    const child = spawn(process.execPath, ['--import', 'tsx', 'src/worker.ts'], {
      cwd: ROOT,
      env: {
        ...inherited,
        NODE_ENV: 'test',
        LOG_LEVEL: 'info',
        DATABASE_URL: schemaUrl(db),
        WORKER_ID: id,
        ...CHAOS_WORKER_ENV,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout?.on('data', (d: Buffer) => output.push(d.toString()));
    child.stderr?.on('data', (d: Buffer) => output.push(d.toString()));
    const exited = new Promise<number | null>((resolve) =>
      child.on('exit', (code) => resolve(code)),
    );
    const worker = { id, child, output, exited };
    workers.push(worker);
    return worker;
  }

  async function enqueueMany(n: number, durationMs: number): Promise<JobId[]> {
    const ids: JobId[] = [];
    for (let i = 0; i < n; i++) {
      const { job } = await jobs.enqueue({
        tenantId: DEFAULT_TENANT_ID,
        queue: 'demo' as QueueName,
        payload: { durationMs },
        maxAttempts: 10,
      });
      ids.push(job.id);
    }
    return ids;
  }

  async function runningJobsOf(workerId: string): Promise<string[]> {
    const { rows } = await db.pool.query<{ id: string }>(
      `SELECT id FROM jobs WHERE status = 'running' AND locked_by = $1`,
      [workerId],
    );
    return rows.map((r) => r.id);
  }

  async function statuses(ids: string[]): Promise<Map<string, string>> {
    const { rows } = await db.pool.query<{ id: string; status: string }>(
      'SELECT id, status FROM jobs WHERE id = ANY($1::uuid[])',
      [ids],
    );
    return new Map(rows.map((r) => [r.id, r.status]));
  }

  async function attempts(ids: string[]): Promise<AttemptRow[]> {
    const { rows } = await db.pool.query<AttemptRow>(
      `SELECT job_id, attempt_number, status::text AS status, worker_id, finished_at
       FROM job_attempts WHERE job_id = ANY($1::uuid[]) ORDER BY job_id, attempt_number`,
      [ids],
    );
    return rows;
  }

  function logsOf(worker: WorkerProcess): string {
    return worker.output.join('');
  }

  beforeAll(async () => {
    db = await createTestDatabase();
    jobs = new JobService(new QueueRepository(db.pool), new JobRepository(db.pool), {
      maxPayloadBytes: 10_000,
    });
  });

  afterEach(async () => {
    for (const w of workers.splice(0)) {
      if (w.child.exitCode === null && w.child.signalCode === null) {
        w.child.kill('SIGKILL');
        await w.exited;
      }
    }
  });

  afterAll(async () => {
    await db?.destroy();
  });

  it('SIGKILLing workers mid-job still completes every job exactly once', async () => {
    const ids = await enqueueMany(24, 400);
    const w0 = startWorker('chaos-w0');
    const w1 = startWorker('chaos-w1');
    const w2 = startWorker('chaos-w2');

    // Kill w0 while it holds jobs, then w1 while it holds jobs.
    let orphaned: string[] = [];
    await waitUntil(async () => (orphaned = await runningJobsOf(w0.id)).length > 0, {
      timeoutMs: 20_000,
      intervalMs: 20,
    });
    w0.child.kill('SIGKILL');
    const killedAt0 = new Date();
    expect(await w0.exited).toBeNull(); // killed by a signal: no exit code, no cleanup ran

    let orphaned1: string[] = [];
    await waitUntil(async () => (orphaned1 = await runningJobsOf(w1.id)).length > 0, {
      timeoutMs: 20_000,
      intervalMs: 20,
    });
    w1.child.kill('SIGKILL');
    const killedAt1 = new Date();
    await w1.exited;

    // A replacement comes up, as a supervisor would start one.
    const w3 = startWorker('chaos-w3');

    await waitUntil(
      async () => [...(await statuses(ids)).values()].every((s) => s === 'succeeded'),
      { timeoutMs: 45_000, intervalMs: 100 },
    ).catch((err: unknown) => {
      throw new Error(
        `jobs did not all succeed: ${String(err)}\nw2:\n${logsOf(w2)}\nw3:\n${logsOf(w3)}`,
      );
    });

    const rows = await attempts(ids);
    // Exactly one succeeded attempt per job.
    for (const id of ids) {
      const succeeded = rows.filter((r) => r.job_id === id && r.status === 'succeeded');
      expect(succeeded, `job ${id}`).toHaveLength(1);
    }
    // Every job the killed workers held was reclaimed by the reaper (timed_out)
    // and then re-run by someone else.
    for (const id of [...orphaned, ...orphaned1]) {
      const history = rows.filter((r) => r.job_id === id);
      expect(history.some((r) => r.status === 'timed_out')).toBe(true);
      expect(history.find((r) => r.status === 'succeeded')?.worker_id).not.toMatch(/^chaos-w[01]$/);
    }
    // Nothing was written by a dead worker after its death.
    for (const r of rows.filter((r) => r.status === 'succeeded')) {
      if (r.worker_id === w0.id) expect(r.finished_at!.getTime()).toBeLessThan(+killedAt0);
      if (r.worker_id === w1.id) expect(r.finished_at!.getTime()).toBeLessThan(+killedAt1);
    }
    // Two expiries at most per job: nobody crossed the poison threshold (3).
    const { rows: dead } = await db.pool.query(
      `SELECT id FROM jobs WHERE id = ANY($1::uuid[]) AND status = 'dead'`,
      [ids],
    );
    expect(dead).toHaveLength(0);
    // The survivors never stopped working.
    expect(w2.child.exitCode).toBeNull();
    expect(w3.child.exitCode).toBeNull();
  });

  it('a frozen worker that wakes up after losing its lease is fenced off', async () => {
    // SIGSTOP models a long GC pause, a frozen VM or a partition: the worker
    // is alive but silent, its heartbeats stop and its lease expires.
    const [jobId] = await enqueueMany(1, 2_500);
    const zombie = startWorker('chaos-zombie');
    await waitUntil(async () => (await runningJobsOf(zombie.id)).length === 1, {
      timeoutMs: 20_000,
      intervalMs: 20,
    });
    zombie.child.kill('SIGSTOP');

    const rescuer = startWorker('chaos-rescuer');
    await waitUntil(async () => (await statuses([jobId!])).get(jobId!) === 'succeeded', {
      timeoutMs: 30_000,
      intervalMs: 50,
    });

    // Resume the zombie: its handler finishes and tries to write the outcome
    // with the stale lease_token.
    zombie.child.kill('SIGCONT');
    await waitUntil(
      () => /lease was lost|Lease lost while running|Heartbeat failed/.test(logsOf(zombie)),
      { timeoutMs: 15_000, intervalMs: 50 },
    ).catch(() => {
      throw new Error(`zombie never noticed the lost lease:\n${logsOf(zombie)}`);
    });

    const rows = await attempts([jobId!]);
    expect(rows.map((r) => [r.attempt_number, r.status, r.worker_id])).toEqual([
      [1, 'timed_out', 'chaos-zombie'],
      [2, 'succeeded', 'chaos-rescuer'],
    ]);
    const { rows: job } = await db.pool.query<{ status: string; attempts: number }>(
      'SELECT status, attempts FROM jobs WHERE id = $1',
      [jobId],
    );
    expect(job[0]).toEqual({ status: 'succeeded', attempts: 2 });

    // Both processes are still healthy and shut down cleanly.
    zombie.child.kill('SIGTERM');
    rescuer.child.kill('SIGTERM');
    expect(await zombie.exited, logsOf(zombie)).toBe(0);
    expect(await rescuer.exited, logsOf(rescuer)).toBe(0);
  });
});
