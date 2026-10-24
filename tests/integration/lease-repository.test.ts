import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { QueueName } from '../../src/domain/job.js';
import type { LeasedJob } from '../../src/domain/lease.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { LeaseRepository, MAX_ERROR_LENGTH } from '../../src/repositories/lease.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import {
  createSchemaPool,
  createTestDatabase,
  describeWithDb,
  type TestDatabase,
} from './helpers.js';

interface JobRow {
  status: string;
  attempts: number;
  locked_by: string | null;
  lease_token: string | null;
  lease_expires_at: Date | null;
  heartbeat_at: Date | null;
  run_at: Date;
  last_error: string | null;
  finished_at: Date | null;
}

interface AttemptRow {
  attempt_number: number;
  status: string;
  worker_id: string | null;
  error: string | null;
  finished_at: Date | null;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describeWithDb('LeaseRepository (Postgres)', () => {
  let db: TestDatabase;
  let repo: LeaseRepository;
  let jobs: JobService;

  beforeAll(async () => {
    db = await createTestDatabase();
    repo = new LeaseRepository(db.pool);
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

  async function enqueue(
    queue: string,
    opts: { priority?: number; delayMs?: number; maxAttempts?: number; payload?: unknown } = {},
  ): Promise<string> {
    const { job } = await jobs.enqueue({
      queue: queue as QueueName,
      payload: opts.payload ?? {},
      ...(opts.priority !== undefined ? { priority: opts.priority } : {}),
      ...(opts.delayMs !== undefined ? { delayMs: opts.delayMs } : {}),
      ...(opts.maxAttempts !== undefined ? { maxAttempts: opts.maxAttempts } : {}),
    });
    return job.id;
  }

  async function jobRow(id: string): Promise<JobRow> {
    const { rows } = await db.pool.query<JobRow>('SELECT * FROM jobs WHERE id = $1', [id]);
    if (!rows[0]) throw new Error(`job ${id} not found`);
    return rows[0];
  }

  async function attempts(id: string): Promise<AttemptRow[]> {
    const { rows } = await db.pool.query<AttemptRow>(
      'SELECT * FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number',
      [id],
    );
    return rows;
  }

  function leaseOne(queue = 'q', leaseMs = 30_000, workerId = 'w1'): Promise<LeasedJob[]> {
    return repo.lease({ queues: [queue], workerId, limit: 1, leaseMs });
  }

  it('leases a job: running, lease columns set, attempts + 1, running attempt row', async () => {
    const id = await enqueue('q', { payload: { a: 1 } });
    const [leased] = await leaseOne();

    expect(leased).toMatchObject({
      jobId: id,
      queue: 'q',
      payload: { a: 1 },
      attempt: 1,
      attemptNumber: 1,
      maxAttempts: 3,
      workerId: 'w1',
    });
    expect(leased?.leaseToken).toMatch(/^[0-9a-f-]{36}$/);
    expect(leased?.leaseExpiresAt.getTime()).toBeGreaterThan(Date.now() + 20_000);

    const row = await jobRow(id);
    expect(row).toMatchObject({ status: 'running', attempts: 1, locked_by: 'w1' });
    expect(row.lease_token).toBe(leased?.leaseToken);
    expect(row.heartbeat_at).not.toBeNull();
    expect(await attempts(id)).toEqual([
      expect.objectContaining({
        attempt_number: 1,
        status: 'running',
        worker_id: 'w1',
        finished_at: null,
      }),
    ]);

    // Running jobs are not leasable again.
    expect(await leaseOne()).toEqual([]);
  });

  it('respects priority (high first), then run_at, and the limit', async () => {
    const low = await enqueue('q', { priority: 0 });
    const high = await enqueue('q', { priority: 90 });
    const mid = await enqueue('q', { priority: 50 });
    const leased = await repo.lease({ queues: ['q'], workerId: 'w', limit: 2, leaseMs: 30_000 });
    expect(leased.map((j) => j.jobId)).toEqual([high, mid]);
    const rest = await repo.lease({ queues: ['q'], workerId: 'w', limit: 5, leaseMs: 30_000 });
    expect(rest.map((j) => j.jobId)).toEqual([low]);
  });

  it('does not lease delayed jobs before run_at, nor jobs from other queues', async () => {
    await enqueue('q', { delayMs: 60_000 });
    await enqueue('other');
    expect(await leaseOne('q')).toEqual([]);
    expect(await repo.lease({ queues: [], workerId: 'w', limit: 5, leaseMs: 1000 })).toEqual([]);
    expect(await repo.lease({ queues: ['q'], workerId: 'w', limit: 0, leaseMs: 1000 })).toEqual([]);
  });

  describe('delayed jobs (M4)', () => {
    it('never leases a runAt or delayMs job early; leases it once due, by priority', async () => {
      const viaRunAt = (
        await jobs.enqueue({
          queue: 'q' as QueueName,
          payload: { kind: 'runAt' },
          runAt: new Date(Date.now() + 3_600_000),
          priority: 10,
        })
      ).job.id;
      const viaDelay = await enqueue('q', { delayMs: 3_600_000, priority: 20 });
      const ready = await enqueue('q', { priority: 0 });

      // Only the immediately runnable job is handed out, despite lower priority.
      const first = await repo.lease({ queues: ['q'], workerId: 'w', limit: 10, leaseMs: 30_000 });
      expect(first.map((j) => j.jobId)).toEqual([ready]);
      expect((await jobRow(viaRunAt)).status).toBe('scheduled');
      expect((await jobRow(viaDelay)).status).toBe('scheduled');
      expect(await repo.promoteDue(['q'])).toBe(0);

      // Time passes (simulated by moving run_at into the past).
      await db.pool.query(
        `UPDATE jobs SET run_at = now() - interval '1 second' WHERE id = ANY($1)`,
        [[viaRunAt, viaDelay]],
      );
      const due = await repo.lease({ queues: ['q'], workerId: 'w', limit: 10, leaseMs: 30_000 });
      expect(due.map((j) => j.jobId)).toEqual([viaDelay, viaRunAt]);
    });

    it('a scheduled job one second from due is not leased; promotion is per queue and bounded', async () => {
      const soon = await enqueue('q', { delayMs: 1_000 });
      expect(await leaseOne('q')).toEqual([]);
      expect((await jobRow(soon)).status).toBe('scheduled');

      const ids: string[] = [];
      for (let i = 0; i < 5; i++) ids.push(await enqueue('q', { delayMs: 60_000 }));
      const otherQueue = await enqueue('other', { delayMs: 60_000 });
      await db.pool.query(`UPDATE jobs SET run_at = now() - interval '1 second'`);

      expect(await repo.promoteDue(['q'], 2)).toBe(2);
      expect(await repo.promoteDue(['q'], 100)).toBe(4);
      expect(await repo.promoteDue(['q'], 100)).toBe(0);
      expect((await jobRow(otherQueue)).status).toBe('scheduled');
      expect(await repo.promoteDue([], 100)).toBe(0);
    });

    it('a retry backoff (fail with delay) is not re-leased until its run_at', async () => {
      await enqueue('q');
      const [leased] = await leaseOne();
      if (!leased) throw new Error('expected a lease');
      expect(
        await repo.fail(leased, { error: 'boom', attemptStatus: 'failed', retryDelayMs: 60_000 }),
      ).toBe('retrying');
      expect(await leaseOne()).toEqual([]);
      await db.pool.query(`UPDATE jobs SET run_at = now() - interval '1 second'`);
      expect((await leaseOne()).map((j) => j.jobId)).toEqual([leased.jobId]);
    });
  });

  it('leases from several queues at once and ignores unknown queue names', async () => {
    const a = await enqueue('a');
    const b = await enqueue('b');
    const leased = await repo.lease({
      queues: ['a', 'b', 'missing'],
      workerId: 'w',
      limit: 10,
      leaseMs: 30_000,
    });
    expect(leased.map((j) => j.jobId).sort()).toEqual([a, b].sort());
  });

  it('heartbeat extends the lease; a stale token cannot', async () => {
    await enqueue('q');
    const [leased] = await leaseOne('q', 1_000);
    if (!leased) throw new Error('nothing leased');
    const extended = await repo.heartbeat(leased, 60_000);
    expect(extended?.getTime()).toBeGreaterThan(leased.leaseExpiresAt.getTime() + 50_000);
    expect(await repo.heartbeat({ ...leased, leaseToken: crypto.randomUUID() }, 60_000)).toBeNull();
  });

  it('complete marks job + attempt succeeded and clears the lease', async () => {
    const id = await enqueue('q');
    const [leased] = await leaseOne();
    if (!leased) throw new Error('nothing leased');
    expect(await repo.complete(leased)).toBe(true);
    const row = await jobRow(id);
    expect(row).toMatchObject({
      status: 'succeeded',
      locked_by: null,
      lease_token: null,
      lease_expires_at: null,
    });
    expect(row.finished_at).not.toBeNull();
    expect((await attempts(id))[0]).toMatchObject({ status: 'succeeded' });
    // Second completion with the same (now spent) lease writes nothing.
    expect(await repo.complete(leased)).toBe(false);
  });

  it('fail with a delay reschedules on the database clock; fail without one dead-letters', async () => {
    const id = await enqueue('q', { maxAttempts: 2 });
    const [first] = await leaseOne();
    if (!first) throw new Error('nothing leased');
    const before = Date.now();
    expect(
      await repo.fail(first, { error: 'boom', attemptStatus: 'failed', retryDelayMs: 60_000 }),
    ).toBe('retrying');
    let row = await jobRow(id);
    expect(row).toMatchObject({
      status: 'scheduled',
      last_error: 'boom',
      locked_by: null,
      finished_at: null,
    });
    expect(row.run_at.getTime()).toBeGreaterThan(before + 55_000);
    expect(await leaseOne()).toEqual([]); // not before run_at

    await db.pool.query('UPDATE jobs SET run_at = now() WHERE id = $1', [id]);
    const [second] = await leaseOne();
    if (!second) throw new Error('nothing leased');
    expect(second).toMatchObject({ attempt: 2, attemptNumber: 2 });
    expect(
      await repo.fail(second, { error: 'slow', attemptStatus: 'timed_out', retryDelayMs: null }),
    ).toBe('dead_lettered');
    row = await jobRow(id);
    expect(row).toMatchObject({ status: 'dead', attempts: 2, last_error: 'slow' });
    expect(row.finished_at).not.toBeNull();
    expect((await attempts(id)).map((a) => [a.status, a.error])).toEqual([
      ['failed', 'boom'],
      ['timed_out', 'slow'],
    ]);
  });

  it('truncates huge error messages', async () => {
    const id = await enqueue('q', { maxAttempts: 1 });
    const [leased] = await leaseOne();
    if (!leased) throw new Error('nothing leased');
    await repo.fail(leased, {
      error: 'x'.repeat(50_000),
      attemptStatus: 'failed',
      retryDelayMs: null,
    });
    expect((await jobRow(id)).last_error).toHaveLength(MAX_ERROR_LENGTH);
  });

  it('release requeues immediately and refunds the attempt (history keeps a released row)', async () => {
    const id = await enqueue('q', { maxAttempts: 1 });
    const [leased] = await leaseOne();
    if (!leased) throw new Error('nothing leased');
    expect(await repo.release(leased, 'shutdown')).toBe(true);
    expect(await jobRow(id)).toMatchObject({ status: 'queued', attempts: 0, locked_by: null });

    // max_attempts = 1 and it can still run: the released attempt did not count.
    const [again] = await leaseOne();
    expect(again).toMatchObject({ attempt: 1, attemptNumber: 2 });
    expect((await attempts(id)).map((a) => [a.attempt_number, a.status, a.error])).toEqual([
      [1, 'released', 'shutdown'],
      [2, 'running', null],
    ]);
    expect(await repo.release(leased, 'stale')).toBe(false);
  });

  it('reaper reclaims expired leases (requeue, or dead-letter on the last attempt) and fences the old owner', async () => {
    const retryable = await enqueue('q', { maxAttempts: 3, priority: 10 });
    const last = await enqueue('q', { maxAttempts: 1 });
    const healthy = await enqueue('q');
    const leasedShort = await repo.lease({
      queues: ['q'],
      workerId: 'crashed',
      limit: 2,
      leaseMs: 50,
    });
    const [leasedLong] = await repo.lease({
      queues: ['q'],
      workerId: 'alive',
      limit: 1,
      leaseMs: 60_000,
    });
    expect(leasedShort.map((j) => j.jobId)).toEqual([retryable, last]);
    expect(leasedLong?.jobId).toBe(healthy);
    await sleep(120);

    const reclaimed = await repo.reapExpired(100);
    expect(reclaimed.map((r) => [r.jobId, r.status, r.previousWorkerId]).sort()).toEqual(
      [
        [retryable, 'queued', 'crashed'],
        [last, 'dead', 'crashed'],
      ].sort(),
    );
    expect(await jobRow(retryable)).toMatchObject({
      status: 'queued',
      attempts: 1,
      locked_by: null,
    });
    expect((await jobRow(retryable)).last_error).toMatch(/Lease expired: worker crashed/);
    expect(await jobRow(last)).toMatchObject({ status: 'dead', attempts: 1 });
    expect(await jobRow(healthy)).toMatchObject({ status: 'running', locked_by: 'alive' });
    expect((await attempts(retryable))[0]).toMatchObject({ status: 'timed_out' });

    // The crashed worker wakes up and tries to finish: fenced out.
    const stale = leasedShort[0];
    if (!stale) throw new Error('missing');
    expect(await repo.complete(stale)).toBe(false);
    expect(await repo.heartbeat(stale, 1_000)).toBeNull();

    // Re-leased by someone else as attempt 2; nothing left to reap.
    const [again] = await leaseOne('q', 30_000, 'w2');
    expect(again).toMatchObject({ jobId: retryable, attempt: 2, attemptNumber: 2 });
    expect(await repo.reapExpired(100)).toEqual([]);
  });

  it('concurrent lease calls never hand out the same job twice (SKIP LOCKED)', async () => {
    const total = 200;
    await db.pool.query(
      `INSERT INTO queues (name) VALUES ('race');
       INSERT INTO jobs (queue_id, payload) SELECT id, '{}'::jsonb FROM queues, generate_series(1, ${total}) WHERE name = 'race'`,
    );
    const pools = Array.from({ length: 10 }, () => createSchemaPool(db, 2));
    try {
      const batches = await Promise.all(
        Array.from({ length: 20 }, (_, i) =>
          new LeaseRepository(pools[i % pools.length]!).lease({
            queues: ['race'],
            workerId: `w${i}`,
            limit: 15,
            leaseMs: 30_000,
          }),
        ),
      );
      const ids = batches.flat().map((j) => j.jobId);
      expect(new Set(ids).size).toBe(ids.length);
      const { rows } = await db.pool.query<{ running: string; attempts: string }>(
        `SELECT (SELECT count(*) FROM jobs WHERE status = 'running') AS running,
                (SELECT count(*) FROM job_attempts) AS attempts`,
      );
      expect(Number(rows[0]?.running)).toBe(ids.length);
      expect(Number(rows[0]?.attempts)).toBe(ids.length);
      expect(ids.length).toBeGreaterThan(0);
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
  });

  it('schema rejects a running job without a lease and a second running attempt', async () => {
    const id = await enqueue('q');
    await expect(
      db.pool.query(`UPDATE jobs SET status = 'running' WHERE id = $1`, [id]),
    ).rejects.toMatchObject({ code: '23514', constraint: 'jobs_running_has_lease' });

    const [leased] = await leaseOne();
    if (!leased) throw new Error('nothing leased');
    await expect(
      db.pool.query(
        `INSERT INTO job_attempts (job_id, attempt_number, status) VALUES ($1, 99, 'running')`,
        [id],
      ),
    ).rejects.toMatchObject({ code: '23505', constraint: 'job_attempts_one_running_per_job_uq' });
  });

  it('the reaper query is served by the partial lease-expiry index', async () => {
    const client = await db.pool.connect();
    try {
      await client.query('SET enable_seqscan = off');
      const { rows } = await client.query<{ 'QUERY PLAN': string }>(
        `EXPLAIN SELECT id FROM jobs WHERE status = 'running' AND lease_expires_at < now()
         ORDER BY lease_expires_at LIMIT 100 FOR UPDATE SKIP LOCKED`,
      );
      expect(rows.map((r) => r['QUERY PLAN']).join('\n')).toContain(
        'jobs_running_lease_expiry_idx',
      );
    } finally {
      client.release();
    }
  });
  describe('nextDueInMs (M5)', () => {
    it('is null when nothing is scheduled on the queues', async () => {
      await enqueue('q');
      expect(await repo.nextDueInMs(['q'])).toBeNull();
      expect(await repo.nextDueInMs([])).toBeNull();
      expect(await repo.nextDueInMs(['missing'])).toBeNull();
    });

    it('returns the time until the earliest scheduled job across the given queues', async () => {
      await enqueue('q', { delayMs: 60_000 });
      await enqueue('q', { delayMs: 20_000 });
      await enqueue('other', { delayMs: 5_000 });
      const due = await repo.nextDueInMs(['q']);
      expect(due).toBeGreaterThan(15_000);
      expect(due).toBeLessThanOrEqual(20_000);
      const both = await repo.nextDueInMs(['q', 'other']);
      expect(both).toBeGreaterThan(1_000);
      expect(both).toBeLessThanOrEqual(5_000);
    });

    it('clamps overdue jobs to 0', async () => {
      const id = await enqueue('q', { delayMs: 60_000 });
      await db.pool.query(`UPDATE jobs SET run_at = now() - interval '1 minute' WHERE id = $1`, [
        id,
      ]);
      expect(await repo.nextDueInMs(['q'])).toBe(0);
    });
  });
});
