import { copyFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import type { DeadLetterReason } from '../../src/domain/dead-letter.js';
import type { JobId, QueueName } from '../../src/domain/job.js';
import { DEFAULT_MIGRATIONS_DIR, runMigrations } from '../../src/db/migrate.js';
import { DeadLetterRepository } from '../../src/repositories/dead-letter.repository.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import {
  createSchemaPool,
  createTestDatabase,
  describeWithDb,
  type TestDatabase,
} from './helpers.js';

interface Row {
  status: string;
  attempts: number;
  dead_reason: string | null;
  dead_lettered_at: Date | null;
  lease_expiries: number;
  replay_count: number;
  last_replayed_at: Date | null;
  finished_at: Date | null;
  locked_by: string | null;
  last_error: string | null;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describeWithDb('dead-letter queue (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let leases: LeaseRepository;
  let dlq: DeadLetterRepository;
  let queues: QueueRepository;
  let jobs: JobService;

  beforeAll(async () => {
    db = await createTestDatabase();
    leases = new LeaseRepository(db.pool);
    dlq = new DeadLetterRepository(db.pool);
    queues = new QueueRepository(db.pool);
    jobs = new JobService(queues, new JobRepository(db.pool), { maxPayloadBytes: 10_000 });
  });

  afterAll(async () => {
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
  });

  async function enqueue(queue: string, maxAttempts = 1): Promise<JobId> {
    const { job } = await jobs.enqueue({ queue: queue as QueueName, payload: {}, maxAttempts });
    return job.id;
  }

  async function queueId(name: string): Promise<string> {
    const queue = await queues.findByName(name as QueueName);
    if (!queue) throw new Error(`no queue ${name}`);
    return queue.id;
  }

  /** Enqueue, lease and fail terminally: a real dead-lettered job with one attempt row. */
  async function deadJob(
    queue = 'q',
    reason: DeadLetterReason = 'attempts_exhausted',
  ): Promise<JobId> {
    const id = await enqueue(queue);
    const [leased] = await leases.lease({
      queues: [queue],
      workerId: 'w',
      limit: 1,
      leaseMs: 30_000,
    });
    if (leased?.jobId !== id) throw new Error('lease mismatch');
    const disposition = await leases.fail(leased, {
      error: 'boom',
      attemptStatus: 'failed',
      retryDelayMs: null,
      deadLetterReason: reason,
    });
    if (disposition !== 'dead_lettered') throw new Error('not dead-lettered');
    return id;
  }

  async function row(id: string): Promise<Row> {
    const { rows } = await db.pool.query<Row>(
      `SELECT status, attempts, dead_reason, dead_lettered_at, lease_expiries, replay_count,
              last_replayed_at, finished_at, locked_by, last_error
       FROM jobs WHERE id = $1`,
      [id],
    );
    return rows[0]!;
  }

  async function attemptCount(id: string): Promise<number> {
    const { rows } = await db.pool.query<{ n: string }>(
      'SELECT count(*) AS n FROM job_attempts WHERE job_id = $1',
      [id],
    );
    return Number(rows[0]?.n);
  }

  it('schema: a dead job must carry DLQ metadata, and legacy failed is rejected', async () => {
    const id = await enqueue('q');
    await expect(
      db.pool.query("UPDATE jobs SET status = 'dead' WHERE id = $1", [id]),
    ).rejects.toThrow(/jobs_dead_has_metadata/);
    await expect(
      db.pool.query(
        "UPDATE jobs SET dead_reason = 'non_retryable', dead_lettered_at = now() WHERE id = $1",
        [id],
      ),
    ).rejects.toThrow(/jobs_dead_has_metadata/);
    await expect(
      db.pool.query("UPDATE jobs SET status = 'failed', finished_at = now() WHERE id = $1", [id]),
    ).rejects.toThrow(/jobs_no_legacy_failed/);
  });

  it('fail(null) dead-letters atomically with the attempt, keeping the history', async () => {
    const id = await deadJob('q', 'invalid_payload');
    const r = await row(id);
    expect(r).toMatchObject({
      status: 'dead',
      dead_reason: 'invalid_payload',
      attempts: 1,
      last_error: 'boom',
      locked_by: null,
    });
    expect(r.dead_lettered_at).not.toBeNull();
    expect(r.finished_at).not.toBeNull();
    const attempts = await dlq.listAttempts(id);
    expect(attempts).toEqual([
      expect.objectContaining({ attemptNumber: 1, status: 'failed', workerId: 'w', error: 'boom' }),
    ]);
  });

  it('a stale lease cannot dead-letter a job another worker re-leased', async () => {
    const id = await enqueue('q', 3);
    const [stale] = await leases.lease({ queues: ['q'], workerId: 'slow', limit: 1, leaseMs: 50 });
    if (!stale) throw new Error('nothing leased');
    await sleep(120);
    expect(await leases.reapExpired(10)).toHaveLength(1);
    const [fresh] = await leases.lease({
      queues: ['q'],
      workerId: 'fast',
      limit: 1,
      leaseMs: 30_000,
    });
    expect(fresh?.jobId).toBe(id);

    // The slow worker comes back and reports a non-retryable failure: fenced out.
    expect(
      await leases.fail(stale, {
        error: 'late',
        attemptStatus: 'failed',
        retryDelayMs: null,
        deadLetterReason: 'non_retryable',
      }),
    ).toBeNull();
    expect(await row(id)).toMatchObject({
      status: 'running',
      locked_by: 'fast',
      dead_reason: null,
    });
  });

  it('reaper dead-letters a poison job after N lease expiries even with attempts left', async () => {
    const id = await enqueue('q', 10);
    for (let i = 1; i <= 3; i++) {
      const [leased] = await leases.lease({
        queues: ['q'],
        workerId: `crash-${i}`,
        limit: 1,
        leaseMs: 30,
      });
      expect(leased?.jobId).toBe(id);
      await sleep(80);
      const [reclaimed] = await leases.reapExpired(10, { poisonLeaseExpiries: 3 });
      expect(reclaimed).toMatchObject({ jobId: id, leaseExpiries: i });
      expect(reclaimed?.status).toBe(i < 3 ? 'queued' : 'dead');
      expect(reclaimed?.deadLetterReason).toBe(i < 3 ? null : 'poison_lease_expired');
    }
    expect(await row(id)).toMatchObject({
      status: 'dead',
      dead_reason: 'poison_lease_expired',
      attempts: 3,
      lease_expiries: 3,
    });
    expect((await dlq.listAttempts(id)).map((a) => a.status)).toEqual([
      'timed_out',
      'timed_out',
      'timed_out',
    ]);
  });

  it('reaper uses attempts_exhausted on the last attempt when the poison threshold is not hit', async () => {
    const id = await enqueue('q', 1);
    await leases.lease({ queues: ['q'], workerId: 'crash', limit: 1, leaseMs: 30 });
    await sleep(80);
    const [reclaimed] = await leases.reapExpired(10, { poisonLeaseExpiries: 5 });
    expect(reclaimed).toMatchObject({ status: 'dead', deadLetterReason: 'attempts_exhausted' });
    expect((await row(id)).dead_reason).toBe('attempts_exhausted');
  });

  it('replay resets the budget, clears DLQ metadata and continues the attempt numbering', async () => {
    const id = await deadJob();
    const replayed = await dlq.replay({ jobId: id, queueId: await queueId('q'), delayMs: 0 });
    expect(replayed).toMatchObject({
      id,
      status: 'queued',
      attempts: 0,
      deadReason: null,
      deadLetteredAt: null,
      replayCount: 1,
      leaseExpiries: 0,
      finishedAt: null,
    });
    expect(replayed?.lastReplayedAt).not.toBeNull();

    const [again] = await leases.lease({
      queues: ['q'],
      workerId: 'w2',
      limit: 1,
      leaseMs: 30_000,
    });
    expect(again).toMatchObject({ jobId: id, attempt: 1, attemptNumber: 2 });
    expect(await leases.complete(again!)).toBe(true);
    expect((await dlq.listAttempts(id)).map((a) => [a.attemptNumber, a.status])).toEqual([
      [1, 'failed'],
      [2, 'succeeded'],
    ]);
  });

  it('replay with a delay schedules on the database clock', async () => {
    const id = await deadJob();
    const before = Date.now();
    const replayed = await dlq.replay({ jobId: id, queueId: await queueId('q'), delayMs: 60_000 });
    expect(replayed?.status).toBe('scheduled');
    expect(replayed!.runAt.getTime()).toBeGreaterThan(before + 55_000);
    expect(await leases.lease({ queues: ['q'], workerId: 'w', limit: 1, leaseMs: 1_000 })).toEqual(
      [],
    );
  });

  it('concurrent replays of the same job: exactly one wins (10 connections)', async () => {
    const id = await deadJob();
    const qid = await queueId('q');
    const pools = Array.from({ length: 10 }, () => createSchemaPool(db, 1));
    try {
      const results = await Promise.all(
        pools.map((pool) =>
          new DeadLetterRepository(pool).replay({ jobId: id, queueId: qid, delayMs: 0 }),
        ),
      );
      expect(results.filter((r) => r !== null)).toHaveLength(1);
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
    expect(await row(id)).toMatchObject({ status: 'queued', replay_count: 1, attempts: 0 });
    expect(await attemptCount(id)).toBe(1);
  });

  it('concurrent replay vs purge of the same job: exactly one of them takes effect', async () => {
    for (let i = 0; i < 10; i++) {
      const id = await deadJob(`race-${i}`);
      const qid = await queueId(`race-${i}`);
      const [a, b] = [createSchemaPool(db, 1), createSchemaPool(db, 1)];
      try {
        const [replayed, purged] = await Promise.all([
          new DeadLetterRepository(a).replay({ jobId: id, queueId: qid, delayMs: 0 }),
          new DeadLetterRepository(b).purge(id, qid),
        ]);
        expect(Number(replayed !== null) + Number(purged)).toBe(1);
        const { rows } = await db.pool.query('SELECT status FROM jobs WHERE id = $1', [id]);
        expect(rows).toEqual(purged ? [] : [{ status: 'queued' }]);
      } finally {
        await Promise.all([a.end(), b.end()]);
      }
    }
  });

  it('concurrent bulk replays partition the DLQ (SKIP LOCKED): every job replayed once', async () => {
    const total = 60;
    const ids = new Set<string>();
    for (let i = 0; i < total; i++) ids.add(await deadJob());
    const qid = await queueId('q');
    const pools = Array.from({ length: 5 }, () => createSchemaPool(db, 1));
    try {
      const batches = await Promise.all(
        pools.map((pool) =>
          new DeadLetterRepository(pool).replayBatch({ queueId: qid, limit: 20, delayMs: 0 }),
        ),
      );
      const all = batches.flat();
      expect(new Set(all).size).toBe(all.length); // no job in two batches
      expect(all.length).toBeLessThanOrEqual(total);
      // Whatever SKIP LOCKED left behind is picked up by one more pass.
      const rest = await dlq.replayBatch({ queueId: qid, limit: total, delayMs: 0 });
      expect(new Set([...all, ...rest])).toEqual(ids);
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
    const { rows } = await db.pool.query<{ n: string }>(
      'SELECT count(*) AS n FROM jobs WHERE replay_count = 1 AND status = $1',
      ['queued'],
    );
    expect(Number(rows[0]?.n)).toBe(total);
  });

  it('bulk replay honours the reason filter and the limit, oldest first', async () => {
    const a = await deadJob('q', 'invalid_payload');
    await deadJob('q', 'attempts_exhausted');
    const c = await deadJob('q', 'invalid_payload');
    const qid = await queueId('q');
    expect(
      await dlq.replayBatch({ queueId: qid, limit: 1, reason: 'invalid_payload', delayMs: 0 }),
    ).toEqual([a]);
    expect(
      await dlq.replayBatch({ queueId: qid, limit: 10, reason: 'invalid_payload', delayMs: 0 }),
    ).toEqual([c]);
  });

  it('lists a queue DLQ newest first with keyset paging and a reason filter', async () => {
    const ids: string[] = [];
    for (let i = 0; i < 5; i++)
      ids.push(await deadJob('q', i % 2 ? 'non_retryable' : 'attempts_exhausted'));
    await deadJob('other');
    await enqueue('q'); // live job: never listed
    const qid = await queueId('q');

    const first = await dlq.listByQueue({ queueId: qid, limit: 3 });
    expect(first.map((j) => j.id)).toEqual([ids[4], ids[3], ids[2]]);
    const last = first[2]!;
    const second = await dlq.listByQueue({
      queueId: qid,
      limit: 3,
      after: { createdAt: last.deadLetteredAtCursor!, id: last.id },
    });
    expect(second.map((j) => j.id)).toEqual([ids[1], ids[0]]);
    const filtered = await dlq.listByQueue({ queueId: qid, limit: 10, reason: 'non_retryable' });
    expect(filtered.map((j) => j.id)).toEqual([ids[3], ids[1]]);
  });

  it('purge deletes a dead job and cascades its attempts; never touches live jobs', async () => {
    const id = await deadJob();
    const live = await enqueue('q');
    const qid = await queueId('q');
    expect(await dlq.purge(live, qid)).toBe(false);
    expect(await dlq.purge(id, qid)).toBe(true);
    expect(await dlq.purge(id, qid)).toBe(false);
    expect(await attemptCount(id)).toBe(0);
    expect((await row(live)).status).toBe('queued');
  });

  it('purgeOlderThan respects the cutoff, the limit and the queue', async () => {
    const old1 = await deadJob();
    const old2 = await deadJob();
    await db.pool.query(
      "UPDATE jobs SET dead_lettered_at = now() - interval '10 days' WHERE id = ANY($1::uuid[])",
      [[old1, old2]],
    );
    const recent = await deadJob();
    const otherQueue = await deadJob('other');
    await db.pool.query(
      "UPDATE jobs SET dead_lettered_at = now() - interval '10 days' WHERE id = $1",
      [otherQueue],
    );
    const qid = await queueId('q');
    const cutoff = new Date(Date.now() - 24 * 3600 * 1000);

    expect(await dlq.purgeOlderThan({ queueId: qid, olderThan: cutoff, limit: 1 })).toBe(1);
    expect(await dlq.purgeOlderThan({ queueId: qid, olderThan: cutoff, limit: 100 })).toBe(1);
    expect(await dlq.purgeOlderThan({ queueId: qid, olderThan: cutoff, limit: 100 })).toBe(0);
    const { rows } = await db.pool.query<{ id: string }>('SELECT id FROM jobs ORDER BY id');
    expect(rows.map((r) => r.id).sort()).toEqual([recent, otherQueue].sort());
  });

  it('DLQ listing walks the partial jobs_dlq_idx in order without a sort', async () => {
    await deadJob();
    const client = await db.pool.connect();
    try {
      // Tiny tables always favour a seq scan; disable it to prove the index *can* serve the query.
      await client.query('SET enable_seqscan = off');
      const { rows } = await client.query<{ 'QUERY PLAN': string }>(
        `EXPLAIN SELECT id FROM jobs
         WHERE queue_id = $1 AND status = 'dead'
         ORDER BY dead_lettered_at DESC, id DESC LIMIT 51`,
        [await queueId('q')],
      );
      const text = rows.map((r) => r['QUERY PLAN']).join('\n');
      expect(text).toContain('jobs_dlq_idx');
      expect(text).not.toMatch(/\bSort\b/);
    } finally {
      await client.query('RESET enable_seqscan');
      client.release();
    }
  });

  it('migration 0003 moves M2 failed jobs into the DLQ with a reason', async () => {
    const legacy = await createTestDatabase({ migrate: false });
    const dir = await mkdtemp(path.join(tmpdir(), 'pq-m2-'));
    try {
      for (const f of ['0001_create_queues_jobs_attempts.sql', '0002_worker_leasing.sql']) {
        await copyFile(path.join(DEFAULT_MIGRATIONS_DIR, f), path.join(dir, f));
      }
      await runMigrations(legacy.pool, { dir });
      await legacy.pool.query(`
        INSERT INTO queues (name) VALUES ('old');
        INSERT INTO jobs (queue_id, payload, status, attempts, max_attempts, finished_at, last_error)
        SELECT id, '{"k":1}', 'failed', 3, 3, now() - interval '1 hour', 'exhausted' FROM queues;
        INSERT INTO jobs (queue_id, payload, status, attempts, max_attempts, finished_at, last_error)
        SELECT id, '{"k":2}', 'failed', 1, 5, now(), 'NonRetryableError: bad' FROM queues;
        INSERT INTO jobs (queue_id, payload) SELECT id, '{"k":3}' FROM queues;`);

      const result = await runMigrations(legacy.pool);
      expect(result.applied).toContain('0003_dead_letter_queue.sql');

      const { rows } = await legacy.pool.query<{
        k: number;
        status: string;
        dead_reason: string | null;
        same: boolean;
      }>(
        `SELECT (payload->>'k')::int AS k, status, dead_reason,
                dead_lettered_at = finished_at AS same
         FROM jobs ORDER BY 1`,
      );
      expect(rows).toEqual([
        { k: 1, status: 'dead', dead_reason: 'attempts_exhausted', same: true },
        { k: 2, status: 'dead', dead_reason: 'non_retryable', same: true },
        { k: 3, status: 'queued', dead_reason: null, same: null },
      ]);
    } finally {
      await rm(dir, { recursive: true, force: true });
      await legacy.destroy();
    }
  });
});
