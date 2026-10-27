import pino from 'pino';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { LeasedJob } from '../../src/domain/lease.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { HandlerRegistry } from '../../src/worker/registry.js';
import { Worker } from '../../src/worker/worker.js';
import {
  createSchemaPool,
  createTestDatabase,
  describeWithDb,
  type TestDatabase,
} from './helpers.js';

const logger = pino({ level: 'silent' });
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function waitFor(predicate: () => Promise<boolean> | boolean, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await sleep(20);
  }
}

/**
 * Queue concurrency caps (M6): max_concurrency is enforced by the lease
 * query itself, so it holds across any number of worker processes.
 */
describeWithDb('queue concurrency cap (Postgres)', { timeout: 60_000 }, () => {
  let db: TestDatabase;
  let repo: LeaseRepository;

  beforeAll(async () => {
    db = await createTestDatabase();
    repo = new LeaseRepository(db.pool);
  });

  afterAll(async () => {
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
  });

  async function queue(name: string, maxConcurrency: number | null, jobs: number, priority = 0) {
    await db.pool.query(`INSERT INTO queues (name, max_concurrency) VALUES ($1, $2)`, [
      name,
      maxConcurrency,
    ]);
    await db.pool.query(
      `INSERT INTO jobs (queue_id, payload, priority)
       SELECT q.id, '{}'::jsonb, $2 FROM queues q, generate_series(1, $3) WHERE q.name = $1`,
      [name, priority, jobs],
    );
  }

  const lease = (queues: string[], limit = 10, r: LeaseRepository = repo) =>
    r.lease({ queues, workerId: 'w', limit, leaseMs: 30_000 });

  async function running(name: string): Promise<number> {
    const { rows } = await db.pool.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM jobs j JOIN queues q ON q.id = j.queue_id
       WHERE q.name = $1 AND j.status = 'running'`,
      [name],
    );
    return rows[0]!.n;
  }

  it('leases at most max_concurrency jobs, and more only as running ones finish', async () => {
    await queue('capped', 2, 6);
    const first = await lease(['capped']);
    expect(first).toHaveLength(2);
    expect(first.every((j) => j.queueCapped)).toBe(true);
    expect(await lease(['capped'])).toEqual([]);

    expect(await repo.complete(first[0]!)).toBe(true);
    const next = await lease(['capped']);
    expect(next).toHaveLength(1);
    expect(await running('capped')).toBe(2);

    // A failure with a retry and a release also free the slot.
    await repo.fail(first[1]!, { error: 'x', attemptStatus: 'failed', retryDelayMs: 0 });
    await repo.release(next[0]!, 'shutdown');
    expect(await lease(['capped'])).toHaveLength(2);
  });

  it('caps only the capped queue; priority order still holds across queues', async () => {
    await queue('capped', 2, 5, 10);
    await queue('free', null, 5, 0);
    const jobs = await lease(['capped', 'free'], 4);
    expect(jobs.map((j) => j.queue)).toEqual(['capped', 'capped', 'free', 'free']);
    expect(jobs.filter((j) => j.queue === 'free').every((j) => !j.queueCapped)).toBe(true);
    // Capped is full; the rest of the free queue is still leasable.
    expect((await lease(['capped', 'free'], 10)).map((j) => j.queue)).toEqual([
      'free',
      'free',
      'free',
    ]);
  });

  it('a settings change applies to the next lease (raising, then lowering below running)', async () => {
    await queue('elastic', 1, 6);
    expect(await lease(['elastic'])).toHaveLength(1);
    await db.pool.query(`UPDATE queues SET max_concurrency = 3 WHERE name = 'elastic'`);
    expect(await lease(['elastic'])).toHaveLength(2);
    // Lowering never preempts running jobs; it only stops new leases.
    await db.pool.query(`UPDATE queues SET max_concurrency = 1 WHERE name = 'elastic'`);
    expect(await lease(['elastic'])).toEqual([]);
    expect(await running('elastic')).toBe(3);
    // Removing the cap goes back to the uncapped path.
    await db.pool.query(`UPDATE queues SET max_concurrency = NULL WHERE name = 'elastic'`);
    expect(await lease(['elastic'])).toHaveLength(3);
  });

  it('holds under concurrent lease calls from many connections (no over-lease)', async () => {
    await queue('race-cap', 5, 200);
    await queue('race-free', null, 100);
    const pools = Array.from({ length: 10 }, () => createSchemaPool(db, 2));
    try {
      for (let round = 0; round < 5; round++) {
        const batches = await Promise.all(
          Array.from({ length: 20 }, (_, i) =>
            lease(['race-cap', 'race-free'], 3, new LeaseRepository(pools[i % pools.length]!)),
          ),
        );
        const leased = batches.flat();
        expect(new Set(leased.map((j) => j.jobId)).size).toBe(leased.length);
        expect(await running('race-cap')).toBe(5);
        // Finish everything leased this round; the next round starts from zero running.
        await Promise.all(leased.map((j) => repo.complete(j)));
      }
      // The uncapped queue was drained in parallel.
      expect(await running('race-free')).toBe(0);
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
  });

  it('never deadlocks when workers lease overlapping sets of capped queues', async () => {
    for (const name of ['dl-a', 'dl-b', 'dl-c']) await queue(name, 2, 20);
    const pools = Array.from({ length: 6 }, () => createSchemaPool(db, 2));
    const sets = [
      ['dl-a', 'dl-b', 'dl-c'],
      ['dl-c', 'dl-b', 'dl-a'],
      ['dl-b', 'dl-a'],
    ];
    try {
      const batches = await Promise.all(
        Array.from({ length: 18 }, (_, i) =>
          lease(sets[i % sets.length]!, 4, new LeaseRepository(pools[i % pools.length]!)),
        ),
      );
      expect(batches.flat()).toHaveLength(6);
      for (const name of ['dl-a', 'dl-b', 'dl-c']) expect(await running(name)).toBe(2);
    } finally {
      await Promise.all(pools.map((p) => p.end()));
    }
  });

  describe('with real workers', () => {
    it('three workers with 4 slots each never run more than the cap at once', async () => {
      await queue('crawl', 2, 24);
      let inFlight = 0;
      let peak = 0;
      let done = 0;
      const pools = Array.from({ length: 3 }, () => createSchemaPool(db, 4));
      const workers = pools.map((pool, i) => {
        const registry = new HandlerRegistry().register('crawl', async () => {
          inFlight++;
          peak = Math.max(peak, inFlight);
          await sleep(15);
          inFlight--;
          done++;
        });
        return new Worker({
          store: new LeaseRepository(pool),
          registry,
          logger,
          workerId: `cap-w${i}`,
          queues: ['crawl'],
          concurrency: 4,
          pollIntervalMs: 10,
          leaseDurationMs: 5_000,
          heartbeatIntervalMs: 1_000,
          jobTimeoutMs: 5_000,
          backoff: { baseMs: 10, maxMs: 40 },
        });
      });
      try {
        for (const w of workers) w.start();
        // The handler returns before the worker records the completion: wait on Postgres.
        await waitFor(async () => {
          const { rows } = await db.pool.query<{ n: number }>(
            `SELECT count(*)::int AS n FROM jobs WHERE status = 'succeeded'`,
          );
          return rows[0]!.n === 24;
        });
        expect(done).toBe(24);
        expect(peak).toBeLessThanOrEqual(2);
        expect(peak).toBeGreaterThan(0);
      } finally {
        await Promise.all(workers.map((w) => w.stop({ timeoutMs: 2_000 })));
        await Promise.all(pools.map((p) => p.end()));
      }
    });
  });

  it('exposes queueCapped only for capped queues', async () => {
    await queue('plain', null, 1);
    const [job] = (await lease(['plain'])) as [LeasedJob];
    expect(job.queueCapped).toBe(false);
  });
});
