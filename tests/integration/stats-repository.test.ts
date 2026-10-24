import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import type { QueueName } from '../../src/domain/job.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { QueueStatsRepository } from '../../src/repositories/stats.repository.js';
import { JobService } from '../../src/services/job.service.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

describeWithDb('QueueStatsRepository (Postgres)', () => {
  let db: TestDatabase;
  let stats: QueueStatsRepository;
  let leases: LeaseRepository;
  let jobs: JobService;

  beforeAll(async () => {
    db = await createTestDatabase();
    stats = new QueueStatsRepository(db.pool);
    leases = new LeaseRepository(db.pool);
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

  const enqueue = async (queue: string, delayMs?: number) =>
    (
      await jobs.enqueue({
        queue: queue as QueueName,
        payload: {},
        ...(delayMs !== undefined ? { delayMs } : {}),
      })
    ).job.id;

  it('returns null for an unknown queue', async () => {
    expect(await stats.compute('nope' as QueueName)).toBeNull();
  });

  it('counts every status (zeros included) and only the requested queue', async () => {
    const ids = [await enqueue('q'), await enqueue('q'), await enqueue('q'), await enqueue('q')];
    await enqueue('q', 60_000);
    await enqueue('other');
    // ids[0..1] running, then complete one and dead-letter another.
    const leased = await leases.lease({ queues: ['q'], workerId: 'w', limit: 2, leaseMs: 30_000 });
    expect(leased).toHaveLength(2);
    await leases.complete(leased[0]!);
    await leases.fail(leased[1]!, {
      error: 'boom',
      attemptStatus: 'failed',
      retryDelayMs: null,
      deadLetterReason: 'non_retryable',
    });
    const [running] = await leases.lease({
      queues: ['q'],
      workerId: 'w',
      limit: 1,
      leaseMs: 30_000,
    });
    expect(running).toBeDefined();

    const result = await stats.compute('q' as QueueName);
    expect(result).not.toBeNull();
    expect(result!.queue).toBe('q');
    expect(result!.counts).toEqual({ queued: 1, scheduled: 1, running: 1, succeeded: 1, dead: 1 });
    expect(result!.total).toBe(5);
    expect(ids).toHaveLength(4);
    expect((await stats.compute('other' as QueueName))!.counts.queued).toBe(1);
  });

  it('reports the oldest queued, next scheduled and oldest running times on the DB clock', async () => {
    await enqueue('empty', 60_000);
    const empty = await stats.compute('empty' as QueueName);
    expect(empty!.oldestQueuedAt).toBeNull();
    expect(empty!.oldestRunningSince).toBeNull();

    const oldest = await enqueue('q');
    await enqueue('q');
    await enqueue('q', 30_000);
    await db.pool.query(`UPDATE jobs SET run_at = now() - interval '90 seconds' WHERE id = $1`, [
      oldest,
    ]);
    await leases.lease({ queues: ['q'], workerId: 'w', limit: 1, leaseMs: 30_000 }); // takes `oldest`
    await db.pool.query(
      `UPDATE jobs SET run_at = now() - interval '10 seconds' WHERE status = 'queued'`,
    );

    const result = (await stats.compute('q' as QueueName))!;
    const at = result.computedAt.getTime();
    expect(at - result.oldestQueuedAt!.getTime()).toBeGreaterThanOrEqual(10_000);
    expect(at - result.oldestQueuedAt!.getTime()).toBeLessThan(15_000);
    expect(result.nextScheduledAt!.getTime() - at).toBeGreaterThan(25_000);
    expect(result.oldestRunningSince!.getTime()).toBeLessThanOrEqual(at);
  });
});
