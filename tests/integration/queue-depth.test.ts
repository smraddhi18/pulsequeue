import pino from 'pino';
import { Registry } from 'prom-client';
import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import type { Pool } from '../../src/db/pool.js';
import {
  QueueDepthCollector,
  type QueueDepthOptions,
} from '../../src/observability/queue-depth.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

const logger = pino({ level: 'silent' });

describeWithDb('QueueDepthCollector (Postgres)', () => {
  let db: TestDatabase;
  let clock = 0;

  beforeAll(async () => {
    db = await createTestDatabase();
  });

  afterAll(async () => {
    await db?.destroy();
  });

  beforeEach(async () => {
    clock = 0;
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
  });

  function collector(overrides: Partial<QueueDepthOptions> = {}) {
    const registry = new Registry();
    const c = new QueueDepthCollector({
      pool: db.pool,
      registry,
      logger,
      maxQueues: 100,
      cacheMs: 1_000,
      timeoutMs: 2_000,
      now: () => clock,
      ...overrides,
    });
    const errors = async () =>
      (await registry.getSingleMetric('pulsequeue_queue_depth_collect_errors_total')!.get())
        .values[0]?.value ?? 0;
    return { c, registry, errors };
  }

  async function seed(queue: string, queued: number, scheduled = 0): Promise<void> {
    const { rows } = await db.pool.query<{ id: string }>(
      'INSERT INTO queues (name) VALUES ($1) ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name RETURNING id',
      [queue],
    );
    await db.pool.query(
      `INSERT INTO jobs (queue_id, payload) SELECT $1, '{}' FROM generate_series(1, $2::int)`,
      [rows[0]!.id, queued],
    );
    await db.pool.query(
      `INSERT INTO jobs (queue_id, payload, status, run_at)
       SELECT $1, '{}', 'scheduled', now() + interval '1 hour' FROM generate_series(1, $2::int)`,
      [rows[0]!.id, scheduled],
    );
  }

  const depth = (rows: Awaited<ReturnType<QueueDepthCollector['read']>>, queue: string) =>
    Object.fromEntries(rows.filter((r) => r.queue === queue).map((r) => [r.status, r.count]));

  it('reports every status per queue, zeros included', async () => {
    await seed('a', 3, 2);
    await seed('idle', 0);
    const { c } = collector();
    const rows = await c.read();
    expect(depth(rows, 'a')).toEqual({ queued: 3, scheduled: 2, running: 0, dead: 0 });
    expect(depth(rows, 'idle')).toEqual({ queued: 0, scheduled: 0, running: 0, dead: 0 });
    expect(rows.every((r) => r.tenantId === 'default')).toBe(true);
  });

  it('serves the cached snapshot until cacheMs has passed', async () => {
    await seed('a', 1);
    const { c } = collector({ cacheMs: 1_000 });
    expect(depth(await c.read(), 'a').queued).toBe(1);
    await seed('a', 4);
    clock = 999;
    expect(depth(await c.read(), 'a').queued).toBe(1);
    clock = 1_000;
    expect(depth(await c.read(), 'a').queued).toBe(5);
  });

  it('shares one query between concurrent scrapes', async () => {
    await seed('a', 1);
    let connects = 0;
    const counting = {
      connect: () => {
        connects++;
        return db.pool.connect();
      },
    } as unknown as Pool;
    const { c } = collector({ pool: counting, cacheMs: 0 });
    await Promise.all([c.read(), c.read(), c.read()]);
    expect(connects).toBe(1);
  });

  it('bounds cost to maxQueues queues', async () => {
    for (const q of ['q1', 'q2', 'q3']) await seed(q, 1);
    const { c } = collector({ maxQueues: 2 });
    const queues = new Set((await c.read()).map((r) => r.queue));
    expect([...queues].sort()).toEqual(['q1', 'q2']);
  });

  it('keeps the last snapshot and counts the error when the query times out', async () => {
    await seed('a', 2);
    const { c, errors } = collector({ cacheMs: 0, timeoutMs: 50 });
    expect(depth(await c.read(), 'a').queued).toBe(2);

    const blocker = await db.pool.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query('LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE');
      const started = Date.now();
      expect(depth(await c.read(), 'a').queued).toBe(2);
      expect(Date.now() - started).toBeLessThan(1_500);
      expect(await errors()).toBe(1);
    } finally {
      await blocker.query('ROLLBACK');
      blocker.release();
    }
    // Recovers on the next scrape; the pool was not poisoned.
    await seed('a', 1);
    expect(depth(await c.read(), 'a').queued).toBe(3);
  });

  it('never fails the scrape when the database is unreachable', async () => {
    const down = {
      connect: () => Promise.reject(new Error('connection refused')),
    } as unknown as Pool;
    const { c, registry, errors } = collector({ pool: down });
    expect(await c.read()).toEqual([]);
    expect(await errors()).toBe(1);
    // A scrape during the outage still succeeds; it just has no depth series.
    const text = await registry.metrics();
    expect(text).toContain('pulsequeue_queue_depth_collect_errors_total');
    expect(text).not.toMatch(/^pulsequeue_queue_depth\{/m);
    expect(await errors()).toBe(2);
  });
});
