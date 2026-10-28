import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import { createTestDatabase, describeWithDb, testEnv, type TestDatabase } from './helpers.js';
import { authedInject, createTestApiKey, ensureTestTenant, type Inject } from './auth-helpers.js';

interface Throughput {
  queue: string;
  from: string;
  to: string;
  bucketMs: number;
  buckets: Array<{
    start: string;
    enqueued: number;
    succeeded: number;
    failed: number;
    timedOut: number;
    deadLettered: number;
  }>;
  totals: Record<string, number>;
}

interface LatencyStats {
  samples: number;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  max: number | null;
}

interface Latency {
  unit: string;
  executionMs: LatencyStats;
  queueWaitMs: LatencyStats;
  truncated: boolean;
  sampleLimit: number;
}

const MINUTE = 60_000;

/**
 * Dashboard API against real Postgres. Data is seeded with explicit
 * timestamps relative to the database clock, so buckets, windows and
 * percentiles have exact expected values.
 */
describeWithDb('dashboard API (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let reader: Inject;
  let otherTenant: Inject;
  let dbNow: Date;

  beforeAll(async () => {
    db = await createTestDatabase();
    await ensureTestTenant(db.pool, 'acme');
    await ensureTestTenant(db.pool, 'rival');
    app = await buildApp({
      env: testEnv({ DASHBOARD_LATENCY_MAX_SAMPLES: '1000' }),
      logger: false,
      pool: db.pool,
    });
    await app.ready();
    reader = authedInject(
      app,
      (await createTestApiKey(db.pool, { tenantId: 'acme', role: 'reader' })).token,
    );
    otherTenant = authedInject(
      app,
      (await createTestApiKey(db.pool, { tenantId: 'rival', role: 'reader' })).token,
    );
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
    const { rows } = await db.pool.query<{ now: Date }>('SELECT now() AS now');
    dbNow = rows[0]!.now;
  });

  const ago = (ms: number) => new Date(dbNow.getTime() - ms);

  async function queue(name: string, tenant = 'acme', maxConcurrency: number | null = null) {
    const { rows } = await db.pool.query<{ id: string }>(
      'INSERT INTO queues (tenant_id, name, max_concurrency) VALUES ($1, $2, $3) RETURNING id',
      [tenant, name, maxConcurrency],
    );
    return rows[0]!.id;
  }

  async function job(
    queueId: string,
    opts: { status?: string; createdAt?: Date; runAt?: Date; deadAt?: Date } = {},
  ): Promise<string> {
    const status = opts.status ?? 'queued';
    const createdAt = opts.createdAt ?? dbNow;
    const { rows } = await db.pool.query<{ id: string }>(
      `INSERT INTO jobs (queue_id, payload, status, created_at, run_at, dead_reason, dead_lettered_at,
                         attempts, max_attempts, locked_by, lease_token, locked_at, lease_expires_at)
       SELECT $1, '{}', $2::job_status, $3, $4, $5, $6, $7, 3, r.w, r.t, r.at, r.exp
       FROM (SELECT CASE WHEN $2 = 'running' THEN 'w' END AS w,
                    CASE WHEN $2 = 'running' THEN gen_random_uuid() END AS t,
                    CASE WHEN $2 = 'running' THEN now() END AS at,
                    CASE WHEN $2 = 'running' THEN now() + interval '1 minute' END AS exp) r
       RETURNING id`,
      [
        queueId,
        status,
        createdAt,
        opts.runAt ?? createdAt,
        status === 'dead' ? 'attempts_exhausted' : null,
        status === 'dead' ? (opts.deadAt ?? dbNow) : null,
        status === 'dead' ? 3 : 0,
      ],
    );
    return rows[0]!.id;
  }

  let attemptSeq = 0;
  async function attempt(
    queueId: string,
    jobId: string,
    opts: { status: string; finishedAt: Date; execMs: number; waitMs?: number | null },
  ) {
    const startedAt = new Date(opts.finishedAt.getTime() - opts.execMs);
    const readyAt =
      opts.waitMs === null ? null : new Date(startedAt.getTime() - (opts.waitMs ?? 0));
    await db.pool.query(
      `INSERT INTO job_attempts (job_id, attempt_number, status, worker_id, started_at, finished_at,
                                 queue_id, ready_at)
       VALUES ($1, $2, $3, 'w', $4, $5, $6, $7)`,
      [jobId, ++attemptSeq, opts.status, startedAt, opts.finishedAt, queueId, readyAt],
    );
  }

  describe('access', () => {
    it('requires an API key (deny by default)', async () => {
      const res = await app.inject({ method: 'GET', url: '/dashboard/queues' });
      expect(res.statusCode).toBe(401);
    });

    it('is tenant-scoped: another tenant gets 404 for the queue and an empty list', async () => {
      await queue('emails');
      for (const path of ['throughput', 'latency', 'failure-rate']) {
        const res = await otherTenant({
          method: 'GET',
          url: `/dashboard/queues/emails/${path}`,
        });
        expect(res.statusCode).toBe(404);
        expect(res.json()).toMatchObject({ error: { code: 'NOT_FOUND' } });
      }
      const list = await otherTenant({ method: 'GET', url: '/dashboard/queues' });
      expect(list.json<{ data: unknown[] }>().data).toEqual([]);
    });
  });

  describe('GET /dashboard/queues', () => {
    it('lists depth per status and the oldest queued job, paginated by name', async () => {
      const emails = await queue('emails', 'acme', 5);
      const reports = await queue('reports');
      await queue('other-tenant-queue', 'rival');
      await job(emails, { createdAt: ago(10 * MINUTE) });
      await job(emails);
      await job(emails, { status: 'scheduled', runAt: new Date(dbNow.getTime() + MINUTE) });
      await job(emails, { status: 'dead' });
      await job(reports, { status: 'running' });

      const first = await reader({ method: 'GET', url: '/dashboard/queues?limit=1' });
      expect(first.statusCode).toBe(200);
      expect(first.headers['cache-control']).toBe('no-store');
      const page = first.json<{
        data: Array<Record<string, unknown>>;
        nextAfter: string | null;
      }>();
      expect(page.data).toHaveLength(1);
      expect(page.data[0]).toMatchObject({
        queue: 'emails',
        counts: { queued: 2, scheduled: 1, running: 0, dead: 1 },
        maxConcurrency: 5,
        oldestQueuedAt: ago(10 * MINUTE).toISOString(),
      });
      expect(page.data[0]!.oldestQueuedAgeMs).toBeGreaterThanOrEqual(10 * MINUTE);
      expect(page.nextAfter).toBe('emails');

      const second = await reader({ method: 'GET', url: '/dashboard/queues?limit=1&after=emails' });
      const page2 = second.json<{
        data: Array<Record<string, unknown>>;
        nextAfter: string | null;
      }>();
      expect(page2.data).toMatchObject([
        { queue: 'reports', counts: { queued: 0, running: 1 }, oldestQueuedAt: null },
      ]);
      expect(page2.nextAfter).toBeNull();
    });

    it('validates query params', async () => {
      const res = await reader({ method: 'GET', url: '/dashboard/queues?limit=0' });
      expect(res.statusCode).toBe(400);
      expect(res.json()).toMatchObject({ error: { code: 'VALIDATION_ERROR' } });
    });
  });

  describe('GET /dashboard/queues/:name/throughput', () => {
    it('buckets enqueues, attempt outcomes and dead-letters on aligned boundaries', async () => {
      const q = await queue('emails');
      const events = {
        enqueuedRecent: ago(30_000),
        enqueuedOlder: ago(5 * MINUTE),
        finished: ago(2 * MINUTE),
        dead: ago(1 * MINUTE),
        outside: ago(30 * MINUTE),
      };
      for (let i = 0; i < 3; i++) await job(q, { createdAt: events.enqueuedRecent });
      for (let i = 0; i < 2; i++) await job(q, { createdAt: events.enqueuedOlder });
      const old = await job(q, { createdAt: events.outside, status: 'succeeded' });
      await attempt(q, old, { status: 'succeeded', finishedAt: events.outside, execMs: 5 });
      const worked = await job(q, {
        createdAt: events.outside,
        status: 'dead',
        deadAt: events.dead,
      });
      for (let i = 0; i < 4; i++) {
        await attempt(q, worked, { status: 'succeeded', finishedAt: events.finished, execMs: 5 });
      }
      await attempt(q, worked, { status: 'failed', finishedAt: events.finished, execMs: 5 });
      await attempt(q, worked, { status: 'timed_out', finishedAt: events.finished, execMs: 5 });

      const res = await reader({
        method: 'GET',
        url: '/dashboard/queues/emails/throughput?window=10m&bucket=1m',
      });
      expect(res.statusCode).toBe(200);
      const body = res.json<Throughput>();
      expect(body.bucketMs).toBe(MINUTE);
      expect(body.buckets).toHaveLength(10);
      // Aligned: every bucket starts on a whole minute, contiguous, ending after now.
      body.buckets.forEach((b, i) => {
        expect(Date.parse(b.start) % MINUTE).toBe(0);
        expect(Date.parse(b.start)).toBe(Date.parse(body.from) + i * MINUTE);
      });
      expect(Date.parse(body.to)).toBeGreaterThan(dbNow.getTime());
      expect(Date.parse(body.to) - Date.parse(body.from)).toBe(10 * MINUTE);

      const bucketOf = (d: Date) =>
        body.buckets.find(
          (b) => b.start === new Date(Math.floor(d.getTime() / MINUTE) * MINUTE).toISOString(),
        )!;
      expect(bucketOf(events.enqueuedRecent).enqueued).toBe(3);
      expect(bucketOf(events.enqueuedOlder).enqueued).toBe(2);
      expect(bucketOf(events.finished)).toMatchObject({ succeeded: 4, failed: 1, timedOut: 1 });
      expect(bucketOf(events.dead).deadLettered).toBe(1);
      // The 30-minute-old enqueue and attempt fall outside the window.
      expect(body.totals).toEqual({
        enqueued: 5,
        succeeded: 4,
        failed: 1,
        timedOut: 1,
        deadLettered: 1,
      });
    });

    it('returns zero-filled buckets for an idle queue', async () => {
      await queue('idle');
      const res = await reader({
        method: 'GET',
        url: '/dashboard/queues/idle/throughput?window=1h&bucket=15m',
      });
      const body = res.json<Throughput>();
      expect(body.buckets).toHaveLength(4);
      expect(body.totals).toEqual({
        enqueued: 0,
        succeeded: 0,
        failed: 0,
        timedOut: 0,
        deadLettered: 0,
      });
    });

    it.each([
      ['window=1d&bucket=10s', 400],
      ['window=1h&bucket=7m', 400],
      ['window=1y', 400],
      ['bucket=2h', 400],
      ['window=1h&unknown=1', 400],
    ])('rejects %s', async (qs, status) => {
      await queue('emails');
      const res = await reader({ method: 'GET', url: `/dashboard/queues/emails/throughput?${qs}` });
      expect(res.statusCode).toBe(status);
      expect(res.json()).toMatchObject({ error: { code: 'VALIDATION_ERROR' } });
    });

    it('404s an unknown queue', async () => {
      const res = await reader({ method: 'GET', url: '/dashboard/queues/nope/throughput' });
      expect(res.statusCode).toBe(404);
    });
  });

  describe('GET /dashboard/queues/:name/latency', () => {
    it('computes p50/p95/p99/max for execution time and queue wait', async () => {
      const q = await queue('emails');
      const j = await job(q, { status: 'succeeded' });
      for (let i = 1; i <= 100; i++) {
        await attempt(q, j, {
          status: i % 10 === 0 ? 'failed' : 'succeeded',
          finishedAt: ago(MINUTE + i),
          execMs: i,
          waitMs: i * 10,
        });
      }
      // Pre-0008 rows (no ready_at) count for execution but not for wait;
      // released attempts measure nothing and are ignored.
      await attempt(q, j, {
        status: 'succeeded',
        finishedAt: ago(MINUTE),
        execMs: 50,
        waitMs: null,
      });
      await attempt(q, j, { status: 'released', finishedAt: ago(MINUTE), execMs: 10_000 });

      const res = await reader({
        method: 'GET',
        url: '/dashboard/queues/emails/latency?window=1h',
      });
      expect(res.statusCode).toBe(200);
      const body = res.json<Latency>();
      expect(body.unit).toBe('ms');
      expect(body.truncated).toBe(false);
      expect(body.executionMs.samples).toBe(101);
      expect(body.executionMs.max).toBe(100);
      expect(body.executionMs.p50).toBeCloseTo(50, 0);
      expect(body.queueWaitMs).toEqual({
        samples: 100,
        p50: 505,
        p95: 950.5,
        p99: 990.1,
        max: 1000,
      });
    });

    it('flags truncation when the window has more attempts than the sample cap', async () => {
      const capped = await buildApp({
        env: testEnv({ DASHBOARD_LATENCY_MAX_SAMPLES: '10' }),
        logger: false,
        pool: db.pool,
      });
      try {
        const q = await queue('emails');
        const j = await job(q, { status: 'succeeded' });
        for (let i = 1; i <= 15; i++) {
          // Newest attempts have the smallest exec time: the cap keeps those.
          await attempt(q, j, { status: 'succeeded', finishedAt: ago(i * 1000), execMs: i });
        }
        const token = (await createTestApiKey(db.pool, { tenantId: 'acme', role: 'reader' })).token;
        const res = await authedInject(
          capped,
          token,
        )({
          method: 'GET',
          url: '/dashboard/queues/emails/latency',
        });
        const body = res.json<Latency>();
        expect(body.truncated).toBe(true);
        expect(body.sampleLimit).toBe(10);
        expect(body.executionMs).toMatchObject({ samples: 10, max: 10 });
      } finally {
        await capped.close();
      }
    });

    it('returns empty summaries for a queue with no finished attempts', async () => {
      await queue('idle');
      const body = (
        await reader({ method: 'GET', url: '/dashboard/queues/idle/latency' })
      ).json<Latency>();
      const empty = { samples: 0, p50: null, p95: null, p99: null, max: null };
      expect(body.executionMs).toEqual(empty);
      expect(body.queueWaitMs).toEqual(empty);
    });

    it('caps the window at 24h', async () => {
      await queue('emails');
      const res = await reader({
        method: 'GET',
        url: '/dashboard/queues/emails/latency?window=2d',
      });
      expect(res.statusCode).toBe(400);
    });
  });

  describe('GET /dashboard/queues/:name/failure-rate', () => {
    it('reports attempt and terminal job failure rates', async () => {
      const q = await queue('emails');
      const j = await job(q, { status: 'succeeded' });
      for (let i = 0; i < 6; i++) {
        await attempt(q, j, { status: 'succeeded', finishedAt: ago(MINUTE), execMs: 1 });
      }
      await attempt(q, j, { status: 'failed', finishedAt: ago(MINUTE), execMs: 1 });
      await attempt(q, j, { status: 'timed_out', finishedAt: ago(MINUTE), execMs: 1 });
      await job(q, { status: 'dead', deadAt: ago(MINUTE) });
      await job(q, { status: 'dead', deadAt: ago(MINUTE) });
      await job(q, { status: 'dead', deadAt: ago(3 * 3_600_000) }); // outside 1h

      const body = (
        await reader({ method: 'GET', url: '/dashboard/queues/emails/failure-rate?window=1h' })
      ).json<Record<string, unknown>>();
      expect(body).toMatchObject({
        queue: 'emails',
        attempts: { succeeded: 6, failed: 1, timedOut: 1, total: 8 },
        attemptFailureRate: 0.25,
        jobs: { succeeded: 6, deadLettered: 2 },
        jobFailureRate: 0.25,
      });
    });

    it('returns null rates with no data', async () => {
      await queue('idle');
      const body = (
        await reader({ method: 'GET', url: '/dashboard/queues/idle/failure-rate' })
      ).json<Record<string, unknown>>();
      expect(body).toMatchObject({ attemptFailureRate: null, jobFailureRate: null });
    });
  });
});
