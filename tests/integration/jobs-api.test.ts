import { randomUUID } from 'node:crypto';
import type { FastifyInstance, LightMyRequestResponse } from 'fastify';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import { createTestDatabase, describeWithDb, testEnv, type TestDatabase } from './helpers.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';

interface JobBody {
  id: string;
  queue: string;
  status: string;
  priority: number;
  payload: unknown;
  attempts: number;
  maxAttempts: number;
  runAt: string;
  idempotencyKey: string | null;
  lastError: string | null;
  finishedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

interface PageBody {
  data: JobBody[];
  nextCursor: string | null;
}

interface ErrorBody {
  error: { code: string; message: string; details?: unknown };
}

const MAX_PAYLOAD_BYTES = 1024;

describeWithDb('jobs API (Postgres)', () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let api: Inject;

  beforeAll(async () => {
    db = await createTestDatabase();
    app = await buildApp({
      env: testEnv({ JOB_MAX_PAYLOAD_BYTES: String(MAX_PAYLOAD_BYTES) }),
      logger: false,
      pool: db.pool,
    });
    await app.ready();
    api = authedInject(app, (await createTestApiKey(db.pool)).token);
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  function enqueue(
    queue: string,
    body: unknown,
    idempotencyKey?: string,
  ): Promise<LightMyRequestResponse> {
    return api({
      method: 'POST',
      url: `/queues/${queue}/jobs`,
      payload: body as Record<string, unknown>,
      headers: idempotencyKey !== undefined ? { 'idempotency-key': idempotencyKey } : {},
    });
  }

  async function countJobs(where = 'TRUE', params: unknown[] = []): Promise<number> {
    const { rows } = await db.pool.query<{ n: string }>(
      `SELECT count(*) AS n FROM jobs WHERE ${where}`,
      params,
    );
    return Number(rows[0]?.n);
  }

  describe('GET /ready', () => {
    it('is ready when Postgres answers', async () => {
      const res = await app.inject({ method: 'GET', url: '/ready' });
      expect(res.statusCode).toBe(200);
      expect(res.json()).toMatchObject({ status: 'ready', checks: { app: 'ok', postgres: 'ok' } });
    });
  });

  describe('POST /queues/:name/jobs', () => {
    it('creates a queued job with defaults, auto-creates the queue, returns 201 + Location', async () => {
      const res = await enqueue('emails', { payload: { to: 'a@example.com' } });
      expect(res.statusCode).toBe(201);
      const job = res.json<JobBody>();
      expect(job).toMatchObject({
        queue: 'emails',
        status: 'queued',
        priority: 0,
        payload: { to: 'a@example.com' },
        attempts: 0,
        maxAttempts: 3,
        idempotencyKey: null,
        lastError: null,
        finishedAt: null,
      });
      expect(job.id).toMatch(/^[0-9a-f-]{36}$/);
      expect(res.headers.location).toBe(`/jobs/${job.id}`);
      expect(res.headers['idempotent-replayed']).toBeUndefined();
      expect(job).not.toHaveProperty('requestFingerprint');

      const { rows } = await db.pool.query("SELECT 1 FROM queues WHERE name = 'emails'");
      expect(rows).toHaveLength(1);
    });

    it('stores any JSON value as payload without driver mangling (arrays, strings, null)', async () => {
      for (const payload of [[1, 2, 3], 'just a string', null, { nested: [{ a: 1 }] }]) {
        const res = await enqueue('payload-shapes', { payload });
        expect(res.statusCode).toBe(201);
        const fetched = await api({ method: 'GET', url: `/jobs/${res.json<JobBody>().id}` });
        expect(fetched.json<JobBody>().payload).toEqual(payload);
      }
    });

    it('schedules delayed jobs using the database clock', async () => {
      const res = await enqueue('delayed', {
        payload: {},
        delayMs: 60_000,
        priority: 7,
        maxAttempts: 5,
      });
      expect(res.statusCode).toBe(201);
      const job = res.json<JobBody>();
      expect(job).toMatchObject({ status: 'scheduled', priority: 7, maxAttempts: 5 });
      const delta = new Date(job.runAt).getTime() - new Date(job.createdAt).getTime();
      expect(delta).toBeGreaterThanOrEqual(59_999);
      expect(delta).toBeLessThanOrEqual(60_001);
    });

    it('schedules a job at an absolute runAt (offset honoured, millisecond precision)', async () => {
      const target = new Date(Date.now() + 10 * 60_000);
      target.setUTCMilliseconds(123);
      // Same instant written in IST.
      const ist = new Date(target.getTime() + 5.5 * 3_600_000).toISOString().replace('Z', '+05:30');
      const res = await enqueue('delayed', { payload: {}, runAt: ist });
      expect(res.statusCode).toBe(201);
      const job = res.json<JobBody>();
      expect(job.status).toBe('scheduled');
      expect(new Date(job.runAt).getTime()).toBe(target.getTime());
    });

    it('treats a runAt in the past as "now" (clamped to the database clock)', async () => {
      const res = await enqueue('delayed', { payload: {}, runAt: '2001-01-01T00:00:00Z' });
      expect(res.statusCode).toBe(201);
      const job = res.json<JobBody>();
      expect(job.status).toBe('queued');
      expect(new Date(job.runAt).getTime()).toBe(new Date(job.createdAt).getTime());
    });

    it('rejects delayMs + runAt and a runAt beyond 30 days with field-level 400s', async () => {
      const both = await enqueue('validation', {
        payload: {},
        delayMs: 1000,
        runAt: new Date(Date.now() + 60_000).toISOString(),
      });
      expect(both.statusCode).toBe(400);
      expect(both.json<ErrorBody>().error).toMatchObject({
        code: 'VALIDATION_ERROR',
        details: { fieldErrors: { runAt: ['Specify either delayMs or runAt, not both'] } },
      });

      const far = await enqueue('validation', {
        payload: {},
        runAt: new Date(Date.now() + 31 * 24 * 3_600_000).toISOString(),
      });
      expect(far.statusCode).toBe(400);
      expect(far.json<ErrorBody>().error).toMatchObject({
        code: 'VALIDATION_ERROR',
        details: { formErrors: [], fieldErrors: { runAt: [expect.stringContaining('30 days')] } },
      });
      expect(await countJobs(`queue_id IN (SELECT id FROM queues WHERE name = 'validation')`)).toBe(
        0,
      );
    });

    it.each([
      ['priority above range', { payload: {}, priority: 101 }],
      ['maxAttempts zero', { payload: {}, maxAttempts: 0 }],
      ['negative delay', { payload: {}, delayMs: -5 }],
      ['unknown field', { payload: {}, delay: 5 }],
      ['missing payload', { priority: 1 }],
      ['runAt without offset', { payload: {}, runAt: '2026-10-23T09:00:00' }],
    ])('returns 400 VALIDATION_ERROR for %s', async (_label, body) => {
      const res = await enqueue('validation', body);
      expect(res.statusCode).toBe(400);
      expect(res.json<ErrorBody>().error.code).toBe('VALIDATION_ERROR');
    });

    it('returns 400 for an invalid queue name and creates nothing', async () => {
      const res = await enqueue('Not_Valid!', { payload: {} });
      expect(res.statusCode).toBe(400);
      const { rows } = await db.pool.query("SELECT 1 FROM queues WHERE name = 'Not_Valid!'");
      expect(rows).toHaveLength(0);
    });

    it('returns 400 for a malformed Idempotency-Key', async () => {
      const res = await enqueue('validation', { payload: {} }, 'has spaces in it');
      expect(res.statusCode).toBe(400);
    });

    it('returns 413 PAYLOAD_TOO_LARGE above the payload budget', async () => {
      const res = await enqueue('big', { payload: { blob: 'x'.repeat(MAX_PAYLOAD_BYTES) } });
      expect(res.statusCode).toBe(413);
      const body = res.json<ErrorBody>();
      expect(body.error.code).toBe('PAYLOAD_TOO_LARGE');
      expect(body.error.details).toMatchObject({ limitBytes: MAX_PAYLOAD_BYTES });
    });

    it('returns 413 when the raw body exceeds the transport cap', async () => {
      const res = await api({
        method: 'POST',
        url: '/queues/big/jobs',
        headers: { 'content-type': 'application/json' },
        payload: JSON.stringify({ payload: 'x'.repeat(MAX_PAYLOAD_BYTES * 2 + 70 * 1024) }),
      });
      expect(res.statusCode).toBe(413);
      expect(res.json<ErrorBody>().error.code).toBe('PAYLOAD_TOO_LARGE');
    });

    it('maps values Postgres cannot store (NUL in jsonb) to 400, not 500', async () => {
      const res = await enqueue('validation', { payload: { s: 'nul\u0000byte' } });
      expect(res.statusCode).toBe(400);
      expect(res.json<ErrorBody>().error.code).toBe('VALIDATION_ERROR');
    });
  });

  describe('idempotency', () => {
    it('replays the original job for the same key and equivalent body (200 + Idempotent-Replayed)', async () => {
      const key = `order-${randomUUID()}`;
      const first = await enqueue('payments', { payload: { amount: 10, currency: 'INR' } }, key);
      expect(first.statusCode).toBe(201);

      // Different key order and an explicit default are the same request.
      const replay = await enqueue(
        'payments',
        { priority: 0, payload: { currency: 'INR', amount: 10 } },
        key,
      );
      expect(replay.statusCode).toBe(200);
      expect(replay.headers['idempotent-replayed']).toBe('true');
      expect(replay.json<JobBody>().id).toBe(first.json<JobBody>().id);
      expect(replay.json<JobBody>().idempotencyKey).toBe(key);
      expect(await countJobs('idempotency_key = $1', [key])).toBe(1);
    });

    it('returns 422 IDEMPOTENCY_KEY_REUSED for the same key with a different body', async () => {
      const key = `order-${randomUUID()}`;
      await enqueue('payments', { payload: { amount: 10 } }, key);

      const res = await enqueue('payments', { payload: { amount: 99 } }, key);
      expect(res.statusCode).toBe(422);
      const body = res.json<ErrorBody>();
      expect(body.error.code).toBe('IDEMPOTENCY_KEY_REUSED');
      expect(await countJobs('idempotency_key = $1', [key])).toBe(1);
    });

    it('scopes keys per queue', async () => {
      const key = `shared-${randomUUID()}`;
      const a = await enqueue('queue-a', { payload: {} }, key);
      const b = await enqueue('queue-b', { payload: {} }, key);
      expect(a.statusCode).toBe(201);
      expect(b.statusCode).toBe(201);
      expect(a.json<JobBody>().id).not.toBe(b.json<JobBody>().id);
    });

    it('is race-safe: 25 concurrent requests with one key create exactly one job', async () => {
      const key = `race-${randomUUID()}`;
      const responses = await Promise.all(
        Array.from({ length: 25 }, () => enqueue('race', { payload: { n: 1 } }, key)),
      );

      const statuses = responses.map((r) => r.statusCode).sort();
      expect(statuses.filter((s) => s === 201)).toHaveLength(1);
      expect(statuses.filter((s) => s === 200)).toHaveLength(24);
      expect(new Set(responses.map((r) => r.json<JobBody>().id)).size).toBe(1);
      expect(await countJobs('idempotency_key = $1', [key])).toBe(1);
    });

    it('is race-safe for queue auto-creation: concurrent first enqueues create one queue', async () => {
      const queue = `fresh-${randomUUID().slice(0, 8)}`;
      const responses = await Promise.all(
        Array.from({ length: 20 }, (_, i) => enqueue(queue, { payload: { i } })),
      );
      expect(responses.every((r) => r.statusCode === 201)).toBe(true);
      const { rows } = await db.pool.query('SELECT id FROM queues WHERE name = $1', [queue]);
      expect(rows).toHaveLength(1);
    });
  });

  describe('GET /jobs/:id', () => {
    it('returns the job', async () => {
      const created = (await enqueue('lookup', { payload: { x: 1 } })).json<JobBody>();
      const res = await api({ method: 'GET', url: `/jobs/${created.id}` });
      expect(res.statusCode).toBe(200);
      expect(res.json()).toEqual(created);
    });

    it('returns 404 for an unknown id and 400 for a non-UUID', async () => {
      const missing = await api({ method: 'GET', url: `/jobs/${randomUUID()}` });
      expect(missing.statusCode).toBe(404);
      expect(missing.json<ErrorBody>().error.code).toBe('NOT_FOUND');

      const bad = await api({ method: 'GET', url: '/jobs/not-a-uuid' });
      expect(bad.statusCode).toBe(400);
    });
  });

  describe('GET /queues/:name/jobs', () => {
    const queue = 'paging';
    const createdIds: string[] = [];

    beforeAll(async () => {
      for (let i = 0; i < 25; i++) {
        const res = await enqueue(queue, {
          payload: { i },
          ...(i % 5 === 0 ? { delayMs: 60_000 } : {}),
        });
        createdIds.push(res.json<JobBody>().id);
      }
    });

    async function page(query: string): Promise<LightMyRequestResponse> {
      return api({ method: 'GET', url: `/queues/${queue}/jobs${query}` });
    }

    async function collectAll(baseQuery: string): Promise<{ ids: string[]; pages: number[] }> {
      const ids: string[] = [];
      const pages: number[] = [];
      let cursor: string | null = null;
      do {
        const sep = baseQuery ? '&' : '?';
        const suffix: string = cursor ? `${sep}cursor=${encodeURIComponent(cursor)}` : '';
        const res = await page(`${baseQuery}${suffix}`);
        expect(res.statusCode).toBe(200);
        const body = res.json<PageBody>();
        ids.push(...body.data.map((j) => j.id));
        pages.push(body.data.length);
        cursor = body.nextCursor;
      } while (cursor);
      return { ids, pages };
    }

    it('walks every job exactly once, newest first, with nextCursor null on the last page', async () => {
      const { ids, pages } = await collectAll('?limit=10');
      expect(pages).toEqual([10, 10, 5]);
      expect(new Set(ids).size).toBe(25);
      expect(ids).toEqual([...createdIds].reverse());
    });

    it('defaults the limit and filters by status', async () => {
      const all = await page('');
      expect(all.json<PageBody>().data).toHaveLength(25);
      expect(all.json<PageBody>().nextCursor).toBeNull();

      const { ids } = await collectAll('?status=scheduled&limit=2');
      expect(ids).toHaveLength(5);
      const scheduled = await page('?status=scheduled');
      expect(scheduled.json<PageBody>().data.every((j) => j.status === 'scheduled')).toBe(true);
    });

    it('stays stable while new jobs are inserted between pages', async () => {
      const first = (await page('?limit=10')).json<PageBody>();
      await enqueue(queue, { payload: { late: true } });
      const second = (
        await page(`?limit=10&cursor=${encodeURIComponent(first.nextCursor!)}`)
      ).json<PageBody>();

      const firstIds = new Set(first.data.map((j) => j.id));
      expect(second.data.some((j) => firstIds.has(j.id))).toBe(false);
      expect(second.data.map((j) => j.id)).toEqual([...createdIds].reverse().slice(10, 20));
    });

    it('returns 404 for an unknown queue without creating it', async () => {
      const res = await api({ method: 'GET', url: '/queues/never-created/jobs' });
      expect(res.statusCode).toBe(404);
      const { rows } = await db.pool.query("SELECT 1 FROM queues WHERE name = 'never-created'");
      expect(rows).toHaveLength(0);
    });

    it.each([
      ['an invalid cursor', '?cursor=garbage!'],
      ['a limit above the max', '?limit=201'],
      ['an unknown status', '?status=pending'],
      ['an unknown parameter', '?offset=10'],
    ])('returns 400 for %s', async (_label, query) => {
      const res = await page(query);
      expect(res.statusCode).toBe(400);
      expect(res.json<ErrorBody>().error.code).toBe('VALIDATION_ERROR');
    });
  });

  describe('keyset precision', () => {
    it('never skips or repeats rows that share a timestamp or differ by 1 microsecond', async () => {
      const queue = 'precision';
      const { rows } = await db.pool.query<{ id: string }>(
        'INSERT INTO queues (name) VALUES ($1) RETURNING id',
        [queue],
      );
      const queueId = rows[0]!.id;
      // 4 rows with an identical created_at (tie broken by id), plus rows 1µs apart within one ms.
      await db.pool.query(
        `INSERT INTO jobs (queue_id, payload, created_at)
         SELECT $1, '{}'::jsonb, ts
         FROM unnest(ARRAY[
           '2026-10-20T00:00:00.000500Z', '2026-10-20T00:00:00.000500Z',
           '2026-10-20T00:00:00.000500Z', '2026-10-20T00:00:00.000500Z',
           '2026-10-20T00:00:00.000501Z', '2026-10-20T00:00:00.000502Z',
           '2026-10-20T00:00:00.000999Z', '2026-10-20T00:00:00.001000Z'
         ]::timestamptz[]) AS ts`,
        [queueId],
      );

      const { rows: expected } = await db.pool.query<{ id: string }>(
        'SELECT id FROM jobs WHERE queue_id = $1 ORDER BY created_at DESC, id DESC',
        [queueId],
      );

      const seen: string[] = [];
      let cursor: string | null = null;
      do {
        const suffix: string = cursor ? `&cursor=${encodeURIComponent(cursor)}` : '';
        const res = await api({
          method: 'GET',
          url: `/queues/${queue}/jobs?limit=1${suffix}`,
        });
        const body = res.json<PageBody>();
        seen.push(...body.data.map((j) => j.id));
        cursor = body.nextCursor;
      } while (cursor);

      expect(seen).toEqual(expected.map((r) => r.id));
    });
  });
});
