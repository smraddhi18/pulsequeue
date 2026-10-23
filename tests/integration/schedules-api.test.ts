import { randomUUID } from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import { ScheduleRepository } from '../../src/repositories/schedule.repository.js';
import { createTestDatabase, describeWithDb, testEnv, type TestDatabase } from './helpers.js';

interface ScheduleBody {
  id: string;
  queue: string;
  name: string;
  cron: string;
  timezone: string;
  payload: unknown;
  priority: number;
  maxAttempts: number;
  misfirePolicy: string;
  enabled: boolean;
  nextRunAt: string | null;
  lastRunAt: string | null;
  lastJobId: string | null;
  fireCount: number;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

interface ErrorBody {
  error: { code: string; message: string; details?: unknown };
}

const MAX_PAYLOAD_BYTES = 1024;

describeWithDb('schedules API (Postgres)', () => {
  let db: TestDatabase;
  let app: FastifyInstance;

  beforeAll(async () => {
    db = await createTestDatabase();
    app = await buildApp({
      env: testEnv({ JOB_MAX_PAYLOAD_BYTES: String(MAX_PAYLOAD_BYTES) }),
      logger: false,
      pool: db.pool,
    });
    await app.ready();
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  async function create(queue: string, body: Record<string, unknown>) {
    return app.inject({ method: 'POST', url: `/queues/${queue}/schedules`, payload: body });
  }

  describe('POST /queues/:name/schedules', () => {
    it('creates a schedule with defaults and a first occurrence in the future', async () => {
      const before = Date.now();
      const res = await create('reports', {
        name: 'every-minute',
        cron: '* * * * *',
        payload: { kind: 'tick' },
      });
      expect(res.statusCode).toBe(201);
      const body = res.json<ScheduleBody>();
      expect(res.headers.location).toBe(`/schedules/${body.id}`);
      expect(body).toMatchObject({
        queue: 'reports',
        name: 'every-minute',
        cron: '* * * * *',
        timezone: 'UTC',
        payload: { kind: 'tick' },
        priority: 0,
        maxAttempts: 3,
        misfirePolicy: 'fire_once',
        enabled: true,
        lastRunAt: null,
        lastJobId: null,
        fireCount: 0,
        lastError: null,
      });
      const next = Date.parse(body.nextRunAt ?? '');
      expect(next).toBeGreaterThan(before - 1000);
      expect(next).toBeLessThanOrEqual(before + 61_000);
      expect(new Date(next).getUTCSeconds()).toBe(0);
    });

    it('evaluates the expression in the given time zone', async () => {
      const res = await create('reports', {
        name: 'ist-morning',
        cron: '30 9 * * *',
        timezone: 'Asia/Kolkata',
        payload: {},
        misfirePolicy: 'skip',
      });
      expect(res.statusCode).toBe(201);
      const body = res.json<ScheduleBody>();
      expect(body.timezone).toBe('Asia/Kolkata');
      // 09:30 IST is always 04:00 UTC (no DST in India).
      expect(body.nextRunAt).toMatch(/T04:00:00\.000Z$/);
    });

    it('creates a paused schedule when enabled is false', async () => {
      const res = await create('reports', {
        name: 'paused',
        cron: '@daily',
        payload: {},
        enabled: false,
      });
      expect(res.json<ScheduleBody>()).toMatchObject({ enabled: false, nextRunAt: null });
    });

    it('returns field-level 400s for bad cron and time zone', async () => {
      const bad = await create('reports', { name: 'bad', cron: '* * *', payload: {} });
      expect(bad.statusCode).toBe(400);
      expect(bad.json<ErrorBody>().error).toMatchObject({
        code: 'VALIDATION_ERROR',
        message: 'Validation failed',
        details: { fieldErrors: { cron: [expect.stringMatching(/Invalid cron/)] } },
      });

      const tz = await create('reports', {
        name: 'bad',
        cron: '0 * * * *',
        timezone: 'Not/AZone',
        payload: {},
      });
      expect(tz.statusCode).toBe(400);
      expect(tz.json<ErrorBody>().error.details).toMatchObject({
        fieldErrors: { timezone: [expect.stringMatching(/Unknown time zone/)] },
      });
    });

    it('409s on a duplicate name in the same queue but allows it in another queue', async () => {
      const body = { name: 'dup', cron: '@hourly', payload: {} };
      expect((await create('a-queue', body)).statusCode).toBe(201);
      const dup = await create('a-queue', body);
      expect(dup.statusCode).toBe(409);
      expect(dup.json<ErrorBody>().error.code).toBe('CONFLICT');
      expect((await create('b-queue', body)).statusCode).toBe(201);
    });

    it('413s when the payload exceeds the job payload budget', async () => {
      const res = await create('reports', {
        name: 'big',
        cron: '@hourly',
        payload: { blob: 'x'.repeat(MAX_PAYLOAD_BYTES) },
      });
      expect(res.statusCode).toBe(413);
    });
  });

  describe('list, get, pause, resume, delete', () => {
    it('pages through a queue newest first with an enabled filter', async () => {
      const queue = `list-${randomUUID().slice(0, 8)}`;
      const ids: string[] = [];
      for (let i = 0; i < 5; i++) {
        const res = await create(queue, {
          name: `s${i}`,
          cron: '@hourly',
          payload: { i },
          enabled: i % 2 === 0,
        });
        ids.push(res.json<ScheduleBody>().id);
      }

      const seen: string[] = [];
      let cursor: string | null = null;
      do {
        const url: string = `/queues/${queue}/schedules?limit=2${cursor ? `&cursor=${cursor}` : ''}`;
        const res = await app.inject({ method: 'GET', url });
        expect(res.statusCode).toBe(200);
        const page = res.json<{ data: ScheduleBody[]; nextCursor: string | null }>();
        seen.push(...page.data.map((s) => s.id));
        cursor = page.nextCursor;
      } while (cursor);
      expect(seen).toEqual([...ids].reverse());

      const paused = await app.inject({
        method: 'GET',
        url: `/queues/${queue}/schedules?enabled=false`,
      });
      expect(paused.json<{ data: ScheduleBody[] }>().data.map((s) => s.name)).toEqual(['s3', 's1']);
    });

    it('404s for an unknown queue and 400s for a bad cursor', async () => {
      const missing = await app.inject({ method: 'GET', url: '/queues/nope-q/schedules' });
      expect(missing.statusCode).toBe(404);
      const badCursor = await app.inject({
        method: 'GET',
        url: '/queues/reports/schedules?cursor=garbage',
      });
      expect(badCursor.statusCode).toBe(400);
    });

    it('pauses and resumes idempotently, then deletes', async () => {
      const { id } = (
        await create('lifecycle', { name: 'life', cron: '*/5 * * * *', payload: {} })
      ).json<ScheduleBody>();

      // Simulate an earlier failure so resume visibly clears it.
      await db.pool.query(`UPDATE schedules SET last_error = 'boom' WHERE id = $1`, [id]);

      for (let i = 0; i < 2; i++) {
        const res = await app.inject({ method: 'POST', url: `/schedules/${id}/pause` });
        expect(res.statusCode).toBe(200);
        expect(res.json<ScheduleBody>()).toMatchObject({ enabled: false, nextRunAt: null });
      }
      for (let i = 0; i < 2; i++) {
        const res = await app.inject({ method: 'POST', url: `/schedules/${id}/resume` });
        expect(res.statusCode).toBe(200);
        const body = res.json<ScheduleBody>();
        expect(body).toMatchObject({ enabled: true, lastError: null });
        expect(new Date(body.nextRunAt ?? '').getUTCMinutes() % 5).toBe(0);
      }

      const got = await app.inject({ method: 'GET', url: `/schedules/${id}` });
      expect(got.json<ScheduleBody>().id).toBe(id);

      expect((await app.inject({ method: 'DELETE', url: `/schedules/${id}` })).statusCode).toBe(
        204,
      );
      expect((await app.inject({ method: 'DELETE', url: `/schedules/${id}` })).statusCode).toBe(
        404,
      );
      expect((await app.inject({ method: 'GET', url: `/schedules/${id}` })).statusCode).toBe(404);
    });

    it('400s for a non-UUID id and 404s for an unknown one', async () => {
      expect((await app.inject({ method: 'GET', url: '/schedules/x' })).statusCode).toBe(400);
      const res = await app.inject({ method: 'POST', url: `/schedules/${randomUUID()}/pause` });
      expect(res.statusCode).toBe(404);
    });
  });

  describe('ScheduleRepository guards', () => {
    it('the database rejects an enabled schedule without a next run', async () => {
      const { id } = (
        await create('guards', { name: 'g', cron: '@hourly', payload: {} })
      ).json<ScheduleBody>();
      await expect(
        db.pool.query(`UPDATE schedules SET next_run_at = NULL WHERE id = $1`, [id]),
      ).rejects.toMatchObject({ constraint: 'schedules_enabled_has_next_run' });
    });

    it('claimDue only returns enabled, due schedules and skips rows locked elsewhere', async () => {
      const queue = `claim-${randomUUID().slice(0, 8)}`;
      const due = (
        await create(queue, { name: 'due', cron: '@hourly', payload: {} })
      ).json<ScheduleBody>();
      const locked = (
        await create(queue, { name: 'locked', cron: '@hourly', payload: {} })
      ).json<ScheduleBody>();
      await create(queue, { name: 'future', cron: '@hourly', payload: {} });
      await create(queue, { name: 'off', cron: '@hourly', payload: {}, enabled: false });
      await db.pool.query(
        `UPDATE schedules SET next_run_at = now() - interval '1 minute' WHERE id = ANY($1)`,
        [[due.id, locked.id]],
      );

      const other = await db.pool.connect();
      const mine = await db.pool.connect();
      try {
        await other.query('BEGIN');
        await other.query('SELECT 1 FROM schedules WHERE id = $1 FOR UPDATE', [locked.id]);
        await mine.query('BEGIN');
        const claimed = await new ScheduleRepository(mine).claimDue(100);
        const names = claimed.filter((s) => s.queue === queue).map((s) => s.name);
        expect(names).toEqual(['due']);
        expect(claimed.find((s) => s.name === 'due')?.queue).toBe(queue);
      } finally {
        await mine.query('ROLLBACK');
        await other.query('ROLLBACK');
        mine.release();
        other.release();
      }
    });
  });
});
