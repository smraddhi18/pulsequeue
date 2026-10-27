import { randomUUID } from 'node:crypto';
import type { FastifyInstance, LightMyRequestResponse } from 'fastify';
import pino from 'pino';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import type { DeadLetterReason } from '../../src/domain/dead-letter.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { registerBuiltinHandlers } from '../../src/worker/handlers/index.js';
import { HandlerRegistry } from '../../src/worker/registry.js';
import { Worker } from '../../src/worker/worker.js';
import { createTestDatabase, describeWithDb, testEnv, type TestDatabase } from './helpers.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';

interface DeadLetterBody {
  id: string;
  queue: string;
  status: string;
  attempts: number;
  lastError: string | null;
  deadLetter: {
    reason: string;
    deadLetteredAt: string;
    attempts: number;
    lastError: string | null;
    queue: string;
    replayCount: number;
    lastReplayedAt: string | null;
  };
  attemptHistory?: Array<{ attemptNumber: number; status: string; error: string | null }>;
}

interface ErrorBody {
  error: { code: string; message: string; details?: unknown };
}

type LogLine = Record<string, unknown> & { msg?: string; event?: string };

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('waitFor timed out');
    await sleep(20);
  }
}

describeWithDb('dead-letter queue API (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let api: Inject;
  let leases: LeaseRepository;
  const appLogs: LogLine[] = [];

  beforeAll(async () => {
    db = await createTestDatabase();
    app = await buildApp({
      env: testEnv({ LOG_LEVEL: 'info' }),
      pool: db.pool,
      logStream: { write: (line) => void appLogs.push(JSON.parse(line) as LogLine) },
    });
    await app.ready();
    api = authedInject(app, (await createTestApiKey(db.pool)).token);
    leases = new LeaseRepository(db.pool);
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
    appLogs.length = 0;
  });

  async function enqueue(
    queue: string,
    body: Record<string, unknown> = { payload: {} },
  ): Promise<string> {
    const res = await api({ method: 'POST', url: `/queues/${queue}/jobs`, payload: body });
    expect(res.statusCode).toBe(201);
    return res.json<{ id: string }>().id;
  }

  /** Enqueue and dead-letter through the real lease/fail path (no worker loop needed). */
  async function deadJob(
    queue: string,
    reason: DeadLetterReason = 'attempts_exhausted',
  ): Promise<string> {
    const id = await enqueue(queue, { payload: { q: queue }, maxAttempts: 1 });
    const [leased] = await leases.lease({
      queues: [queue],
      workerId: 'w',
      limit: 1,
      leaseMs: 30_000,
    });
    if (leased?.jobId !== id) throw new Error('lease mismatch');
    await leases.fail(leased, {
      error: `boom ${reason}`,
      attemptStatus: 'failed',
      retryDelayMs: null,
      deadLetterReason: reason,
    });
    return id;
  }

  const replay = (
    queue: string,
    id: string,
    payload?: Record<string, unknown>,
  ): Promise<LightMyRequestResponse> =>
    api({
      method: 'POST',
      url: `/queues/${queue}/dlq/${id}/replay`,
      ...(payload !== undefined ? { payload } : {}),
    });

  it('end to end: worker exhausts retries -> DLQ -> inspect with history -> replay -> succeeds', async () => {
    let healthy = false;
    const workerLogs: LogLine[] = [];
    const registry = new HandlerRegistry().register('billing', async () => {
      if (!healthy) throw new Error('payment provider down');
    });
    const worker = new Worker({
      store: leases,
      registry,
      logger: pino(
        { level: 'info' },
        { write: (line: string) => void workerLogs.push(JSON.parse(line) as LogLine) },
      ),
      workerId: 'w-e2e',
      queues: ['billing'],
      concurrency: 2,
      pollIntervalMs: 20,
      leaseDurationMs: 5_000,
      heartbeatIntervalMs: 500,
      jobTimeoutMs: 5_000,
      backoff: { baseMs: 5, maxMs: 10 },
    });
    worker.start();
    try {
      const id = await enqueue('billing', { payload: { invoice: 42 }, maxAttempts: 2 });
      await waitFor(
        async () => (await api({ method: 'GET', url: `/dlq/${id}` })).statusCode === 200,
      );

      const list = await api({ method: 'GET', url: '/queues/billing/dlq' });
      expect(list.statusCode).toBe(200);
      const page = list.json<{ data: DeadLetterBody[]; nextCursor: string | null }>();
      expect(page.nextCursor).toBeNull();
      expect(page.data).toHaveLength(1);
      expect(page.data[0]).toMatchObject({
        id,
        status: 'dead',
        deadLetter: {
          reason: 'attempts_exhausted',
          attempts: 2,
          lastError: 'payment provider down',
          queue: 'billing',
          replayCount: 0,
          lastReplayedAt: null,
        },
      });

      const detail = (await api({ method: 'GET', url: `/dlq/${id}` })).json<DeadLetterBody>();
      expect(detail.attemptHistory?.map((a) => [a.attemptNumber, a.status])).toEqual([
        [1, 'failed'],
        [2, 'failed'],
      ]);
      expect(workerLogs).toContainEqual(
        expect.objectContaining({
          event: 'dlq.dead_lettered',
          jobId: id,
          reason: 'attempts_exhausted',
          source: 'worker',
        }),
      );

      healthy = true;
      const res = await replay('billing', id);
      expect(res.statusCode).toBe(200);
      expect(res.headers.location).toBe(`/jobs/${id}`);
      expect(res.json()).toMatchObject({ id, status: 'queued', attempts: 0, finishedAt: null });
      expect(appLogs).toContainEqual(
        expect.objectContaining({
          event: 'dlq.replayed',
          jobId: id,
          queue: 'billing',
          previousReason: 'attempts_exhausted',
          replayCount: 1,
          reqId: expect.any(String),
        }),
      );

      await waitFor(
        async () =>
          (await api({ method: 'GET', url: `/jobs/${id}` })).json<{ status: string }>().status ===
          'succeeded',
      );
      const { rows } = await db.pool.query<{ attempt_number: number; status: string }>(
        'SELECT attempt_number, status FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number',
        [id],
      );
      expect(rows.map((r) => [r.attempt_number, r.status])).toEqual([
        [1, 'failed'],
        [2, 'failed'],
        [3, 'succeeded'],
      ]);
      // Out of the DLQ now.
      expect((await api({ method: 'GET', url: `/dlq/${id}` })).statusCode).toBe(404);
    } finally {
      await worker.stop({ timeoutMs: 2_000 });
    }
  });

  it('two concurrent replays of the same job: one 200, one 409 JOB_NOT_DEAD_LETTERED', async () => {
    const id = await deadJob('race');
    const results = await Promise.all([replay('race', id), replay('race', id), replay('race', id)]);
    const codes = results.map((r) => r.statusCode).sort();
    expect(codes).toEqual([200, 409, 409]);
    for (const r of results.filter((x) => x.statusCode === 409)) {
      expect(r.json<ErrorBody>().error.code).toBe('JOB_NOT_DEAD_LETTERED');
    }
    const { rows } = await db.pool.query('SELECT replay_count, status FROM jobs WHERE id = $1', [
      id,
    ]);
    expect(rows).toEqual([{ replay_count: 1, status: 'queued' }]);
  });

  it('replay validates input and distinguishes 404 (unknown / other queue) from 409 (not dead)', async () => {
    const id = await deadJob('a');
    await enqueue('b');
    const live = await enqueue('a');

    const badId = await replay('a', 'not-a-uuid');
    expect(badId.statusCode).toBe(400);
    expect(badId.json<ErrorBody>().error.code).toBe('VALIDATION_ERROR');
    expect((await replay('a', id, { delayMs: -5 })).statusCode).toBe(400);
    expect((await replay('a', id, { payload: {} })).statusCode).toBe(400);

    expect((await replay('nope', id)).json<ErrorBody>().error.code).toBe('NOT_FOUND');
    expect((await replay('b', id)).statusCode).toBe(404);
    expect((await replay('a', randomUUID())).statusCode).toBe(404);
    const notDead = await replay('a', live);
    expect(notDead.statusCode).toBe(409);
    expect(notDead.json<ErrorBody>().error.details).toMatchObject({ status: 'queued' });

    const delayed = await replay('a', id, { delayMs: 60_000 });
    expect(delayed.statusCode).toBe(200);
    expect(delayed.json()).toMatchObject({ status: 'scheduled' });
  });

  it('lists with cursor pagination and a reason filter; validates the query', async () => {
    const ids: string[] = [];
    for (let i = 0; i < 5; i++)
      ids.push(await deadJob('pages', i === 2 ? 'invalid_payload' : 'attempts_exhausted'));

    const seen: string[] = [];
    let cursor: string | null = null;
    do {
      const url: string = `/queues/pages/dlq?limit=2${cursor ? `&cursor=${cursor}` : ''}`;
      const res = await api({ method: 'GET', url });
      expect(res.statusCode).toBe(200);
      const body = res.json<{ data: DeadLetterBody[]; nextCursor: string | null }>();
      seen.push(...body.data.map((d) => d.id));
      cursor = body.nextCursor;
    } while (cursor);
    expect(seen).toEqual([...ids].reverse());

    const filtered = await api({
      method: 'GET',
      url: '/queues/pages/dlq?reason=invalid_payload',
    });
    expect(filtered.json<{ data: DeadLetterBody[] }>().data.map((d) => d.id)).toEqual([ids[2]]);

    expect((await api({ method: 'GET', url: '/queues/pages/dlq?reason=meh' })).statusCode).toBe(
      400,
    );
    expect((await api({ method: 'GET', url: '/queues/pages/dlq?cursor=%%%' })).statusCode).toBe(
      400,
    );
    expect((await api({ method: 'GET', url: '/queues/missing/dlq' })).statusCode).toBe(404);
  });

  it('GET /dlq/:jobId is 404 for live or unknown jobs and 400 for a bad id', async () => {
    const live = await enqueue('x');
    expect((await api({ method: 'GET', url: `/dlq/${live}` })).statusCode).toBe(404);
    expect((await api({ method: 'GET', url: `/dlq/${randomUUID()}` })).statusCode).toBe(404);
    expect((await api({ method: 'GET', url: '/dlq/123' })).statusCode).toBe(400);
  });

  it('bulk replay: limit, reason filter, empty body defaults, logged', async () => {
    for (let i = 0; i < 3; i++) await deadJob('bulk', 'non_retryable');
    await deadJob('bulk', 'invalid_payload');

    const first = await api({
      method: 'POST',
      url: '/queues/bulk/dlq/replay',
      payload: { limit: 2, reason: 'non_retryable' },
    });
    expect(first.statusCode).toBe(200);
    expect(first.json<{ replayed: number; jobIds: string[] }>()).toMatchObject({ replayed: 2 });

    const rest = await api({ method: 'POST', url: '/queues/bulk/dlq/replay' });
    expect(rest.json<{ replayed: number }>().replayed).toBe(2);
    const none = await api({ method: 'POST', url: '/queues/bulk/dlq/replay', payload: {} });
    expect(none.json()).toEqual({ replayed: 0, jobIds: [] });

    expect(
      (
        await api({
          method: 'POST',
          url: '/queues/bulk/dlq/replay',
          payload: { limit: 5000 },
        })
      ).statusCode,
    ).toBe(400);
    expect(appLogs.filter((l) => l.event === 'dlq.bulk_replayed').map((l) => l.replayed)).toEqual([
      2, 2, 0,
    ]);
  });

  it('purge one: 204 then 404; a live job is 409 and survives', async () => {
    const id = await deadJob('purge');
    const live = await enqueue('purge');
    const res = await api({ method: 'DELETE', url: `/queues/purge/dlq/${id}` });
    expect(res.statusCode).toBe(204);
    expect(res.body).toBe('');
    expect((await api({ method: 'DELETE', url: `/queues/purge/dlq/${id}` })).statusCode).toBe(404);
    expect((await api({ method: 'DELETE', url: `/queues/purge/dlq/${live}` })).statusCode).toBe(
      409,
    );
    expect((await api({ method: 'GET', url: `/jobs/${live}` })).statusCode).toBe(200);
    expect(appLogs).toContainEqual(
      expect.objectContaining({ event: 'dlq.purged', jobId: id, queue: 'purge' }),
    );
  });

  it('bulk purge requires olderThan and deletes only older dead jobs', async () => {
    const old = await deadJob('bp');
    await db.pool.query(
      "UPDATE jobs SET dead_lettered_at = now() - interval '30 days' WHERE id = $1",
      [old],
    );
    const recent = await deadJob('bp');

    const missing = await api({ method: 'DELETE', url: '/queues/bp/dlq' });
    expect(missing.statusCode).toBe(400);
    expect(missing.json<ErrorBody>().error.code).toBe('VALIDATION_ERROR');

    const weekAgo = new Date(Date.now() - 7 * 24 * 3600 * 1000).toISOString();
    const res = await api({
      method: 'DELETE',
      url: `/queues/bp/dlq?olderThan=${encodeURIComponent(weekAgo)}`,
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ purged: 1 });
    expect((await api({ method: 'GET', url: `/dlq/${old}` })).statusCode).toBe(404);
    expect((await api({ method: 'GET', url: `/dlq/${recent}` })).statusCode).toBe(200);
    expect(appLogs).toContainEqual(
      expect.objectContaining({ event: 'dlq.bulk_purged', queue: 'bp', purged: 1 }),
    );
  });

  describe('poison messages through the API', () => {
    it('an invalid payload is dead-lettered on the first attempt with reason invalid_payload', async () => {
      const registry = registerBuiltinHandlers(new HandlerRegistry());
      const worker = new Worker({
        store: leases,
        registry,
        logger: pino({ level: 'silent' }),
        workerId: 'w-poison',
        queues: ['demo'],
        concurrency: 1,
        pollIntervalMs: 20,
        leaseDurationMs: 5_000,
        heartbeatIntervalMs: 500,
        jobTimeoutMs: 5_000,
        backoff: { baseMs: 5, maxMs: 10 },
      });
      worker.start();
      try {
        const id = await enqueue('demo', { payload: { durationMs: 'soon' }, maxAttempts: 5 });
        await waitFor(
          async () => (await api({ method: 'GET', url: `/dlq/${id}` })).statusCode === 200,
        );
        const detail = (await api({ method: 'GET', url: `/dlq/${id}` })).json<DeadLetterBody>();
        expect(detail.deadLetter).toMatchObject({ reason: 'invalid_payload', attempts: 1 });
        expect(detail.lastError).toMatch(/^InvalidPayloadError: Invalid demo payload/);
      } finally {
        await worker.stop({ timeoutMs: 2_000 });
      }
    });
  });
});
