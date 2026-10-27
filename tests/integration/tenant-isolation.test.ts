import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { createTestDatabase, describeWithDb, testEnv, type TestDatabase } from './helpers.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';

interface ErrorBody {
  error: { code: string; message: string };
}

/**
 * Tenant isolation through the real HTTP stack: every resource is scoped by the
 * caller's tenant inside the SQL, and another tenant's resource answers 404
 * (never 403), so ids and queue names cannot be probed across tenants.
 */
describeWithDb('tenant isolation (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let acme: Inject;
  let globex: Inject;
  let leases: LeaseRepository;

  beforeAll(async () => {
    db = await createTestDatabase();
    app = await buildApp({ env: testEnv(), logger: false, pool: db.pool });
    await app.ready();
    acme = authedInject(app, (await createTestApiKey(db.pool, { tenantId: 'acme' })).token);
    globex = authedInject(app, (await createTestApiKey(db.pool, { tenantId: 'globex' })).token);
    leases = new LeaseRepository(db.pool);
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  async function enqueue(api: Inject, queue: string, body: Record<string, unknown> = {}) {
    const res = await api({
      method: 'POST',
      url: `/queues/${queue}/jobs`,
      payload: { payload: {}, ...body },
    });
    expect(res.statusCode).toBe(201);
    return res.json<{ id: string }>().id;
  }

  function expectNotFound(res: { statusCode: number; json: <T>() => T }) {
    expect(res.statusCode).toBe(404);
    expect(res.json<ErrorBody>().error.code).toBe('NOT_FOUND');
  }

  it('the same queue name is an independent queue per tenant', async () => {
    await enqueue(acme, 'iso-emails');
    await enqueue(acme, 'iso-emails');
    await enqueue(globex, 'iso-emails');

    const a = await acme({ method: 'GET', url: '/queues/iso-emails/jobs' });
    const g = await globex({ method: 'GET', url: '/queues/iso-emails/jobs' });
    expect(a.json<{ data: unknown[] }>().data).toHaveLength(2);
    expect(g.json<{ data: unknown[] }>().data).toHaveLength(1);

    const { rows } = await db.pool.query<{ tenant_id: string }>(
      `SELECT tenant_id FROM queues WHERE name = 'iso-emails' ORDER BY tenant_id`,
    );
    expect(rows.map((r) => r.tenant_id)).toEqual(['acme', 'globex']);

    const sa = await acme({ method: 'GET', url: '/queues/iso-emails/stats' });
    const sg = await globex({ method: 'GET', url: '/queues/iso-emails/stats' });
    expect(sa.json<{ counts: { queued: number } }>().counts.queued).toBe(2);
    expect(sg.json<{ counts: { queued: number } }>().counts.queued).toBe(1);
  });

  it("another tenant's job and queue answer 404", async () => {
    const id = await enqueue(acme, 'iso-private');
    expect((await acme({ method: 'GET', url: `/jobs/${id}` })).statusCode).toBe(200);
    expectNotFound(await globex({ method: 'GET', url: `/jobs/${id}` }));
    expectNotFound(await globex({ method: 'GET', url: '/queues/iso-private/stats' }));
    expectNotFound(await globex({ method: 'GET', url: '/queues/iso-private/dlq' }));
    expectNotFound(await globex({ method: 'GET', url: '/queues/iso-private/schedules' }));
  });

  it("another tenant's dead letter cannot be read, replayed or purged", async () => {
    const id = await enqueue(acme, 'iso-dlq', { maxAttempts: 1 });
    const [leased] = await leases.lease({
      queues: ['iso-dlq'],
      workerId: 'w',
      limit: 1,
      leaseMs: 30_000,
    });
    expect(leased?.jobId).toBe(id);
    expect(leased?.tenantId).toBe('acme');
    await leases.fail(leased!, {
      error: 'boom',
      attemptStatus: 'failed',
      retryDelayMs: null,
      deadLetterReason: 'attempts_exhausted',
    });

    expect((await acme({ method: 'GET', url: `/dlq/${id}` })).statusCode).toBe(200);
    expectNotFound(await globex({ method: 'GET', url: `/dlq/${id}` }));
    // Even when globex owns a queue with the same name, acme's job is not in it.
    await enqueue(globex, 'iso-dlq');
    expectNotFound(await globex({ method: 'POST', url: `/queues/iso-dlq/dlq/${id}/replay` }));
    expectNotFound(await globex({ method: 'DELETE', url: `/queues/iso-dlq/dlq/${id}` }));
    expect((await acme({ method: 'GET', url: `/dlq/${id}` })).statusCode).toBe(200);
  });

  it("another tenant's schedule cannot be read, paused, resumed or deleted", async () => {
    const created = await acme({
      method: 'POST',
      url: '/queues/iso-sched/schedules',
      payload: { name: 'nightly', cron: '0 3 * * *', payload: {} },
    });
    expect(created.statusCode).toBe(201);
    const id = created.json<{ id: string }>().id;

    expectNotFound(await globex({ method: 'GET', url: `/schedules/${id}` }));
    expectNotFound(await globex({ method: 'POST', url: `/schedules/${id}/pause` }));
    expectNotFound(await globex({ method: 'POST', url: `/schedules/${id}/resume` }));
    expectNotFound(await globex({ method: 'DELETE', url: `/schedules/${id}` }));

    const still = await acme({ method: 'GET', url: `/schedules/${id}` });
    expect(still.statusCode).toBe(200);
    expect(still.json<{ enabled: boolean }>().enabled).toBe(true);
  });
});
