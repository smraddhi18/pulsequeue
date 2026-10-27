import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import type { QueueSettingsResponse } from '../../src/routes/queues.js';
import { createTestDatabase, describeWithDb, testEnv, type TestDatabase } from './helpers.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';

describeWithDb('queue settings API (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let admin: Inject;
  let producer: Inject;
  let reader: Inject;
  let otherTenant: Inject;

  beforeAll(async () => {
    db = await createTestDatabase();
    app = await buildApp({ env: testEnv(), logger: false, pool: db.pool });
    await app.ready();
    admin = authedInject(app, (await createTestApiKey(db.pool, { tenantId: 'acme' })).token);
    producer = authedInject(
      app,
      (await createTestApiKey(db.pool, { tenantId: 'acme', role: 'producer' })).token,
    );
    reader = authedInject(
      app,
      (await createTestApiKey(db.pool, { tenantId: 'acme', role: 'reader' })).token,
    );
    otherTenant = authedInject(
      app,
      (await createTestApiKey(db.pool, { tenantId: 'globex' })).token,
    );
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  const put = (api: Inject, queue: string, payload: unknown) =>
    api({ method: 'PUT', url: `/queues/${queue}/settings`, payload: payload as object });
  const get = (api: Inject, queue: string) =>
    api({ method: 'GET', url: `/queues/${queue}/settings` });

  it('creates the queue on PUT, then reads the limits back', async () => {
    expect((await get(admin, 'settings-a')).statusCode).toBe(404);
    const res = await put(admin, 'settings-a', {
      maxConcurrency: 2,
      rateLimit: { perSec: 5, burst: 20 },
    });
    expect(res.statusCode).toBe(200);
    const body = res.json<QueueSettingsResponse>();
    expect(body).toMatchObject({
      queue: 'settings-a',
      maxConcurrency: 2,
      rateLimit: { perSec: 5, burst: 20 },
    });
    expect(new Date(body.updatedAt).toISOString()).toBe(body.updatedAt);

    const read = await get(reader, 'settings-a');
    expect(read.statusCode).toBe(200);
    expect(read.json()).toEqual(body);
  });

  it('null clears a limit (full replacement)', async () => {
    await put(admin, 'settings-b', { maxConcurrency: 3, rateLimit: { perSec: 1, burst: 1 } });
    const res = await put(admin, 'settings-b', { maxConcurrency: null, rateLimit: null });
    expect(res.json()).toMatchObject({ maxConcurrency: null, rateLimit: null });
  });

  it('only admins may change settings', async () => {
    const body = { maxConcurrency: 1, rateLimit: null };
    expect((await put(producer, 'settings-c', body)).statusCode).toBe(403);
    expect((await put(reader, 'settings-c', body)).statusCode).toBe(403);
    // Nothing was created by the rejected writes.
    expect((await get(admin, 'settings-c')).statusCode).toBe(404);
  });

  it.each([
    ['a missing key', { maxConcurrency: 1 }],
    ['an unknown key', { maxConcurrency: 1, rateLimit: null, concurrency: 1 }],
    ['zero concurrency', { maxConcurrency: 0, rateLimit: null }],
    ['a huge concurrency', { maxConcurrency: 10_001, rateLimit: null }],
    ['a fractional rate', { maxConcurrency: null, rateLimit: { perSec: 0.5, burst: 1 } }],
    ['a half-set rate limit', { maxConcurrency: null, rateLimit: { perSec: 5 } }],
    ['no body', undefined],
  ])('400s %s', async (_label, payload) => {
    const res = await put(admin, 'settings-d', payload);
    expect(res.statusCode).toBe(400);
    expect(res.json()).toMatchObject({ error: { code: 'VALIDATION_ERROR' } });
  });

  it("is per tenant: another tenant's queue of the same name is separate", async () => {
    await put(admin, 'settings-e', { maxConcurrency: 4, rateLimit: null });
    expect((await get(otherTenant, 'settings-e')).statusCode).toBe(404);
    await put(otherTenant, 'settings-e', { maxConcurrency: 9, rateLimit: null });
    expect((await get(admin, 'settings-e')).json()).toMatchObject({ maxConcurrency: 4 });
    expect((await get(otherTenant, 'settings-e')).json()).toMatchObject({ maxConcurrency: 9 });
  });
});
