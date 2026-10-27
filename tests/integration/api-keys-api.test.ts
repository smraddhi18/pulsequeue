import { randomUUID } from 'node:crypto';
import type { FastifyInstance } from 'fastify';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import type { ApiKeyResponse } from '../../src/routes/api-keys.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';
import {
  createTestDatabase,
  describeWithDb,
  testEnv,
  waitUntil,
  type TestDatabase,
} from './helpers.js';

type Issued = ApiKeyResponse & { token: string };

describeWithDb('API keys and auth (Postgres)', () => {
  let db: TestDatabase;
  let app: FastifyInstance;
  let admin: Inject;
  let adminKeyId: string;
  const as = (token: string): Inject => authedInject(app, token);

  beforeAll(async () => {
    db = await createTestDatabase();
    app = await buildApp({ env: testEnv(), logger: false, pool: db.pool });
    await app.ready();
    const key = await createTestApiKey(db.pool, { tenantId: 'acme', role: 'admin' });
    admin = as(key.token);
    adminKeyId = key.id;
  });

  afterAll(async () => {
    await app?.close();
    await db?.destroy();
  });

  async function issue(body: Record<string, unknown>, by: Inject = admin): Promise<Issued> {
    const res = await by({ method: 'POST', url: '/api-keys', payload: body });
    expect(res.statusCode).toBe(201);
    return res.json<Issued>();
  }

  const whoami = (token: string) => as(token)({ method: 'GET', url: '/auth/whoami' });

  it('whoami identifies the calling key', async () => {
    const res = await admin({ method: 'GET', url: '/auth/whoami' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ tenantId: 'acme', keyId: adminKeyId, role: 'admin' });
  });

  it('issues a key once: token in the 201 only, hash at rest, never the secret', async () => {
    const res = await admin({
      method: 'POST',
      url: '/api-keys',
      payload: { name: '  billing-producer ', role: 'producer' },
    });
    expect(res.statusCode).toBe(201);
    expect(res.headers['cache-control']).toBe('no-store');
    const issued = res.json<Issued>();
    expect(res.headers.location).toBe(`/api-keys/${issued.id}`);
    expect(issued).toMatchObject({ tenantId: 'acme', name: 'billing-producer', role: 'producer' });
    expect(issued.token).toMatch(new RegExp(`^pq_${issued.prefix}_[A-Za-z0-9_-]{43}$`));

    const fetched = await admin({ method: 'GET', url: `/api-keys/${issued.id}` });
    expect(fetched.json()).not.toHaveProperty('token');
    expect(JSON.stringify(fetched.json())).not.toContain('secret');

    const { rows } = await db.pool.query<{ row: string }>(
      'SELECT row_to_json(k)::text AS row FROM api_keys k WHERE id = $1',
      [issued.id],
    );
    const secret = issued.token.split('_').slice(2).join('_');
    expect(rows[0]!.row).not.toContain(secret);

    expect((await whoami(issued.token)).json()).toMatchObject({ role: 'producer' });
  });

  it('records last use', async () => {
    const issued = await issue({ name: 'touch', role: 'reader' });
    expect(issued.lastUsedAt).toBeNull();
    expect((await whoami(issued.token)).statusCode).toBe(200);
    // The touch is fire-and-forget (auth never waits on a write), so poll for it.
    await waitUntil(async () => {
      const res = await admin({ method: 'GET', url: `/api-keys/${issued.id}` });
      return res.json<ApiKeyResponse>().lastUsedAt !== null;
    });
  });

  it('only admin keys manage keys (403 otherwise)', async () => {
    const producer = await issue({ name: 'p', role: 'producer' });
    const reader = await issue({ name: 'r', role: 'reader' });
    for (const token of [producer.token, reader.token]) {
      const res = await as(token)({ method: 'GET', url: '/api-keys' });
      expect(res.statusCode).toBe(403);
      expect(res.json()).toMatchObject({
        error: { code: 'FORBIDDEN', details: { required: 'manage' } },
      });
    }
    const readerEnqueue = await as(reader.token)({
      method: 'POST',
      url: '/queues/q/jobs',
      payload: { payload: {} },
    });
    expect(readerEnqueue.statusCode).toBe(403);
    expect(
      (
        await as(producer.token)({
          method: 'POST',
          url: '/queues/q/jobs',
          payload: { payload: {} },
        })
      ).statusCode,
    ).toBe(201);
    expect((await as(reader.token)({ method: 'GET', url: '/queues/q/jobs' })).statusCode).toBe(200);
  });

  it('answers 401 for missing, malformed and unknown keys', async () => {
    expect((await app.inject({ method: 'GET', url: '/auth/whoami' })).statusCode).toBe(401);
    expect((await whoami('pq_nope')).statusCode).toBe(401);
    const unknown = await whoami(`pq_zzzzzzzzzzzz_${'A'.repeat(43)}`);
    expect(unknown.statusCode).toBe(401);
    expect(unknown.json()).toMatchObject({ error: { message: 'Invalid API key' } });
    // Right prefix, wrong secret.
    const real = await issue({ name: 'victim', role: 'reader' });
    const forged = `pq_${real.prefix}_${'B'.repeat(43)}`;
    expect((await whoami(forged)).statusCode).toBe(401);
  });

  it('revocation takes effect immediately on this instance and is idempotent', async () => {
    const issued = await issue({ name: 'to-revoke', role: 'reader' });
    expect((await whoami(issued.token)).statusCode).toBe(200); // now cached
    const res = await admin({ method: 'DELETE', url: `/api-keys/${issued.id}` });
    expect(res.statusCode).toBe(200);
    const revokedAt = res.json<ApiKeyResponse>().revokedAt;
    expect(revokedAt).not.toBeNull();
    expect((await whoami(issued.token)).statusCode).toBe(401);
    const again = await admin({ method: 'DELETE', url: `/api-keys/${issued.id}` });
    expect(again.json<ApiKeyResponse>().revokedAt).toBe(revokedAt);
  });

  it('rejects expired keys', async () => {
    const issued = await issue({ name: 'short', role: 'reader' });
    await db.pool.query(
      `UPDATE api_keys SET created_at = now() - interval '2 hours', expires_at = now() - interval '1 hour' WHERE id = $1`,
      [issued.id],
    );
    expect((await whoami(issued.token)).statusCode).toBe(401);
  });

  it('rotates with no grace (old key dead at once) or with a grace period', async () => {
    const original = await issue({ name: 'svc', role: 'producer' });
    const hard = await admin({ method: 'POST', url: `/api-keys/${original.id}/rotate` });
    expect(hard.statusCode).toBe(201);
    const next = hard.json<Issued>();
    expect(next).toMatchObject({ name: 'svc', role: 'producer', rotatedFrom: original.id });
    expect((await whoami(original.token)).statusCode).toBe(401);
    expect((await whoami(next.token)).statusCode).toBe(200);

    const soft = await admin({
      method: 'POST',
      url: `/api-keys/${next.id}/rotate`,
      payload: { gracePeriodSeconds: 3600 },
    });
    expect(soft.statusCode).toBe(201);
    expect((await whoami(next.token)).statusCode).toBe(200); // still valid during grace
    const old = (
      await admin({ method: 'GET', url: `/api-keys/${next.id}` })
    ).json<ApiKeyResponse>();
    expect(old.revokedAt).toBeNull();
    expect(Date.parse(old.expiresAt!) - Date.now()).toBeGreaterThan(3500_000);

    // A revoked key cannot be rotated back to life.
    const dead = await admin({ method: 'POST', url: `/api-keys/${original.id}/rotate` });
    expect(dead.statusCode).toBe(409);
  });

  it('lists keys newest first with cursors, hiding revoked ones unless asked', async () => {
    const res = await admin({ method: 'GET', url: '/api-keys?limit=2' });
    expect(res.statusCode).toBe(200);
    const page = res.json<{ data: ApiKeyResponse[]; nextCursor: string | null }>();
    expect(page.data).toHaveLength(2);
    expect(page.nextCursor).not.toBeNull();

    const all: ApiKeyResponse[] = [];
    let cursor: string | null = null;
    do {
      const url: string = `/api-keys?limit=3${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
      const p = (await admin({ method: 'GET', url })).json<{
        data: ApiKeyResponse[];
        nextCursor: string | null;
      }>();
      all.push(...p.data);
      cursor = p.nextCursor;
    } while (cursor);
    expect(all.every((k) => k.revokedAt === null)).toBe(true);
    expect(new Set(all.map((k) => k.id)).size).toBe(all.length);
    const sorted = [...all].sort((a, b) => b.createdAt.localeCompare(a.createdAt));
    expect(all.map((k) => k.id)).toEqual(sorted.map((k) => k.id));

    const withRevoked = await admin({
      method: 'GET',
      url: '/api-keys?includeRevoked=true&limit=200',
    });
    expect(withRevoked.json<{ data: ApiKeyResponse[] }>().data.some((k) => k.revokedAt)).toBe(true);
  });

  it('validates input', async () => {
    const bad = async (payload: unknown) =>
      (await admin({ method: 'POST', url: '/api-keys', payload: payload as object })).statusCode;
    expect(await bad({ name: '', role: 'admin' })).toBe(400);
    expect(await bad({ name: 'x', role: 'root' })).toBe(400);
    expect(await bad({ name: 'x', role: 'admin', tenantId: 'globex' })).toBe(400);
    expect(await bad({ name: 'x', role: 'admin', expiresAt: '2000-01-01T00:00:00Z' })).toBe(400);
    expect((await admin({ method: 'GET', url: '/api-keys/nope' })).statusCode).toBe(400);
    expect((await admin({ method: 'GET', url: '/api-keys?cursor=%%%' })).statusCode).toBe(400);
    expect(
      (
        await admin({
          method: 'POST',
          url: `/api-keys/${adminKeyId}/rotate`,
          payload: { gracePeriodSeconds: -1 },
        })
      ).statusCode,
    ).toBe(400);
  });

  describe('tenant isolation', () => {
    it("never shows, revokes or rotates another tenant's keys (404)", async () => {
      const acmeKey = await issue({ name: 'acme-only', role: 'reader' });
      const globex = await createTestApiKey(db.pool, { tenantId: 'globex', role: 'admin' });
      const other = as(globex.token);

      expect((await other({ method: 'GET', url: `/api-keys/${acmeKey.id}` })).statusCode).toBe(404);
      expect((await other({ method: 'DELETE', url: `/api-keys/${acmeKey.id}` })).statusCode).toBe(
        404,
      );
      expect(
        (await other({ method: 'POST', url: `/api-keys/${acmeKey.id}/rotate` })).statusCode,
      ).toBe(404);
      expect((await whoami(acmeKey.token)).statusCode).toBe(200);

      const list = await other({ method: 'GET', url: '/api-keys?limit=200' });
      const ids = list.json<{ data: ApiKeyResponse[] }>().data.map((k) => k.id);
      expect(ids).toEqual([globex.id]);
      expect((await admin({ method: 'GET', url: `/api-keys/${randomUUID()}` })).statusCode).toBe(
        404,
      );
    });
  });

  it('keeps /health and /ready public', async () => {
    expect((await app.inject({ method: 'GET', url: '/health' })).statusCode).toBe(200);
    expect((await app.inject({ method: 'GET', url: '/ready' })).statusCode).toBe(200);
  });
});
