import { afterAll, beforeAll, beforeEach, expect, it } from 'vitest';
import { ConflictError, NotFoundError, ValidationError } from '../../src/domain/errors.js';
import type { TenantId } from '../../src/domain/tenant.js';
import { ApiKeyRepository } from '../../src/repositories/api-key.repository.js';
import { TenantRepository } from '../../src/repositories/tenant.repository.js';
import { ApiKeyService } from '../../src/services/api-key.service.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

const ACME = 'acme' as TenantId;
const GLOBEX = 'globex' as TenantId;
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describeWithDb('API keys and tenants (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let tenants: TenantRepository;
  let repo: ApiKeyRepository;
  let service: ApiKeyService;

  beforeAll(async () => {
    db = await createTestDatabase();
    tenants = new TenantRepository(db.pool);
    repo = new ApiKeyRepository(db.pool);
  });

  afterAll(async () => {
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query(`DELETE FROM tenants WHERE id <> 'default'`);
    await db.pool.query('DELETE FROM api_keys');
    await tenants.create({ id: ACME, name: 'Acme' });
    await tenants.create({ id: GLOBEX, name: 'Globex' });
    // No cache: every authenticate() goes to Postgres, so these tests see the DB's view.
    service = new ApiKeyService(repo, { cacheTtlMs: 0, cacheMaxEntries: 1, touchIntervalMs: 0 });
  });

  it('creates tenants, rejects duplicates and stores rate-limit overrides as a pair', async () => {
    await expect(tenants.create({ id: ACME, name: 'again' })).rejects.toBeInstanceOf(ConflictError);
    await expect(tenants.create({ id: 'Bad_Id' as TenantId, name: 'x' })).rejects.toBeInstanceOf(
      ValidationError,
    );

    const limited = await tenants.setRateLimit(ACME, { perSec: 5, burst: 10 });
    expect(limited?.rateLimit).toEqual({ perSec: 5, burst: 10 });
    expect((await tenants.setRateLimit(ACME, null))?.rateLimit).toBeNull();
    expect(await tenants.setRateLimit('nope' as TenantId, null)).toBeNull();
    expect((await tenants.findById('default' as TenantId))?.name).toBe('Default tenant');
  });

  it('stores only the SHA-256 of the token and authenticates through Postgres', async () => {
    const issued = await service.create({ tenantId: ACME, name: 'ci', role: 'producer' });
    const { rows } = await db.pool.query<{ secret_hash: Buffer; prefix: string }>(
      'SELECT secret_hash, prefix FROM api_keys WHERE id = $1',
      [issued.key.id],
    );
    expect(rows[0]!.secret_hash).toHaveLength(32);
    expect(rows[0]!.secret_hash.toString('utf8')).not.toContain(issued.token.slice(16));
    expect(rows[0]!.prefix).toBe(issued.key.prefix);

    await tenants.setRateLimit(ACME, { perSec: 7, burst: 9 });
    const principal = await service.authenticate(`Bearer ${issued.token}`);
    expect(principal).toMatchObject({
      tenantId: ACME,
      role: 'producer',
      tenantRateLimit: { perSec: 7, burst: 9 },
    });
  });

  it('records last use, throttled per key', async () => {
    const issued = await service.create({ tenantId: ACME, name: 'ci', role: 'reader' });
    await repo.touchLastUsed(issued.key.id, 60_000);
    const first = (await repo.findById(ACME, issued.key.id))!.lastUsedAt;
    expect(first).not.toBeNull();
    await sleep(5);
    await repo.touchLastUsed(issued.key.id, 60_000);
    expect((await repo.findById(ACME, issued.key.id))!.lastUsedAt).toEqual(first);
    await repo.touchLastUsed(issued.key.id, 0);
    expect((await repo.findById(ACME, issued.key.id))!.lastUsedAt!.getTime()).toBeGreaterThan(
      first!.getTime(),
    );
  });

  it('rejects revoked and expired keys', async () => {
    const revoked = await service.create({ tenantId: ACME, name: 'r', role: 'admin' });
    await service.revoke(ACME, revoked.key.id);
    await expect(service.authenticate(`Bearer ${revoked.token}`)).rejects.toThrow(
      'Invalid API key',
    );

    const expiring = await service.create({
      tenantId: ACME,
      name: 'e',
      role: 'admin',
      expiresAt: new Date(Date.now() + 300),
    });
    await service.authenticate(`Bearer ${expiring.token}`);
    await sleep(350);
    await expect(service.authenticate(`Bearer ${expiring.token}`)).rejects.toThrow(
      'Invalid API key',
    );
  });

  it('keeps tenants apart on every id-based operation', async () => {
    const issued = await service.create({ tenantId: ACME, name: 'ci', role: 'admin' });
    expect(await repo.findById(GLOBEX, issued.key.id)).toBeNull();
    expect(await repo.revoke(GLOBEX, issued.key.id)).toBeNull();
    await expect(
      service.rotate({ tenantId: GLOBEX, id: issued.key.id, gracePeriodSeconds: 0 }),
    ).rejects.toBeInstanceOf(NotFoundError);
    const listed = await service.list({ tenantId: GLOBEX, includeRevoked: true, limit: 10 });
    expect(listed.keys).toEqual([]);
    // Still intact for its owner.
    expect((await service.authenticate(`Bearer ${issued.token}`)).tenantId).toBe(ACME);
  });

  it('rotates atomically: grace 0 revokes, a grace period shortens the expiry', async () => {
    const old = await service.create({ tenantId: ACME, name: 'deploy', role: 'producer' });
    const fresh = await service.rotate({ tenantId: ACME, id: old.key.id, gracePeriodSeconds: 0 });
    expect(fresh.key).toMatchObject({ name: 'deploy', role: 'producer', rotatedFrom: old.key.id });
    expect((await repo.findById(ACME, old.key.id))!.revokedAt).not.toBeNull();
    await expect(service.authenticate(`Bearer ${old.token}`)).rejects.toThrow();
    await expect(
      service.rotate({ tenantId: ACME, id: old.key.id, gracePeriodSeconds: 0 }),
    ).rejects.toBeInstanceOf(ConflictError);

    const graced = await service.rotate({
      tenantId: ACME,
      id: fresh.key.id,
      gracePeriodSeconds: 3600,
    });
    const previous = (await repo.findById(ACME, fresh.key.id))!;
    expect(previous.revokedAt).toBeNull();
    expect(previous.expiresAt!.getTime()).toBeGreaterThan(Date.now() + 3_500_000);
    expect((await service.authenticate(`Bearer ${fresh.token}`)).keyId).toBe(fresh.key.id);
    expect((await service.authenticate(`Bearer ${graced.token}`)).keyId).toBe(graced.key.id);
  });

  it('lets exactly one of two concurrent grace-0 rotations win', async () => {
    const old = await service.create({ tenantId: ACME, name: 'deploy', role: 'producer' });
    const results = await Promise.allSettled([
      service.rotate({ tenantId: ACME, id: old.key.id, gracePeriodSeconds: 0 }),
      service.rotate({ tenantId: ACME, id: old.key.id, gracePeriodSeconds: 0 }),
    ]);
    expect(results.filter((r) => r.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find((r) => r.status === 'rejected') as PromiseRejectedResult;
    expect(rejected.reason).toBeInstanceOf(ConflictError);
  });

  it('pages through keys newest first and never selects the hash', async () => {
    for (const name of ['a', 'b', 'c', 'd', 'e']) {
      await service.create({ tenantId: ACME, name, role: 'reader' });
    }
    const seen: string[] = [];
    let cursor: string | undefined;
    do {
      const page = await service.list({
        tenantId: ACME,
        includeRevoked: false,
        limit: 2,
        ...(cursor !== undefined ? { cursor } : {}),
      });
      for (const key of page.keys) {
        expect(Object.keys(key)).not.toContain('secretHash');
        seen.push(key.name);
      }
      cursor = page.nextCursor ?? undefined;
    } while (cursor !== undefined);
    expect(seen).toEqual(['e', 'd', 'c', 'b', 'a']);
  });

  it('cascades key deletion with the tenant', async () => {
    await service.create({ tenantId: GLOBEX, name: 'x', role: 'reader' });
    await db.pool.query(`DELETE FROM tenants WHERE id = 'globex'`);
    const { rows } = await db.pool.query(`SELECT 1 FROM api_keys WHERE tenant_id = 'globex'`);
    expect(rows).toHaveLength(0);
  });
});
