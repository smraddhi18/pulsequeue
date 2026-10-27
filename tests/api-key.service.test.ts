import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import { ConflictError, NotFoundError, UnauthorizedError } from '../src/domain/errors.js';
import type { TenantId } from '../src/domain/tenant.js';
import { decodeCursor } from '../src/lib/cursor.js';
import type {
  ApiKeyCredential,
  InsertApiKeyParams,
  ListApiKeysParams,
  RotateApiKeyParams,
  StoredApiKey,
} from '../src/repositories/api-key.repository.js';
import { ApiKeyService, type ApiKeyStore } from '../src/services/api-key.service.js';

const T1 = 'acme' as TenantId;
const T2 = 'globex' as TenantId;

/** In-memory ApiKeyStore with the repository's semantics (active = not revoked, not expired). */
class FakeKeys implements ApiKeyStore {
  readonly rows = new Map<string, StoredApiKey & { secretHash: Buffer }>();
  credentialLookups = 0;
  touches: string[] = [];
  insertFailures: Error[] = [];
  now = Date.parse('2026-10-26T00:00:00Z');
  private tick = 0;

  private active(row: StoredApiKey): boolean {
    return row.revokedAt === null && (row.expiresAt === null || row.expiresAt.getTime() > this.now);
  }

  async insert(p: InsertApiKeyParams): Promise<StoredApiKey> {
    const failure = this.insertFailures.shift();
    if (failure) throw failure;
    const at = new Date(this.now + this.tick++);
    const row = {
      id: randomUUID(),
      tenantId: p.tenantId,
      name: p.name,
      prefix: p.prefix,
      role: p.role,
      createdAt: at,
      expiresAt: p.expiresAt,
      lastUsedAt: null,
      revokedAt: null,
      rotatedFrom: null,
      createdAtCursor: at.toISOString().replace('Z', '000Z'),
      secretHash: p.secretHash,
    };
    this.rows.set(row.id, row);
    return row;
  }

  async findActiveCredential(prefix: string): Promise<ApiKeyCredential | null> {
    this.credentialLookups++;
    for (const row of this.rows.values()) {
      if (row.prefix === prefix && this.active(row)) {
        return { key: row, secretHash: row.secretHash, tenantRateLimit: null };
      }
    }
    return null;
  }

  async findById(tenantId: TenantId, id: string): Promise<StoredApiKey | null> {
    const row = this.rows.get(id);
    return row && row.tenantId === tenantId ? row : null;
  }

  lastList: ListApiKeysParams | undefined;
  async list(p: ListApiKeysParams): Promise<StoredApiKey[]> {
    this.lastList = p;
    return [...this.rows.values()]
      .filter((r) => r.tenantId === p.tenantId && (p.includeRevoked || r.revokedAt === null))
      .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())
      .filter((r) => !p.after || r.createdAtCursor < p.after.createdAt)
      .slice(0, p.limit);
  }

  async revoke(tenantId: TenantId, id: string): Promise<StoredApiKey | null> {
    const row = await this.findById(tenantId, id);
    if (!row) return null;
    const updated = { ...this.rows.get(id)!, revokedAt: row.revokedAt ?? new Date(this.now) };
    this.rows.set(id, updated);
    return updated;
  }

  async rotate(p: RotateApiKeyParams): Promise<StoredApiKey | null> {
    const old = this.rows.get(p.id);
    if (!old || old.tenantId !== p.tenantId || !this.active(old)) return null;
    if (p.graceMs === 0) old.revokedAt = new Date(this.now);
    else old.expiresAt = new Date(this.now + p.graceMs);
    const created = await this.insert({
      tenantId: old.tenantId,
      name: old.name,
      prefix: p.prefix,
      secretHash: p.secretHash,
      role: old.role,
      expiresAt: p.expiresAt,
    });
    const row = this.rows.get(created.id)!;
    row.rotatedFrom = old.id;
    return row;
  }

  async touchLastUsed(id: string): Promise<void> {
    this.touches.push(id);
  }
}

const bearer = (token: string) => `Bearer ${token}`;

describe('ApiKeyService', () => {
  let keys: FakeKeys;
  let service: ApiKeyService;

  beforeEach(() => {
    keys = new FakeKeys();
    service = new ApiKeyService(keys, {
      cacheTtlMs: 5_000,
      cacheMaxEntries: 100,
      touchIntervalMs: 60_000,
      now: () => keys.now,
    });
  });

  it('issues a key whose plaintext authenticates as its tenant and role', async () => {
    const issued = await service.create({ tenantId: T1, name: 'ci', role: 'producer' });
    expect(issued.token).toMatch(/^pq_[a-z0-9]{12}_/);
    expect(issued.key).not.toHaveProperty('secretHash');
    expect(issued.key).not.toHaveProperty('createdAtCursor');
    expect(issued.token.startsWith(`pq_${issued.key.prefix}_`)).toBe(true);

    const principal = await service.authenticate(bearer(issued.token));
    expect(principal).toEqual({
      tenantId: T1,
      keyId: issued.key.id,
      keyPrefix: issued.key.prefix,
      role: 'producer',
      tenantRateLimit: null,
    });
  });

  it.each([
    [undefined, /Missing API key/],
    ['', /Missing API key/],
    ['Basic Zm9vOmJhcg==', /Malformed API key/],
    ['Bearer not-a-key', /Malformed API key/],
  ])('rejects header %j with 401', async (header, message) => {
    await expect(service.authenticate(header)).rejects.toThrow(message);
    await expect(service.authenticate(header)).rejects.toBeInstanceOf(UnauthorizedError);
    expect(keys.credentialLookups).toBe(0);
  });

  it('rejects a well-formed key with the right prefix but a wrong secret', async () => {
    const issued = await service.create({ tenantId: T1, name: 'ci', role: 'admin' });
    const forged = `pq_${issued.key.prefix}_${'A'.repeat(43)}`;
    await expect(service.authenticate(bearer(forged))).rejects.toThrow('Invalid API key');
  });

  it('serves repeat verifications from the cache and touches last_used_at at most per interval', async () => {
    const issued = await service.create({ tenantId: T1, name: 'ci', role: 'reader' });
    await service.authenticate(bearer(issued.token));
    await service.authenticate(bearer(issued.token));
    expect(keys.credentialLookups).toBe(1);
    expect(keys.touches).toEqual([issued.key.id]);

    keys.now += 4_000;
    await service.authenticate(bearer(issued.token));
    expect(keys.credentialLookups).toBe(1);

    keys.now += 2_000; // cache TTL (5s) passed
    await service.authenticate(bearer(issued.token));
    expect(keys.credentialLookups).toBe(2);
    expect(keys.touches).toHaveLength(2);
  });

  it('does not cache failed verifications', async () => {
    const forged = `pq_abcdefghijkl_${'A'.repeat(43)}`;
    await expect(service.authenticate(bearer(forged))).rejects.toThrow();
    await expect(service.authenticate(bearer(forged))).rejects.toThrow();
    expect(keys.credentialLookups).toBe(2);
  });

  it('stops honouring a cached key at its expiry', async () => {
    const issued = await service.create({
      tenantId: T1,
      name: 'short',
      role: 'reader',
      expiresAt: new Date(keys.now + 1_000),
    });
    await service.authenticate(bearer(issued.token));
    keys.now += 1_000;
    await expect(service.authenticate(bearer(issued.token))).rejects.toThrow('Invalid API key');
  });

  it('revocation takes effect immediately on this instance despite the cache', async () => {
    const issued = await service.create({ tenantId: T1, name: 'ci', role: 'admin' });
    await service.authenticate(bearer(issued.token));
    const revoked = await service.revoke(T1, issued.key.id);
    expect(revoked.revokedAt).not.toBeNull();
    await expect(service.authenticate(bearer(issued.token))).rejects.toThrow('Invalid API key');
  });

  it('treats another tenant’s key id as missing', async () => {
    const issued = await service.create({ tenantId: T1, name: 'ci', role: 'admin' });
    await expect(service.get(T2, issued.key.id)).rejects.toBeInstanceOf(NotFoundError);
    await expect(service.revoke(T2, issued.key.id)).rejects.toBeInstanceOf(NotFoundError);
    await expect(
      service.rotate({ tenantId: T2, id: issued.key.id, gracePeriodSeconds: 0 }),
    ).rejects.toBeInstanceOf(NotFoundError);
    expect((await service.get(T1, issued.key.id)).id).toBe(issued.key.id);
  });

  it('rotation without grace revokes the old key and links the new one', async () => {
    const old = await service.create({ tenantId: T1, name: 'deploy', role: 'producer' });
    await service.authenticate(bearer(old.token));
    const fresh = await service.rotate({ tenantId: T1, id: old.key.id, gracePeriodSeconds: 0 });
    expect(fresh.token).not.toBe(old.token);
    expect(fresh.key).toMatchObject({ name: 'deploy', role: 'producer', rotatedFrom: old.key.id });
    await expect(service.authenticate(bearer(old.token))).rejects.toThrow('Invalid API key');
    expect((await service.authenticate(bearer(fresh.token))).keyId).toBe(fresh.key.id);
  });

  it('rotation with grace keeps the old key working until the grace period ends', async () => {
    const old = await service.create({ tenantId: T1, name: 'deploy', role: 'producer' });
    await service.rotate({ tenantId: T1, id: old.key.id, gracePeriodSeconds: 60 });
    expect((await service.authenticate(bearer(old.token))).keyId).toBe(old.key.id);
    keys.now += 60_000;
    await expect(service.authenticate(bearer(old.token))).rejects.toThrow('Invalid API key');
  });

  it('refuses to rotate a revoked key with 409', async () => {
    const old = await service.create({ tenantId: T1, name: 'deploy', role: 'producer' });
    await service.revoke(T1, old.key.id);
    await expect(
      service.rotate({ tenantId: T1, id: old.key.id, gracePeriodSeconds: 0 }),
    ).rejects.toBeInstanceOf(ConflictError);
  });

  it('retries a prefix collision and surfaces other conflicts', async () => {
    keys.insertFailures.push(
      new ConflictError('Resource already exists', { constraint: 'api_keys_prefix_uq' }),
    );
    const issued = await service.create({ tenantId: T1, name: 'ci', role: 'admin' });
    expect(keys.rows.has(issued.key.id)).toBe(true);

    keys.insertFailures.push(new ConflictError('Resource already exists', { constraint: 'other' }));
    await expect(service.create({ tenantId: T1, name: 'ci', role: 'admin' })).rejects.toThrow(
      'Resource already exists',
    );
  });

  it('paginates newest first within the tenant and hides revoked keys by default', async () => {
    const a = await service.create({ tenantId: T1, name: 'a', role: 'admin' });
    const b = await service.create({ tenantId: T1, name: 'b', role: 'admin' });
    const c = await service.create({ tenantId: T1, name: 'c', role: 'admin' });
    await service.create({ tenantId: T2, name: 'other', role: 'admin' });
    await service.revoke(T1, b.key.id);

    const first = await service.list({ tenantId: T1, includeRevoked: true, limit: 2 });
    expect(first.keys.map((k) => k.name)).toEqual(['c', 'b']);
    expect(keys.lastList?.limit).toBe(3);
    expect(first.nextCursor).not.toBeNull();
    expect(decodeCursor(first.nextCursor!).id).toBe(b.key.id);

    const active = await service.list({ tenantId: T1, includeRevoked: false, limit: 10 });
    expect(active.keys.map((k) => k.id)).toEqual([c.key.id, a.key.id]);
    expect(active.nextCursor).toBeNull();
  });
});
