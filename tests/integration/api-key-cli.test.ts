import { afterAll, beforeAll, expect, it } from 'vitest';
import { runApiKeyCli } from '../../src/cli/api-key-cli.js';
import { ApiKeyRepository } from '../../src/repositories/api-key.repository.js';
import { ApiKeyService } from '../../src/services/api-key.service.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

describeWithDb('api-key CLI (Postgres)', () => {
  let db: TestDatabase;

  beforeAll(async () => {
    db = await createTestDatabase();
  });

  afterAll(async () => {
    await db?.destroy();
  });

  async function cli(...argv: string[]) {
    let stdout = '';
    let stderr = '';
    const code = await runApiKeyCli(argv, db.pool, {
      stdout: { write: (c: string) => (stdout += c) },
      stderr: { write: (c: string) => (stderr += c) },
    });
    return { code, stdout, stderr, json: () => JSON.parse(stdout) as Record<string, unknown> };
  }

  const authenticate = (token: string) =>
    new ApiKeyService(new ApiKeyRepository(db.pool), {
      cacheTtlMs: 0,
      cacheMaxEntries: 1,
    }).authenticate(`Bearer ${token}`);

  it('bootstraps a tenant and its first admin key; the token works', async () => {
    const missing = await cli('create', '--tenant', 'acme', '--role', 'admin', '--name', 'root');
    expect(missing.code).toBe(2);
    expect(missing.stderr).toMatch(/does not exist/);

    const res = await cli(
      'create',
      '--tenant',
      'acme',
      '--role',
      'admin',
      '--name',
      'root',
      '--create-tenant',
    );
    expect(res.code).toBe(0);
    expect(res.stderr).toMatch(/cannot be shown again/);
    const out = res.json();
    expect(out).toMatchObject({ tenantId: 'acme', role: 'admin', name: 'root' });
    const principal = await authenticate(out.token as string);
    expect(principal).toMatchObject({ tenantId: 'acme', role: 'admin', keyId: out.id });
  });

  it('lists and revokes keys of one tenant', async () => {
    const created = (
      await cli('create', '--tenant', 'default', '--role', 'reader', '--name', 'ro')
    ).json();
    const list = await cli('list', '--tenant', 'default');
    expect(list.code).toBe(0);
    expect((JSON.parse(list.stdout) as { id: string; token?: string }[]).map((k) => k.id)).toEqual([
      created.id,
    ]);
    expect(list.stdout).not.toContain('"token"');

    const revoked = await cli('revoke', '--tenant', 'default', '--id', created.id as string);
    expect(revoked.code).toBe(0);
    expect(revoked.json().revokedAt).not.toBeNull();
    await expect(authenticate(created.token as string)).rejects.toThrow(/Invalid API key/);
    expect(JSON.parse((await cli('list', '--tenant', 'default')).stdout)).toEqual([]);
    expect(
      JSON.parse((await cli('list', '--tenant', 'default', '--include-revoked')).stdout),
    ).toHaveLength(1);

    const wrongTenant = await cli('revoke', '--tenant', 'acme', '--id', created.id as string);
    expect(wrongTenant.code).toBe(1);
    expect(wrongTenant.stderr).toMatch(/not found/i);
  });

  it('creates tenants and sets or clears their rate limit', async () => {
    const t = await cli('tenant-create', '--tenant', 'globex', '--name', 'Globex Corp');
    expect(t.code).toBe(0);
    expect(t.json()).toMatchObject({ id: 'globex', name: 'Globex Corp', rateLimit: null });
    expect((await cli('tenant-create', '--tenant', 'globex', '--name', 'again')).code).toBe(1);

    const set = await cli(
      'tenant-rate-limit',
      '--tenant',
      'globex',
      '--per-sec',
      '5',
      '--burst',
      '10',
    );
    expect(set.code).toBe(0);
    expect(set.json().rateLimit).toEqual({ perSec: 5, burst: 10 });
    const cleared = await cli('tenant-rate-limit', '--tenant', 'globex', '--clear');
    expect(cleared.json().rateLimit).toBeNull();

    expect((await cli('tenant-rate-limit', '--tenant', 'globex', '--per-sec', '5')).code).toBe(2);
    expect(
      (await cli('tenant-rate-limit', '--tenant', 'globex', '--clear', '--burst', '1')).code,
    ).toBe(2);
    expect((await cli('tenant-rate-limit', '--tenant', 'nope', '--clear')).code).toBe(2);
  });

  it('rejects bad usage with exit code 2', async () => {
    expect((await cli()).code).toBe(2);
    expect((await cli('frobnicate')).stderr).toMatch(/Unknown command "frobnicate"/);
    expect((await cli('list', '--tenant', 'acme', '--bogus')).code).toBe(2);
    expect((await cli('list', '--tenant', 'Not Valid')).code).toBe(2);
    expect((await cli('create', '--tenant', 'acme', '--role', 'root', '--name', 'x')).code).toBe(2);
    expect((await cli('revoke', '--tenant', 'acme', '--id', 'nope')).code).toBe(2);
  });
});
