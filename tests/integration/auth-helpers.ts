import type { FastifyInstance, InjectOptions, LightMyRequestResponse } from 'fastify';
import type { Pool } from '../../src/db/pool.js';
import { DEFAULT_TENANT_ID, type ApiKeyRole, type TenantId } from '../../src/domain/tenant.js';
import { ApiKeyRepository } from '../../src/repositories/api-key.repository.js';
import { TenantRepository } from '../../src/repositories/tenant.repository.js';
import { ApiKeyService } from '../../src/services/api-key.service.js';

export interface TestApiKey {
  id: string;
  token: string;
  tenantId: TenantId;
  role: ApiKeyRole;
}

/** Create a tenant if it does not exist yet (the 'default' tenant always does). */
export async function ensureTestTenant(pool: Pool, tenantId: string): Promise<TenantId> {
  const tenants = new TenantRepository(pool);
  const id = tenantId as TenantId;
  if (!(await tenants.findById(id))) await tenants.create({ id, name: tenantId });
  return id;
}

/** Issue a real API key straight through the service (what the CLI does). */
export async function createTestApiKey(
  pool: Pool,
  options: { tenantId?: string; role?: ApiKeyRole; name?: string } = {},
): Promise<TestApiKey> {
  const tenantId = await ensureTestTenant(pool, options.tenantId ?? DEFAULT_TENANT_ID);
  const role = options.role ?? 'admin';
  const service = new ApiKeyService(new ApiKeyRepository(pool), {
    cacheTtlMs: 0,
    cacheMaxEntries: 1,
  });
  const issued = await service.create({ tenantId, role, name: options.name ?? `test-${role}` });
  return { id: issued.key.id, token: issued.token, tenantId, role };
}

export type Inject = (options: InjectOptions) => Promise<LightMyRequestResponse>;

/** app.inject with `Authorization: Bearer <token>` added (explicit headers win). */
export function authedInject(app: FastifyInstance, token: string): Inject {
  return (options) =>
    app.inject({
      ...options,
      headers: { authorization: `Bearer ${token}`, ...(options.headers ?? {}) },
    });
}
