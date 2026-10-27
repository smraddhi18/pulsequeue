import type { RateLimit, Tenant, TenantId } from '../domain/tenant.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';

interface TenantRow {
  id: string;
  name: string;
  rate_limit_per_sec: number | null;
  rate_limit_burst: number | null;
  created_at: Date;
  updated_at: Date;
}

const TENANT_COLUMNS = 'id, name, rate_limit_per_sec, rate_limit_burst, created_at, updated_at';

export function toRateLimit(perSec: number | null, burst: number | null): RateLimit | null {
  return perSec !== null && burst !== null ? { perSec, burst } : null;
}

function toTenant(row: TenantRow): Tenant {
  return {
    id: row.id as TenantId,
    name: row.name,
    rateLimit: toRateLimit(row.rate_limit_per_sec, row.rate_limit_burst),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

/**
 * Tenants are provisioned by operators (the api-key CLI), not through the
 * public API: creating a tenant is creating a customer.
 */
export class TenantRepository {
  constructor(private readonly db: Queryable) {}

  async findById(id: TenantId): Promise<Tenant | null> {
    const rows = await runQuery<TenantRow>(
      this.db,
      `SELECT ${TENANT_COLUMNS} FROM tenants WHERE id = $1`,
      [id],
    );
    return rows[0] ? toTenant(rows[0]) : null;
  }

  /** Insert; a duplicate id surfaces as ConflictError (tenants_pkey). */
  async create(params: { id: TenantId; name: string }): Promise<Tenant> {
    const rows = await runQuery<TenantRow>(
      this.db,
      `INSERT INTO tenants (id, name) VALUES ($1, $2) RETURNING ${TENANT_COLUMNS}`,
      [params.id, params.name],
    );
    return toTenant(rows[0]!);
  }

  /** Set (or clear, with null) the tenant's enqueue rate-limit override. */
  async setRateLimit(id: TenantId, rateLimit: RateLimit | null): Promise<Tenant | null> {
    const rows = await runQuery<TenantRow>(
      this.db,
      `UPDATE tenants SET rate_limit_per_sec = $2, rate_limit_burst = $3
       WHERE id = $1
       RETURNING ${TENANT_COLUMNS}`,
      [id, rateLimit?.perSec ?? null, rateLimit?.burst ?? null],
    );
    return rows[0] ? toTenant(rows[0]) : null;
  }
}
