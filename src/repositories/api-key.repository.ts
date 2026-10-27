import type { ApiKey, ApiKeyRole, RateLimit, TenantId } from '../domain/tenant.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';
import { toRateLimit } from './tenant.repository.js';

/** API key as stored, including columns that never leave the service layer. */
export interface StoredApiKey extends ApiKey {
  /** Exact created_at text (UTC, microseconds) for keyset cursors. */
  createdAtCursor: string;
}

/** What authentication needs: the key, its hash and its tenant's limits. */
export interface ApiKeyCredential {
  key: ApiKey;
  secretHash: Buffer;
  tenantRateLimit: RateLimit | null;
}

export interface InsertApiKeyParams {
  tenantId: TenantId;
  name: string;
  prefix: string;
  secretHash: Buffer;
  role: ApiKeyRole;
  expiresAt: Date | null;
}

export interface ListApiKeysParams {
  tenantId: TenantId;
  includeRevoked: boolean;
  limit: number;
  /** Return rows strictly older than this (created_at, id) position. */
  after?: { createdAt: string; id: string };
}

export interface RotateApiKeyParams {
  tenantId: TenantId;
  id: string;
  prefix: string;
  secretHash: Buffer;
  /** 0 revokes the old key now; otherwise it expires after this many ms. */
  graceMs: number;
  expiresAt: Date | null;
}

interface ApiKeyRow {
  id: string;
  tenant_id: string;
  name: string;
  prefix: string;
  role: ApiKeyRole;
  created_at: Date;
  expires_at: Date | null;
  last_used_at: Date | null;
  revoked_at: Date | null;
  rotated_from: string | null;
  created_at_cursor: string;
}

interface CredentialRow extends ApiKeyRow {
  secret_hash: Buffer;
  rate_limit_per_sec: number | null;
  rate_limit_burst: number | null;
}

/** Never selects secret_hash: listings and responses cannot leak it by accident. */
const KEY_COLUMNS = `
  k.id, k.tenant_id, k.name, k.prefix, k.role, k.created_at, k.expires_at,
  k.last_used_at, k.revoked_at, k.rotated_from,
  to_char(k.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS created_at_cursor`;

/** A key authenticates only while it is neither revoked nor expired. */
const ACTIVE = 'k.revoked_at IS NULL AND (k.expires_at IS NULL OR k.expires_at > now())';

function toStoredApiKey(row: ApiKeyRow): StoredApiKey {
  return {
    id: row.id,
    tenantId: row.tenant_id as TenantId,
    name: row.name,
    prefix: row.prefix,
    role: row.role,
    createdAt: row.created_at,
    expiresAt: row.expires_at,
    lastUsedAt: row.last_used_at,
    revokedAt: row.revoked_at,
    rotatedFrom: row.rotated_from,
    createdAtCursor: row.created_at_cursor,
  };
}

export class ApiKeyRepository {
  constructor(private readonly db: Queryable) {}

  async insert(params: InsertApiKeyParams): Promise<StoredApiKey> {
    const rows = await runQuery<ApiKeyRow>(
      this.db,
      `INSERT INTO api_keys AS k (tenant_id, name, prefix, secret_hash, role, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING ${KEY_COLUMNS}`,
      [
        params.tenantId,
        params.name,
        params.prefix,
        params.secretHash,
        params.role,
        params.expiresAt,
      ],
    );
    return toStoredApiKey(rows[0]!);
  }

  /**
   * Authentication lookup by the non-secret prefix (unique index). Returns
   * only active keys; the caller still compares the hash in constant time.
   */
  async findActiveCredential(prefix: string): Promise<ApiKeyCredential | null> {
    const rows = await runQuery<CredentialRow>(
      this.db,
      `SELECT ${KEY_COLUMNS}, k.secret_hash, t.rate_limit_per_sec, t.rate_limit_burst
       FROM api_keys k
       JOIN tenants t ON t.id = k.tenant_id
       WHERE k.prefix = $1 AND ${ACTIVE}`,
      [prefix],
    );
    const row = rows[0];
    if (!row) return null;
    return {
      key: toStoredApiKey(row),
      secretHash: row.secret_hash,
      tenantRateLimit: toRateLimit(row.rate_limit_per_sec, row.rate_limit_burst),
    };
  }

  /** Tenant-scoped: another tenant's key id is indistinguishable from a missing one. */
  async findById(tenantId: TenantId, id: string): Promise<StoredApiKey | null> {
    const rows = await runQuery<ApiKeyRow>(
      this.db,
      `SELECT ${KEY_COLUMNS} FROM api_keys k WHERE k.id = $1::uuid AND k.tenant_id = $2`,
      [id, tenantId],
    );
    return rows[0] ? toStoredApiKey(rows[0]) : null;
  }

  /** Newest first, keyset-paginated on (created_at, id) via api_keys_tenant_created_idx. */
  async list(params: ListApiKeysParams): Promise<StoredApiKey[]> {
    const values: unknown[] = [params.tenantId];
    const where = ['k.tenant_id = $1'];
    if (!params.includeRevoked) where.push('k.revoked_at IS NULL');
    if (params.after !== undefined) {
      values.push(params.after.createdAt, params.after.id);
      where.push(
        `(k.created_at, k.id) < ($${values.length - 1}::timestamptz, $${values.length}::uuid)`,
      );
    }
    values.push(params.limit);
    const rows = await runQuery<ApiKeyRow>(
      this.db,
      `SELECT ${KEY_COLUMNS}
       FROM api_keys k
       WHERE ${where.join(' AND ')}
       ORDER BY k.created_at DESC, k.id DESC
       LIMIT $${values.length}`,
      values,
    );
    return rows.map(toStoredApiKey);
  }

  /** Idempotent: revoking a revoked key keeps the original revoked_at. */
  async revoke(tenantId: TenantId, id: string): Promise<StoredApiKey | null> {
    const rows = await runQuery<ApiKeyRow>(
      this.db,
      `UPDATE api_keys k SET revoked_at = coalesce(k.revoked_at, now())
       WHERE k.id = $1::uuid AND k.tenant_id = $2
       RETURNING ${KEY_COLUMNS}`,
      [id, tenantId],
    );
    return rows[0] ? toStoredApiKey(rows[0]) : null;
  }

  /**
   * Replace an active key in ONE statement: the old key is revoked (grace 0)
   * or shortened to expire after the grace period, and the new key (same
   * tenant, name and role) records rotated_from. Two concurrent rotations of
   * the same key serialize on the old row's lock; the loser re-evaluates the
   * WHERE clause, and with grace 0 finds the key revoked and returns null.
   * Returns null when the key is missing, in another tenant, or inactive.
   */
  async rotate(params: RotateApiKeyParams): Promise<StoredApiKey | null> {
    const rows = await runQuery<ApiKeyRow>(
      this.db,
      `WITH old AS (
         UPDATE api_keys k
         SET revoked_at = CASE WHEN $5::bigint = 0 THEN now() ELSE k.revoked_at END,
             expires_at = CASE WHEN $5::bigint = 0 THEN k.expires_at
                               ELSE LEAST(coalesce(k.expires_at, 'infinity'),
                                          now() + $5::bigint * interval '1 millisecond')
                          END
         WHERE k.id = $1::uuid AND k.tenant_id = $2 AND ${ACTIVE}
         RETURNING k.id, k.tenant_id, k.name, k.role
       )
       INSERT INTO api_keys AS k (tenant_id, name, prefix, secret_hash, role, expires_at, rotated_from)
       SELECT old.tenant_id, old.name, $3, $4, old.role, $6, old.id FROM old
       RETURNING ${KEY_COLUMNS}`,
      [
        params.id,
        params.tenantId,
        params.prefix,
        params.secretHash,
        params.graceMs,
        params.expiresAt,
      ],
    );
    return rows[0] ? toStoredApiKey(rows[0]) : null;
  }

  /**
   * Record use, at most once per `minIntervalMs` per key: a busy key would
   * otherwise turn every authenticated request into a row update. Callers
   * fire and forget; a lost update only makes last_used_at slightly stale.
   */
  async touchLastUsed(id: string, minIntervalMs: number): Promise<void> {
    await runQuery(
      this.db,
      `UPDATE api_keys SET last_used_at = now()
       WHERE id = $1::uuid
         AND (last_used_at IS NULL OR last_used_at < now() - $2::bigint * interval '1 millisecond')`,
      [id, minIntervalMs],
    );
  }
}
