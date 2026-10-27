/**
 * Tenants, API keys and the authenticated principal (Milestone 6).
 *
 * A tenant owns queues (and through them jobs, schedules and dead letters).
 * Every API request authenticates with a tenant-scoped API key; the key's
 * role decides what it may do inside its tenant. Nothing is ever shared
 * across tenants, and a resource in another tenant is reported as missing
 * (404), never as forbidden, so ids and queue names do not leak.
 */

/** Short immutable slug; mirrors the tenants_id_format CHECK in 0005. */
export const TENANT_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/;

/** The tenant every pre-M6 queue belongs to (seeded by migration 0005). */
export const DEFAULT_TENANT_ID = 'default' as TenantId;

export type TenantId = string & { readonly __brand: 'TenantId' };

export const API_KEY_ROLES = ['admin', 'producer', 'reader'] as const;
export type ApiKeyRole = (typeof API_KEY_ROLES)[number];

/**
 * What a route needs. Roles are fixed bundles of permissions so the policy
 * lives in one table instead of being scattered across route handlers.
 */
export type Permission = 'read' | 'enqueue' | 'manage';

const ROLE_PERMISSIONS: Readonly<Record<ApiKeyRole, ReadonlySet<Permission>>> = {
  admin: new Set<Permission>(['read', 'enqueue', 'manage']),
  producer: new Set<Permission>(['read', 'enqueue']),
  reader: new Set<Permission>(['read']),
};

export function roleAllows(role: ApiKeyRole, permission: Permission): boolean {
  return ROLE_PERMISSIONS[role].has(permission);
}

/** Token-bucket parameters; `burst` is the bucket size, `perSec` the refill rate. */
export interface RateLimit {
  perSec: number;
  burst: number;
}

export interface Tenant {
  id: TenantId;
  name: string;
  /** null = the RATE_LIMIT_TENANT_* defaults. */
  rateLimit: RateLimit | null;
  createdAt: Date;
  updatedAt: Date;
}

export const API_KEY_LIMITS = {
  NAME_MAX_LENGTH: 100,
  /** Longest overlap a rotation may give the old key. */
  MAX_ROTATION_GRACE_SECONDS: 7 * 24 * 60 * 60,
  DEFAULT_LIST_LIMIT: 50,
  MAX_LIST_LIMIT: 200,
} as const;

/** API key metadata. The secret is never stored and never returned after creation. */
export interface ApiKey {
  id: string;
  tenantId: TenantId;
  name: string;
  /** Non-secret identifier shown in listings and logs (`pq_<prefix>_...`). */
  prefix: string;
  role: ApiKeyRole;
  createdAt: Date;
  expiresAt: Date | null;
  lastUsedAt: Date | null;
  revokedAt: Date | null;
  /** The key this one replaced, when it was created by a rotation. */
  rotatedFrom: string | null;
}

/** An API key together with its plaintext token: only ever returned once. */
export interface IssuedApiKey {
  key: ApiKey;
  token: string;
}

/** Who is calling: attached to every authenticated request. */
export interface Principal {
  tenantId: TenantId;
  keyId: string;
  keyPrefix: string;
  role: ApiKeyRole;
  /** The tenant's own enqueue limit override (null = server defaults). */
  tenantRateLimit: RateLimit | null;
}

export interface CreateApiKeyInput {
  tenantId: TenantId;
  name: string;
  role: ApiKeyRole;
  expiresAt?: Date;
}

export interface ListApiKeysInput {
  tenantId: TenantId;
  includeRevoked: boolean;
  limit: number;
  cursor?: string;
}

export interface ApiKeyPage {
  keys: ApiKey[];
  nextCursor: string | null;
}

export interface RotateApiKeyInput {
  tenantId: TenantId;
  id: string;
  /** How long the old key keeps working; 0 revokes it immediately. */
  gracePeriodSeconds: number;
  expiresAt?: Date;
}
