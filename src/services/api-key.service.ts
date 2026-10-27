import { ConflictError, NotFoundError, UnauthorizedError } from '../domain/errors.js';
import type {
  ApiKey,
  ApiKeyPage,
  CreateApiKeyInput,
  IssuedApiKey,
  ListApiKeysInput,
  Principal,
  RotateApiKeyInput,
  TenantId,
} from '../domain/tenant.js';
import {
  apiKeyHashesEqual,
  bearerToken,
  generateApiKeyToken,
  hashApiKeyToken,
  parseApiKeyToken,
  type GeneratedToken,
} from '../lib/api-key-token.js';
import { decodeCursor, encodeCursor } from '../lib/cursor.js';
import type { StructuredLogger } from '../lib/logger.js';
import { TtlCache } from '../lib/ttl-cache.js';
import type { ApiKeyRepository, StoredApiKey } from '../repositories/api-key.repository.js';

export type ApiKeyStore = Pick<
  ApiKeyRepository,
  'insert' | 'findActiveCredential' | 'findById' | 'list' | 'revoke' | 'rotate' | 'touchLastUsed'
>;

export interface ApiKeyServiceOptions {
  /** How long a verified key is trusted without asking Postgres (0 disables). */
  cacheTtlMs: number;
  cacheMaxEntries: number;
  /** Minimum gap between last_used_at writes for one key. */
  touchIntervalMs?: number;
  logger?: StructuredLogger;
  now?: () => number;
}

interface CachedPrincipal {
  principal: Principal;
  /** Key expiry (ms epoch) or null: a cached key still stops at its expiry. */
  expiresAt: number | null;
  lastTouchAt: number;
}

const DEFAULT_TOUCH_INTERVAL_MS = 60_000;
/** Prefix collisions are ~2^-62 per pair; a retry or two is plenty. */
const MAX_PREFIX_ATTEMPTS = 3;

/**
 * API key lifecycle and request authentication.
 *
 * Authentication is one indexed lookup by prefix plus a constant-time hash
 * comparison, fronted by a short per-process cache so steady traffic does
 * not hit Postgres on every request. The cache holds only successful
 * verifications (an attacker spraying random keys cannot fill it with
 * negatives) and is keyed by the token's SHA-256, never the token.
 * Revocation evicts locally at once; other API instances notice within
 * cacheTtlMs (the documented bound on revocation lag).
 */
export class ApiKeyService {
  private readonly cache: TtlCache<string, CachedPrincipal>;

  constructor(
    private readonly keys: ApiKeyStore,
    private readonly options: ApiKeyServiceOptions,
  ) {
    this.cache = new TtlCache({
      ttlMs: options.cacheTtlMs,
      maxEntries: options.cacheMaxEntries,
      now: () => this.now(),
    });
  }

  /**
   * Resolve an Authorization header to a principal, or throw 401. The
   * messages say what is wrong with the header, never whether a well-formed
   * key exists, was revoked or expired.
   */
  async authenticate(authorization: string | undefined): Promise<Principal> {
    if (authorization === undefined || authorization.trim() === '') {
      throw new UnauthorizedError('Missing API key: send Authorization: Bearer <key>');
    }
    const raw = bearerToken(authorization);
    const parsed = raw === null ? null : parseApiKeyToken(raw);
    if (!parsed) {
      throw new UnauthorizedError('Malformed API key: expected Authorization: Bearer pq_...');
    }

    const hash = hashApiKeyToken(parsed.token);
    const cacheKey = hash.toString('hex');
    const now = this.now();

    const cached = this.cache.get(cacheKey);
    if (cached) {
      if (cached.expiresAt !== null && cached.expiresAt <= now) {
        this.cache.delete(cacheKey);
      } else {
        if (now - cached.lastTouchAt >= this.touchIntervalMs()) {
          cached.lastTouchAt = now;
          this.touch(cached.principal.keyId);
        }
        return cached.principal;
      }
    }

    const credential = await this.keys.findActiveCredential(parsed.prefix);
    if (!credential || !apiKeyHashesEqual(credential.secretHash, hash)) {
      throw new UnauthorizedError('Invalid API key');
    }

    const { key } = credential;
    const principal: Principal = {
      tenantId: key.tenantId,
      keyId: key.id,
      keyPrefix: key.prefix,
      role: key.role,
      tenantRateLimit: credential.tenantRateLimit,
    };
    this.cache.set(cacheKey, {
      principal,
      expiresAt: key.expiresAt?.getTime() ?? null,
      lastTouchAt: now,
    });
    this.touch(key.id);
    return principal;
  }

  /** Issue a key; the plaintext token is in the result and nowhere else, ever. */
  async create(input: CreateApiKeyInput): Promise<IssuedApiKey> {
    const stored = await this.withFreshPrefix((generated) =>
      this.keys.insert({
        tenantId: input.tenantId,
        name: input.name,
        prefix: generated.prefix,
        secretHash: generated.secretHash,
        role: input.role,
        expiresAt: input.expiresAt ?? null,
      }),
    );
    this.options.logger?.info(
      { event: 'apikey.created', tenantId: input.tenantId, keyId: stored.key.id, role: input.role },
      'API key created',
    );
    return { key: toApiKey(stored.key), token: stored.token };
  }

  async list(input: ListApiKeysInput): Promise<ApiKeyPage> {
    const after = input.cursor !== undefined ? decodeCursor(input.cursor) : undefined;
    const rows = await this.keys.list({
      tenantId: input.tenantId,
      includeRevoked: input.includeRevoked,
      limit: input.limit + 1,
      ...(after !== undefined ? { after } : {}),
    });
    const page = rows.slice(0, input.limit);
    const last = page[page.length - 1];
    const nextCursor =
      rows.length > input.limit && last
        ? encodeCursor({ createdAt: last.createdAtCursor, id: last.id })
        : null;
    return { keys: page.map(toApiKey), nextCursor };
  }

  async get(tenantId: TenantId, id: string): Promise<ApiKey> {
    const key = await this.keys.findById(tenantId, id);
    if (!key) throw new NotFoundError('API key not found', { id });
    return toApiKey(key);
  }

  /** Idempotent. Takes effect on this instance at once, on others within cacheTtlMs. */
  async revoke(tenantId: TenantId, id: string): Promise<ApiKey> {
    const key = await this.keys.revoke(tenantId, id);
    if (!key) throw new NotFoundError('API key not found', { id });
    this.evict(id);
    this.options.logger?.info({ event: 'apikey.revoked', tenantId, keyId: id }, 'API key revoked');
    return toApiKey(key);
  }

  /**
   * Issue a replacement with the same name and role. The old key is revoked
   * at once (grace 0) or keeps working for the grace period, so clients can
   * roll the new secret out without downtime.
   */
  async rotate(input: RotateApiKeyInput): Promise<IssuedApiKey> {
    const rotated = await this.withFreshPrefix((generated) =>
      this.keys.rotate({
        tenantId: input.tenantId,
        id: input.id,
        prefix: generated.prefix,
        secretHash: generated.secretHash,
        graceMs: input.gracePeriodSeconds * 1000,
        expiresAt: input.expiresAt ?? null,
      }),
    );
    if (!rotated.key) {
      const current = await this.keys.findById(input.tenantId, input.id);
      if (!current) throw new NotFoundError('API key not found', { id: input.id });
      throw new ConflictError(
        'Only an active API key can be rotated (this one is revoked or expired)',
        {
          id: input.id,
        },
      );
    }
    // The old key's cached entry would outlive a shortened expiry; drop it.
    this.evict(input.id);
    this.options.logger?.info(
      {
        event: 'apikey.rotated',
        tenantId: input.tenantId,
        keyId: rotated.key.id,
        rotatedFrom: input.id,
        gracePeriodSeconds: input.gracePeriodSeconds,
      },
      'API key rotated',
    );
    return { key: toApiKey(rotated.key), token: rotated.token };
  }

  /** Drop every cached verification (tests, and a hook for future push invalidation). */
  clearCache(): void {
    this.cache.clear();
  }

  private evict(keyId: string): void {
    this.cache.deleteWhere((entry) => entry.principal.keyId === keyId);
  }

  private async withFreshPrefix<T>(
    write: (generated: GeneratedToken) => Promise<T>,
  ): Promise<{ key: T; token: string }> {
    for (let attempt = 1; ; attempt++) {
      const generated = generateApiKeyToken();
      try {
        return { key: await write(generated), token: generated.token };
      } catch (err) {
        const collided =
          err instanceof ConflictError &&
          (err.details as { constraint?: unknown } | undefined)?.constraint ===
            'api_keys_prefix_uq';
        if (!collided || attempt >= MAX_PREFIX_ATTEMPTS) throw err;
      }
    }
  }

  private touch(keyId: string): void {
    this.keys.touchLastUsed(keyId, this.touchIntervalMs()).catch((err: unknown) => {
      this.options.logger?.warn(
        { event: 'apikey.touch_failed', keyId, err },
        'Could not record API key use',
      );
    });
  }

  private touchIntervalMs(): number {
    return this.options.touchIntervalMs ?? DEFAULT_TOUCH_INTERVAL_MS;
  }

  private now(): number {
    return (this.options.now ?? Date.now)();
  }
}

/** Strip repository-only fields so they can never reach a response. */
function toApiKey(stored: StoredApiKey): ApiKey {
  return {
    id: stored.id,
    tenantId: stored.tenantId,
    name: stored.name,
    prefix: stored.prefix,
    role: stored.role,
    createdAt: stored.createdAt,
    expiresAt: stored.expiresAt,
    lastUsedAt: stored.lastUsedAt,
    revokedAt: stored.revokedAt,
    rotatedFrom: stored.rotatedFrom,
  };
}
