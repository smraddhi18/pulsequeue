/**
 * Every Redis key and channel PulseQueue uses, in one place.
 *
 * Stats keys carry the queue as a hash tag (`{tenant:queue}`, see
 * queueRefKey), so a queue's entry and dirty flag live in the same Redis
 * Cluster slot and can be read in one pipeline. Since M6 the tag includes the
 * owning tenant (two tenants may both have an `emails` queue); during a
 * rolling upgrade, invalidations from M5 processes use the old tag, so an
 * M6 reader may serve a snapshot up to STATS_CACHE_TTL_MS old. The entry key is versioned: a deploy that changes the cached
 * shape starts from empty keys instead of misreading old entries, while the
 * dirty flag (written by every process) is version-independent.
 */
export const STATS_CACHE_VERSION = 1;

export interface RedisKeys {
  /** Pub/sub channel that wakes workers consuming `queue` (a name: workers serve every tenant). */
  wakeupChannel(queue: string): string;
  /** Cached stats snapshot (hash: seq, data); `queue` is a queueRefKey. */
  statsEntry(queue: string): string;
  /** Set when the queue changed after the cached snapshot was taken. */
  statsDirty(queue: string): string;
  /**
   * Token bucket of a tenant (M6). Tenant and queue buckets share the
   * `{t:<tenant>}` hash tag, so one script can update both atomically on
   * Redis Cluster.
   */
  rateLimitTenant(tenantId: string): string;
  /** Token bucket of one queue of a tenant. */
  rateLimitQueue(tenantId: string, queue: string): string;
}

export function redisKeys(prefix: string): RedisKeys {
  return {
    wakeupChannel: (queue) => `${prefix}:wakeup:${queue}`,
    statsEntry: (queue) => `${prefix}:stats:v${STATS_CACHE_VERSION}:{${queue}}`,
    statsDirty: (queue) => `${prefix}:stats:dirty:{${queue}}`,
    rateLimitTenant: (tenantId) => `${prefix}:rl:{t:${tenantId}}`,
    rateLimitQueue: (tenantId, queue) => `${prefix}:rl:{t:${tenantId}}:q:${queue}`,
  };
}
