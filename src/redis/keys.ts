/**
 * Every Redis key and channel PulseQueue uses, in one place.
 *
 * Stats keys carry the queue name as a hash tag (`{queue}`), so a queue's
 * entry and dirty flag live in the same Redis Cluster slot and can be read in
 * one pipeline. The entry key is versioned: a deploy that changes the cached
 * shape starts from empty keys instead of misreading old entries, while the
 * dirty flag (written by every process) is version-independent.
 */
export const STATS_CACHE_VERSION = 1;

export interface RedisKeys {
  /** Pub/sub channel that wakes workers consuming `queue`. */
  wakeupChannel(queue: string): string;
  /** Cached stats snapshot (hash: seq, data). */
  statsEntry(queue: string): string;
  /** Set when the queue changed after the cached snapshot was taken. */
  statsDirty(queue: string): string;
}

export function redisKeys(prefix: string): RedisKeys {
  return {
    wakeupChannel: (queue) => `${prefix}:wakeup:${queue}`,
    statsEntry: (queue) => `${prefix}:stats:v${STATS_CACHE_VERSION}:{${queue}}`,
    statsDirty: (queue) => `${prefix}:stats:dirty:{${queue}}`,
  };
}
