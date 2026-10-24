import type { QueueName } from '../domain/job.js';
import { STATS_STATUSES, type QueueStats, type QueueStatusCounts } from '../domain/stats.js';
import type { CachedQueueStats, StatsCacheStore } from '../services/stats-cache.js';
import { isRedisReady, type RedisClient } from './client.js';
import type { RedisKeys } from './keys.js';

/**
 * Compare-and-set by snapshot time. KEYS[1] = entry hash; ARGV = seq
 * (snapshot epoch ms), data (JSON), ttl (ms). Equal seq overwrites (same
 * snapshot instant), older is rejected, so a slow reader that computed
 * before a newer write can never roll the cache back.
 */
const CAS_WRITE_SCRIPT = `
local current = redis.call('HGET', KEYS[1], 'seq')
if current and tonumber(current) > tonumber(ARGV[1]) then
  return 0
end
redis.call('HSET', KEYS[1], 'seq', ARGV[1], 'data', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`;

type CasClient = RedisClient & {
  pqStatsCasWrite(key: string, seq: string, data: string, ttlMs: string): Promise<number>;
};

/** JSON shape of a cached entry (dates as ISO strings). */
interface SerializedEntry {
  cachedAt: number;
  stats: {
    queue: string;
    counts: QueueStatusCounts;
    total: number;
    oldestQueuedAt: string | null;
    nextScheduledAt: string | null;
    oldestRunningSince: string | null;
    computedAt: string;
  };
}

export function serializeStatsEntry(entry: CachedQueueStats): string {
  const { stats } = entry;
  const body: SerializedEntry = {
    cachedAt: entry.cachedAt,
    stats: {
      queue: stats.queue,
      counts: stats.counts,
      total: stats.total,
      oldestQueuedAt: stats.oldestQueuedAt?.toISOString() ?? null,
      nextScheduledAt: stats.nextScheduledAt?.toISOString() ?? null,
      oldestRunningSince: stats.oldestRunningSince?.toISOString() ?? null,
      computedAt: stats.computedAt.toISOString(),
    },
  };
  return JSON.stringify(body);
}

/** Returns null for anything unreadable: a corrupt entry is a miss, not an error. */
export function deserializeStatsEntry(raw: string): CachedQueueStats | null {
  try {
    const body = JSON.parse(raw) as SerializedEntry;
    const { stats } = body;
    if (typeof body.cachedAt !== 'number' || typeof stats?.queue !== 'string') return null;
    const counts = {} as QueueStatusCounts;
    for (const status of STATS_STATUSES) {
      const n = stats.counts?.[status];
      if (typeof n !== 'number') return null;
      counts[status] = n;
    }
    const computedAt = new Date(stats.computedAt);
    if (Number.isNaN(computedAt.getTime())) return null;
    const date = (v: string | null) => (v === null ? null : new Date(v));
    const result: QueueStats = {
      queue: stats.queue as QueueName,
      counts,
      total: stats.total,
      oldestQueuedAt: date(stats.oldestQueuedAt),
      nextScheduledAt: date(stats.nextScheduledAt),
      oldestRunningSince: date(stats.oldestRunningSince),
      computedAt,
    };
    return { cachedAt: body.cachedAt, stats: result };
  } catch {
    return null;
  }
}

export interface RedisStatsCacheStoreOptions {
  client: RedisClient;
  keys: RedisKeys;
  /** STATS_CACHE_TTL_MS: expiry of entries and of dirty flags. */
  ttlMs: number;
}

/**
 * Redis side of the stats cache: per queue an entry hash (seq + data, with
 * TTL) and a dirty flag (also with TTL, so a flag can never outlive the
 * entry it refers to by more than one TTL). Both keys share a hash tag, so
 * the read is one pipelined round-trip even on Redis Cluster.
 */
export class RedisStatsCacheStore implements StatsCacheStore {
  private readonly client: CasClient;

  constructor(private readonly options: RedisStatsCacheStoreOptions) {
    options.client.defineCommand('pqStatsCasWrite', {
      numberOfKeys: 1,
      lua: CAS_WRITE_SCRIPT,
    });
    this.client = options.client as CasClient;
  }

  isAvailable(): boolean {
    return isRedisReady(this.options.client);
  }

  async read(queue: string): Promise<{ entry: CachedQueueStats | null; dirty: boolean }> {
    const { keys } = this.options;
    const results = await this.client
      .pipeline()
      .hget(keys.statsEntry(queue), 'data')
      .exists(keys.statsDirty(queue))
      .exec();
    if (!results) throw new Error('Redis pipeline was aborted');
    const [[entryErr, data], [dirtyErr, dirty]] = results as [
      [Error | null, string | null],
      [Error | null, number],
    ];
    if (entryErr) throw entryErr;
    if (dirtyErr) throw dirtyErr;
    return {
      entry: data === null ? null : deserializeStatsEntry(data),
      dirty: dirty === 1,
    };
  }

  async claimRefresh(queue: string): Promise<boolean> {
    return (await this.client.del(this.options.keys.statsDirty(queue))) === 1;
  }

  async write(queue: string, entry: CachedQueueStats): Promise<boolean> {
    const result = await this.client.pqStatsCasWrite(
      this.options.keys.statsEntry(queue),
      String(entry.stats.computedAt.getTime()),
      serializeStatsEntry(entry),
      String(this.options.ttlMs),
    );
    return result === 1;
  }
}
