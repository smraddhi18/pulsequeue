import type { TenantId } from '../domain/tenant.js';
import type { BucketRequest, RateLimitStore, TakeResult } from '../services/rate-limiter.js';
import { isRedisReady, type RedisClient } from './client.js';
import type { RedisKeys } from './keys.js';

/**
 * Atomic multi-bucket token bucket.
 *
 * KEYS[i] = bucket hash (fields tokens, ts); ARGV[2i-1] = refill rate per
 * second, ARGV[2i] = capacity (burst). Takes one token from every bucket or
 * from none. Time comes from the Redis server (TIME), never from the API
 * instances, so clock skew between them cannot mint tokens; the script only
 * writes after reading TIME, which effects replication (Redis >= 5) handles.
 *
 * State is lazy: a bucket is refilled arithmetically when touched, and a
 * missing key means "full". The key expires when it would be full again,
 * so idle tenants cost no memory.
 *
 * Returns { allowed, retryAfterMs, remaining_1, fullInMs_1, ... }.
 */
export const TOKEN_BUCKET_SCRIPT = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local n = #KEYS
local levels = {}
local allowed = 1
local retry = 0
for i = 1, n do
  local rate = tonumber(ARGV[2 * i - 1])
  local burst = tonumber(ARGV[2 * i])
  local state = redis.call('HMGET', KEYS[i], 'tokens', 'ts')
  local level = burst
  if state[1] and state[2] then
    local elapsed = math.max(0, now - tonumber(state[2]))
    level = math.min(burst, tonumber(state[1]) + elapsed * rate / 1000)
  end
  levels[i] = level
  if level < 1 then
    allowed = 0
    local wait = math.ceil((1 - level) * 1000 / rate)
    if wait > retry then retry = wait end
  end
end
local out = { allowed, retry }
for i = 1, n do
  local rate = tonumber(ARGV[2 * i - 1])
  local burst = tonumber(ARGV[2 * i])
  local level = levels[i]
  if allowed == 1 then
    level = level - 1
    local full_in = math.ceil((burst - level) * 1000 / rate)
    redis.call('HSET', KEYS[i], 'tokens', string.format('%.6f', level), 'ts', string.format('%d', now))
    redis.call('PEXPIRE', KEYS[i], full_in + 1000)
  end
  out[#out + 1] = math.floor(level)
  out[#out + 1] = math.ceil((burst - level) * 1000 / rate)
end
return out
`;

type TokenBucketClient = RedisClient & {
  pqTokenBucket(numKeys: number, ...keysAndArgs: (string | number)[]): Promise<number[]>;
};

export interface RedisRateLimitStoreOptions {
  client: RedisClient;
  keys: RedisKeys;
}

/** Redis side of the rate limiter: one EVALSHA per enqueue. */
export class RedisRateLimitStore implements RateLimitStore {
  private readonly client: TokenBucketClient;

  constructor(private readonly options: RedisRateLimitStoreOptions) {
    // No numberOfKeys: the bucket count varies, so it is the first argument.
    options.client.defineCommand('pqTokenBucket', { lua: TOKEN_BUCKET_SCRIPT });
    this.client = options.client as TokenBucketClient;
  }

  isAvailable(): boolean {
    return isRedisReady(this.options.client);
  }

  async take(
    tenantId: TenantId,
    queue: string,
    buckets: readonly BucketRequest[],
  ): Promise<TakeResult> {
    const { keys } = this.options;
    const bucketKeys = buckets.map((b) =>
      b.scope === 'tenant' ? keys.rateLimitTenant(tenantId) : keys.rateLimitQueue(tenantId, queue),
    );
    const args = buckets.flatMap((b) => [b.limit.perSec, b.limit.burst]);
    const raw = await this.client.pqTokenBucket(bucketKeys.length, ...bucketKeys, ...args);
    return parseTakeResult(raw, buckets);
  }
}

export function parseTakeResult(raw: unknown, buckets: readonly BucketRequest[]): TakeResult {
  if (!Array.isArray(raw) || raw.length !== 2 + 2 * buckets.length) {
    throw new Error('Unexpected token bucket script reply');
  }
  const num = (i: number): number => {
    const v = Number(raw[i]);
    if (!Number.isFinite(v)) throw new Error('Unexpected token bucket script reply');
    return v;
  };
  return {
    allowed: num(0) === 1,
    retryAfterMs: num(1),
    buckets: buckets.map((b, i) => ({
      scope: b.scope,
      limit: b.limit,
      remaining: num(2 + 2 * i),
      fullInMs: num(3 + 2 * i),
    })),
  };
}
