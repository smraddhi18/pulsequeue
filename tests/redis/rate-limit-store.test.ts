import { describe, expect, it } from 'vitest';
import { parseTakeResult } from '../../src/redis/rate-limit-store.js';
import { redisKeys } from '../../src/redis/keys.js';

describe('rate limit store helpers', () => {
  const buckets = [
    { scope: 'tenant' as const, limit: { perSec: 10, burst: 20 } },
    { scope: 'queue' as const, limit: { perSec: 1, burst: 5 } },
  ];

  it('parses the script reply in bucket order', () => {
    expect(parseTakeResult([1, 0, 19, 100, 4, 1000], buckets)).toEqual({
      allowed: true,
      retryAfterMs: 0,
      buckets: [
        { ...buckets[0], remaining: 19, fullInMs: 100 },
        { ...buckets[1], remaining: 4, fullInMs: 1000 },
      ],
    });
  });

  it.each([[null], [[1, 0, 19]], [[1, 0, 'x', 1, 2, 3]]])('rejects malformed replies %j', (raw) => {
    expect(() => parseTakeResult(raw, buckets)).toThrow(/Unexpected token bucket script reply/);
  });

  it('puts tenant and queue buckets in the same cluster slot ({t:<tenant>})', () => {
    const keys = redisKeys('pq');
    expect(keys.rateLimitTenant('acme')).toBe('pq:rl:{t:acme}');
    expect(keys.rateLimitQueue('acme', 'emails')).toBe('pq:rl:{t:acme}:q:emails');
  });
});
