import { describe, expect, it } from 'vitest';
import { reconnectDelay } from '../../src/redis/client.js';
import { redisKeys, STATS_CACHE_VERSION } from '../../src/redis/keys.js';
import { decodeWakeupMessage, encodeWakeupMessage } from '../../src/redis/wakeup-publisher.js';

describe('reconnectDelay (equal jitter)', () => {
  it('doubles the cap per attempt up to the max', () => {
    const top = (n: number) => reconnectDelay(n, 100, 5_000, () => 1);
    expect([1, 2, 3, 4, 5, 6, 7, 8].map(top)).toEqual([
      100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000,
    ]);
  });

  it('stays within [cap/2, cap]: never zero, so a dead server is not hammered', () => {
    expect(reconnectDelay(1, 100, 5_000, () => 0)).toBe(50);
    expect(reconnectDelay(10, 100, 5_000, () => 0)).toBe(2_500);
    for (let i = 0; i < 100; i++) {
      const d = reconnectDelay(4, 100, 5_000);
      expect(d).toBeGreaterThanOrEqual(400);
      expect(d).toBeLessThanOrEqual(800);
    }
  });

  it('survives absurd attempt numbers without overflowing', () => {
    expect(reconnectDelay(10_000, 100, 5_000, () => 1)).toBe(5_000);
    expect(reconnectDelay(0, 100, 5_000, () => 1)).toBe(100);
  });
});

describe('redisKeys', () => {
  it('namespaces channels and keys by prefix and hash-tags both stats keys by queue', () => {
    const keys = redisKeys('pq');
    expect(keys.wakeupChannel('emails')).toBe('pq:wakeup:emails');
    expect(keys.statsEntry('emails')).toBe(`pq:stats:v${STATS_CACHE_VERSION}:{emails}`);
    // Same {hash tag}: entry and flag land in one Redis Cluster slot, so the
    // pipelined read stays a single-node operation.
    expect(keys.statsDirty('emails')).toBe('pq:stats:dirty:{emails}');
  });
});

describe('wake-up messages', () => {
  it('round-trips', () => {
    const message = { queue: 'emails', sentAt: 1_700_000_000_000, source: 'api:h-1' };
    expect(decodeWakeupMessage(encodeWakeupMessage(message))).toEqual(message);
  });

  it('decodes garbage leniently (a wake-up is just a hint)', () => {
    expect(decodeWakeupMessage('not json')).toEqual({});
    expect(decodeWakeupMessage('null')).toEqual({});
    expect(decodeWakeupMessage('42')).toEqual({});
  });
});
