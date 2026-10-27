import { describe, expect, it } from 'vitest';
import { TtlCache } from '../src/lib/ttl-cache.js';

describe('TtlCache', () => {
  it('returns values until their TTL passes', () => {
    let now = 0;
    const cache = new TtlCache<string, number>({ ttlMs: 100, maxEntries: 10, now: () => now });
    cache.set('a', 1);
    now = 99;
    expect(cache.get('a')).toBe(1);
    now = 100;
    expect(cache.get('a')).toBeUndefined();
    expect(cache.size).toBe(0);
  });

  it('evicts the least recently used entry when full', () => {
    const cache = new TtlCache<string, number>({ ttlMs: 1_000, maxEntries: 2, now: () => 0 });
    cache.set('a', 1);
    cache.set('b', 2);
    expect(cache.get('a')).toBe(1); // a is now most recent
    cache.set('c', 3);
    expect(cache.get('b')).toBeUndefined();
    expect(cache.get('a')).toBe(1);
    expect(cache.get('c')).toBe(3);
  });

  it('caches nothing when the TTL is 0', () => {
    const cache = new TtlCache<string, number>({ ttlMs: 0, maxEntries: 10 });
    cache.set('a', 1);
    expect(cache.get('a')).toBeUndefined();
  });

  it('deletes by predicate and clears', () => {
    const cache = new TtlCache<string, { owner: string }>({ ttlMs: 1_000, maxEntries: 10 });
    cache.set('a', { owner: 'x' });
    cache.set('b', { owner: 'y' });
    cache.set('c', { owner: 'x' });
    expect(cache.deleteWhere((v) => v.owner === 'x')).toBe(2);
    expect(cache.size).toBe(1);
    cache.clear();
    expect(cache.size).toBe(0);
  });
});
