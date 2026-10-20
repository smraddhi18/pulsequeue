import { describe, expect, it } from 'vitest';
import { canonicalJson, requestFingerprint } from '../src/lib/fingerprint.js';

describe('canonicalJson', () => {
  it('sorts object keys recursively', () => {
    expect(canonicalJson({ b: 1, a: { d: [3, { z: 1, y: 2 }], c: null } })).toBe(
      '{"a":{"c":null,"d":[3,{"y":2,"z":1}]},"b":1}',
    );
  });

  it('keeps array order (order is meaningful)', () => {
    expect(canonicalJson([2, 1])).not.toBe(canonicalJson([1, 2]));
  });

  it('matches JSON.stringify for primitives and drops undefined object members', () => {
    expect(canonicalJson('x"y')).toBe(JSON.stringify('x"y'));
    expect(canonicalJson(1.5)).toBe('1.5');
    expect(canonicalJson(null)).toBe('null');
    expect(canonicalJson({ a: undefined, b: 1 })).toBe('{"b":1}');
    expect(canonicalJson([undefined])).toBe('[null]');
  });

  it('produces valid JSON that parses back to an equal value', () => {
    const value = { z: [1, 'two', { three: true }], a: { nested: { deep: 'ü€' } } };
    expect(JSON.parse(canonicalJson(value))).toEqual(value);
  });
});

describe('requestFingerprint', () => {
  const base = { payload: { to: 'a@b.c', n: 1 }, priority: 0, delayMs: 0, maxAttempts: 3 };

  it('is stable regardless of key order', () => {
    const reordered = { maxAttempts: 3, delayMs: 0, priority: 0, payload: { n: 1, to: 'a@b.c' } };
    expect(requestFingerprint(reordered)).toBe(requestFingerprint(base));
  });

  it('is versioned sha256 hex', () => {
    expect(requestFingerprint(base)).toMatch(/^v1:[0-9a-f]{64}$/);
  });

  it('changes when any field changes', () => {
    const fp = requestFingerprint(base);
    expect(requestFingerprint({ ...base, priority: 1 })).not.toBe(fp);
    expect(requestFingerprint({ ...base, delayMs: 1 })).not.toBe(fp);
    expect(requestFingerprint({ ...base, maxAttempts: 4 })).not.toBe(fp);
    expect(requestFingerprint({ ...base, payload: { to: 'a@b.c', n: 2 } })).not.toBe(fp);
  });

  it('distinguishes values that stringify alike under naive concatenation', () => {
    expect(requestFingerprint({ payload: '1', priority: 0 })).not.toBe(
      requestFingerprint({ payload: 1, priority: 0 }),
    );
  });
});
