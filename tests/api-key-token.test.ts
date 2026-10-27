import { describe, expect, it } from 'vitest';
import {
  API_KEY_PREFIX_LENGTH,
  apiKeyHashesEqual,
  bearerToken,
  generateApiKeyToken,
  hashApiKeyToken,
  parseApiKeyToken,
} from '../src/lib/api-key-token.js';

describe('API key tokens', () => {
  it('generates pq_<prefix>_<secret> with a 12-char prefix and a 256-bit secret', () => {
    const { token, prefix, secretHash } = generateApiKeyToken();
    expect(token).toMatch(/^pq_[a-z0-9]{12}_[A-Za-z0-9_-]{43}$/);
    expect(prefix).toHaveLength(API_KEY_PREFIX_LENGTH);
    expect(token.startsWith(`pq_${prefix}_`)).toBe(true);
    expect(Buffer.from(token.split('_').slice(2).join('_'), 'base64url')).toHaveLength(32);
    expect(secretHash).toHaveLength(32);
    expect(secretHash.equals(hashApiKeyToken(token))).toBe(true);
  });

  it('never repeats a prefix or token across many generations', () => {
    const prefixes = new Set<string>();
    const tokens = new Set<string>();
    for (let i = 0; i < 2_000; i++) {
      const generated = generateApiKeyToken();
      prefixes.add(generated.prefix);
      tokens.add(generated.token);
    }
    expect(prefixes.size).toBe(2_000);
    expect(tokens.size).toBe(2_000);
  });

  it('uses the whole prefix alphabet (no stuck symbols)', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 500; i++) for (const ch of generateApiKeyToken().prefix) seen.add(ch);
    expect(seen.size).toBe(36);
  });

  it('parses a generated token back to its prefix', () => {
    const { token, prefix } = generateApiKeyToken();
    expect(parseApiKeyToken(token)).toEqual({ prefix, token });
  });

  it.each([
    ['empty', ''],
    ['wrong marker', 'pk_abcdefghijkl_' + 'a'.repeat(43)],
    ['short prefix', 'pq_abc_' + 'a'.repeat(43)],
    ['uppercase prefix', 'pq_ABCDEFGHIJKL_' + 'a'.repeat(43)],
    ['short secret', 'pq_abcdefghijkl_' + 'a'.repeat(42)],
    ['long secret', 'pq_abcdefghijkl_' + 'a'.repeat(44)],
    ['bad secret char', 'pq_abcdefghijkl_' + 'a'.repeat(42) + '='],
    ['trailing space', 'pq_abcdefghijkl_' + 'a'.repeat(43) + ' '],
  ])('rejects a malformed token (%s)', (_label, raw) => {
    expect(parseApiKeyToken(raw)).toBeNull();
  });

  it('compares digests in constant time and by value', () => {
    const a = hashApiKeyToken('pq_a');
    expect(apiKeyHashesEqual(a, hashApiKeyToken('pq_a'))).toBe(true);
    expect(apiKeyHashesEqual(a, hashApiKeyToken('pq_b'))).toBe(false);
    expect(apiKeyHashesEqual(a, a.subarray(0, 16))).toBe(false);
  });

  it.each([
    ['Bearer abc', 'abc'],
    ['bearer abc', 'abc'],
    ['BEARER   abc  ', 'abc'],
  ])('extracts the token from %j', (header, expected) => {
    expect(bearerToken(header)).toBe(expected);
  });

  it.each([[undefined], [''], ['Basic abc'], ['Bearer'], ['Bearer a b'], ['Bearerabc']])(
    'returns null for a non-Bearer header %j',
    (header) => {
      expect(bearerToken(header)).toBeNull();
    },
  );
});
