import { createHash } from 'node:crypto';

/**
 * Deterministic JSON: object keys sorted recursively, arrays kept in order.
 * `{"a":1,"b":2}` and `{"b":2,"a":1}` describe the same request and must
 * produce the same fingerprint; JSON.stringify alone depends on key order.
 */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    // Primitives (and undefined -> "null" so the output is always valid JSON).
    return JSON.stringify(value) ?? 'null';
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => canonicalJson(v === undefined ? null : v)).join(',')}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(',')}}`;
}

const FINGERPRINT_VERSION = 'v1';

/**
 * SHA-256 over the canonical form of the *normalized* request (defaults
 * applied), prefixed with a version so the algorithm can evolve. Omitting
 * `priority` and sending the default explicitly are the same request.
 */
export function requestFingerprint(normalizedRequest: Record<string, unknown>): string {
  const digest = createHash('sha256')
    .update(canonicalJson(normalizedRequest), 'utf8')
    .digest('hex');
  return `${FINGERPRINT_VERSION}:${digest}`;
}
