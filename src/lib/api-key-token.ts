import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';

/**
 * API key tokens: `pq_<prefix>_<secret>`.
 *
 * - prefix: 12 chars of [a-z0-9] (~62 bits), random, NOT secret. It is the
 *   database lookup key and the identifier shown in listings and logs.
 * - secret: 32 bytes from the CSPRNG, base64url (43 chars, 256 bits).
 *
 * Only SHA-256(token) is stored. A slow password hash (bcrypt/argon2) buys
 * nothing here: the secret is uniformly random with 256 bits of entropy, so
 * an attacker holding the hash cannot brute-force it, and verification must
 * stay cheap because it runs on every request. The constant `pq_` marker
 * makes leaked keys easy for secret scanners to spot.
 */

const TOKEN_PATTERN = /^pq_([a-z0-9]{12})_([A-Za-z0-9_-]{43})$/;
const PREFIX_ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';
export const API_KEY_PREFIX_LENGTH = 12;
const SECRET_BYTES = 32;

export interface GeneratedToken {
  token: string;
  prefix: string;
  /** SHA-256 of the full token: the only thing persisted. */
  secretHash: Buffer;
}

export interface ParsedToken {
  prefix: string;
  token: string;
}

export function generateApiKeyToken(): GeneratedToken {
  const prefix = randomPrefix();
  const secret = randomBytes(SECRET_BYTES).toString('base64url');
  const token = `pq_${prefix}_${secret}`;
  return { token, prefix, secretHash: hashApiKeyToken(token) };
}

/** Syntactic check only; returns null for anything that cannot be a key. */
export function parseApiKeyToken(raw: string): ParsedToken | null {
  const match = TOKEN_PATTERN.exec(raw);
  if (!match) return null;
  return { prefix: match[1]!, token: raw };
}

export function hashApiKeyToken(token: string): Buffer {
  return createHash('sha256').update(token, 'utf8').digest();
}

/** Constant-time comparison of two SHA-256 digests. */
export function apiKeyHashesEqual(a: Buffer, b: Buffer): boolean {
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * Extract the token from an `Authorization: Bearer <token>` header value.
 * The scheme is case-insensitive (RFC 7235); anything else returns null.
 */
export function bearerToken(header: string | undefined): string | null {
  if (header === undefined) return null;
  const match = /^Bearer[ ]+(\S+)[ ]*$/i.exec(header);
  return match ? match[1]! : null;
}

/** Uniform over the 36-symbol alphabet: bytes >= 252 are rejected (no modulo bias). */
function randomPrefix(): string {
  let out = '';
  while (out.length < API_KEY_PREFIX_LENGTH) {
    for (const byte of randomBytes(API_KEY_PREFIX_LENGTH * 2)) {
      if (byte >= 252) continue;
      out += PREFIX_ALPHABET[byte % PREFIX_ALPHABET.length];
      if (out.length === API_KEY_PREFIX_LENGTH) break;
    }
  }
  return out;
}
