import { describe, expect, it } from 'vitest';
import { ValidationError } from '../src/domain/errors.js';
import { decodeCursor, encodeCursor } from '../src/lib/cursor.js';

const sample = {
  createdAt: '2026-10-20T04:30:34.201123Z',
  id: '222efd5a-939e-47e4-ad55-410a8bd63c6a',
};

describe('keyset cursor', () => {
  it('round-trips (created_at, id) without losing microseconds', () => {
    const encoded = encodeCursor(sample);
    expect(decodeCursor(encoded)).toEqual(sample);
  });

  it('is URL-safe base64 (no padding, +, /)', () => {
    const encoded = encodeCursor(sample);
    expect(encoded).toMatch(/^[A-Za-z0-9_-]+$/);
  });

  const invalid: Array<[string, string]> = [
    ['empty', ''],
    ['non-base64url characters', 'abc$%^'],
    ['standard base64 with padding', 'eyJ2IjoxfQ=='],
    ['not JSON', Buffer.from('not json').toString('base64url')],
    [
      'wrong version',
      Buffer.from(JSON.stringify({ v: 2, t: sample.createdAt, id: sample.id })).toString(
        'base64url',
      ),
    ],
    [
      'millisecond timestamp',
      Buffer.from(JSON.stringify({ v: 1, t: '2026-10-20T04:30:34.201Z', id: sample.id })).toString(
        'base64url',
      ),
    ],
    [
      'impossible date',
      Buffer.from(
        JSON.stringify({ v: 1, t: '2026-13-45T04:30:34.201123Z', id: sample.id }),
      ).toString('base64url'),
    ],
    [
      'non-uuid id',
      Buffer.from(JSON.stringify({ v: 1, t: sample.createdAt, id: '1 OR 1=1' })).toString(
        'base64url',
      ),
    ],
    [
      'extra fields',
      Buffer.from(JSON.stringify({ v: 1, t: sample.createdAt, id: sample.id, x: 1 })).toString(
        'base64url',
      ),
    ],
    ['oversized', 'a'.repeat(513)],
  ];

  it.each(invalid)('rejects %s with a ValidationError', (_label, raw) => {
    expect(() => decodeCursor(raw)).toThrow(ValidationError);
  });
});
