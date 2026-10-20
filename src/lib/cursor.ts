import { z } from 'zod';
import { ValidationError } from '../domain/errors.js';

/**
 * Opaque keyset cursor over (created_at, id).
 *
 * The timestamp is carried as the exact text Postgres produced (UTC ISO-8601
 * with 6 fractional digits), never via a JS Date: timestamptz has microsecond
 * precision and a millisecond Date would round, so rows created within the
 * same millisecond could be skipped or repeated across pages. Clients must
 * treat the cursor as opaque; the version field lets the encoding change
 * without breaking cursors already handed out.
 */
export interface KeysetCursor {
  /** created_at as `YYYY-MM-DDTHH:MM:SS.ffffffZ` (UTC, microseconds). */
  createdAt: string;
  id: string;
}

const CURSOR_VERSION = 1;
const MAX_CURSOR_LENGTH = 512;
const BASE64URL = /^[A-Za-z0-9_-]+$/;

const cursorPayloadSchema = z
  .object({
    v: z.literal(CURSOR_VERSION),
    t: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/)
      .refine((t) => !Number.isNaN(Date.parse(t))),
    id: z.string().uuid(),
  })
  .strict();

export function encodeCursor(cursor: KeysetCursor): string {
  const body = JSON.stringify({ v: CURSOR_VERSION, t: cursor.createdAt, id: cursor.id });
  return Buffer.from(body, 'utf8').toString('base64url');
}

/** Decode a client-supplied cursor; any malformed input is a 400, never a 500. */
export function decodeCursor(raw: string): KeysetCursor {
  if (raw.length === 0 || raw.length > MAX_CURSOR_LENGTH || !BASE64URL.test(raw)) {
    throw new ValidationError('Invalid cursor');
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8'));
  } catch {
    throw new ValidationError('Invalid cursor');
  }

  const result = cursorPayloadSchema.safeParse(parsed);
  if (!result.success) {
    throw new ValidationError('Invalid cursor');
  }
  return { createdAt: result.data.t, id: result.data.id };
}
