import { describe, expect, it } from 'vitest';
import {
  bulkReplayBodySchema,
  dlqJobParamsSchema,
  listDeadLettersQuerySchema,
  purgeDeadLettersQuerySchema,
  replayJobBodySchema,
} from '../src/routes/dlq.schemas.js';

describe('DLQ request schemas', () => {
  it('list query: defaults, coercion, reason enum, strict', () => {
    expect(listDeadLettersQuerySchema.parse({})).toEqual({ limit: 50 });
    expect(listDeadLettersQuerySchema.parse({ limit: '10', reason: 'invalid_payload' })).toEqual({
      limit: 10,
      reason: 'invalid_payload',
    });
    expect(listDeadLettersQuerySchema.safeParse({ reason: 'bored' }).success).toBe(false);
    expect(listDeadLettersQuerySchema.safeParse({ limit: '201' }).success).toBe(false);
    expect(listDeadLettersQuerySchema.safeParse({ status: 'dead' }).success).toBe(false);
  });

  it('job params must be a UUID', () => {
    expect(dlqJobParamsSchema.safeParse({ jobId: 'abc' }).success).toBe(false);
    expect(
      dlqJobParamsSchema.safeParse({ jobId: '7d9f0c1e-3a59-4e0c-9f53-4b1f8b8e2a10' }).success,
    ).toBe(true);
  });

  it('replay body is optional, strict and bounds delayMs', () => {
    expect(replayJobBodySchema.parse(undefined)).toEqual({});
    expect(replayJobBodySchema.parse({ delayMs: 1_000 })).toEqual({ delayMs: 1_000 });
    expect(replayJobBodySchema.safeParse({ delayMs: -1 }).success).toBe(false);
    expect(replayJobBodySchema.safeParse({ payload: {} }).success).toBe(false);
  });

  it('bulk replay body: default limit, max 1000', () => {
    expect(bulkReplayBodySchema.parse(undefined)).toEqual({ limit: 100 });
    expect(bulkReplayBodySchema.parse({})).toEqual({ limit: 100 });
    expect(bulkReplayBodySchema.parse({ limit: 5, reason: 'non_retryable' })).toEqual({
      limit: 5,
      reason: 'non_retryable',
    });
    expect(bulkReplayBodySchema.safeParse({ limit: 1_001 }).success).toBe(false);
    expect(bulkReplayBodySchema.safeParse({ limit: '5' }).success).toBe(false);
  });

  it('bulk purge requires a zoned ISO olderThan', () => {
    expect(purgeDeadLettersQuerySchema.safeParse({}).success).toBe(false);
    expect(purgeDeadLettersQuerySchema.safeParse({ olderThan: 'yesterday' }).success).toBe(false);
    expect(
      purgeDeadLettersQuerySchema.safeParse({ olderThan: '2026-10-01T00:00:00' }).success,
    ).toBe(false);
    const parsed = purgeDeadLettersQuerySchema.parse({
      olderThan: '2026-10-01T05:30:00+05:30',
      limit: '20',
    });
    expect(parsed.olderThan.toISOString()).toBe('2026-10-01T00:00:00.000Z');
    expect(parsed.limit).toBe(20);
    expect(purgeDeadLettersQuerySchema.parse({ olderThan: '2026-10-01T00:00:00Z' }).limit).toBe(
      1_000,
    );
  });
});
