import { describe, expect, it } from 'vitest';
import { JOB_LIMITS, PAGINATION_LIMITS } from '../src/domain/job.js';
import {
  enqueueJobBodySchema,
  enqueueJobHeadersSchema,
  jobParamsSchema,
  listJobsQuerySchema,
  queueParamsSchema,
} from '../src/routes/jobs.schemas.js';

describe('enqueueJobBodySchema', () => {
  it('accepts a minimal body and leaves defaults to the service', () => {
    const parsed = enqueueJobBodySchema.parse({ payload: { a: 1 } });
    expect(parsed).toEqual({ payload: { a: 1 } });
  });

  it('accepts any JSON value as payload, including null and arrays', () => {
    for (const payload of [null, 0, 'text', [1, 2], { nested: { ok: true } }]) {
      expect(enqueueJobBodySchema.safeParse({ payload }).success).toBe(true);
    }
  });

  it('accepts values at the edges of every range', () => {
    expect(
      enqueueJobBodySchema.safeParse({
        payload: {},
        priority: JOB_LIMITS.MAX_PRIORITY,
        delayMs: JOB_LIMITS.MAX_DELAY_MS,
        maxAttempts: JOB_LIMITS.MAX_MAX_ATTEMPTS,
      }).success,
    ).toBe(true);
    expect(
      enqueueJobBodySchema.safeParse({
        payload: {},
        priority: JOB_LIMITS.MIN_PRIORITY,
        delayMs: 0,
        maxAttempts: JOB_LIMITS.MIN_MAX_ATTEMPTS,
      }).success,
    ).toBe(true);
  });

  const invalid: Array<[string, unknown]> = [
    ['missing payload', {}],
    ['missing body', undefined],
    ['priority above max', { payload: {}, priority: JOB_LIMITS.MAX_PRIORITY + 1 }],
    ['priority below min', { payload: {}, priority: -1 }],
    ['fractional priority', { payload: {}, priority: 1.5 }],
    ['priority as string', { payload: {}, priority: '5' }],
    ['negative delay', { payload: {}, delayMs: -1 }],
    ['delay beyond 30 days', { payload: {}, delayMs: JOB_LIMITS.MAX_DELAY_MS + 1 }],
    ['zero maxAttempts', { payload: {}, maxAttempts: 0 }],
    ['maxAttempts above max', { payload: {}, maxAttempts: JOB_LIMITS.MAX_MAX_ATTEMPTS + 1 }],
    ['unknown field (typo)', { payload: {}, delay: 1000 }],
  ];

  it.each(invalid)('rejects %s', (_label, body) => {
    expect(enqueueJobBodySchema.safeParse(body).success).toBe(false);
  });
});

describe('enqueueJobHeadersSchema', () => {
  it('accepts printable keys up to 255 chars and ignores other headers', () => {
    const key = 'order-123:v1';
    expect(enqueueJobHeadersSchema.parse({ 'idempotency-key': key, host: 'x' })).toEqual({
      'idempotency-key': key,
    });
    expect(enqueueJobHeadersSchema.safeParse({ 'idempotency-key': 'k'.repeat(255) }).success).toBe(
      true,
    );
  });

  it.each([
    ['empty', ''],
    ['contains a space', 'has space'],
    ['non-ASCII', 'clé'],
    ['too long', 'k'.repeat(256)],
  ])('rejects a key that is %s', (_label, key) => {
    expect(enqueueJobHeadersSchema.safeParse({ 'idempotency-key': key }).success).toBe(false);
  });
});

describe('queueParamsSchema', () => {
  it.each(['emails', 'a', 'billing.invoices-v2_eu', '9lives', 'q'.repeat(64)])(
    'accepts %s',
    (name) => {
      expect(queueParamsSchema.safeParse({ name }).success).toBe(true);
    },
  );

  it.each(['', 'Emails', '-leading-dash', '.hidden', 'has space', 'q'.repeat(65), 'a/b'])(
    'rejects %j',
    (name) => {
      expect(queueParamsSchema.safeParse({ name }).success).toBe(false);
    },
  );
});

describe('jobParamsSchema', () => {
  it('requires a UUID', () => {
    expect(jobParamsSchema.safeParse({ id: '222efd5a-939e-47e4-ad55-410a8bd63c6a' }).success).toBe(
      true,
    );
    expect(jobParamsSchema.safeParse({ id: '123' }).success).toBe(false);
  });
});

describe('listJobsQuerySchema', () => {
  it('defaults limit and coerces query-string numbers', () => {
    expect(listJobsQuerySchema.parse({})).toEqual({ limit: PAGINATION_LIMITS.DEFAULT_LIMIT });
    expect(listJobsQuerySchema.parse({ limit: '10', status: 'queued' })).toEqual({
      limit: 10,
      status: 'queued',
    });
  });

  it.each([
    ['limit 0', { limit: '0' }],
    ['limit above max', { limit: String(PAGINATION_LIMITS.MAX_LIMIT + 1) }],
    ['non-numeric limit', { limit: 'ten' }],
    ['unknown status', { status: 'pending' }],
    ['unknown parameter', { statuses: 'queued' }],
    ['repeated status', { status: ['queued', 'dead'] }],
  ])('rejects %s', (_label, query) => {
    expect(listJobsQuerySchema.safeParse(query).success).toBe(false);
  });
});
