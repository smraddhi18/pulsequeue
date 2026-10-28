import { describe, expect, it } from 'vitest';
import { parseDuration } from '../src/domain/dashboard.js';
import {
  dashboardQueuesQuerySchema,
  failureRateQuerySchema,
  latencyQuerySchema,
  throughputQuerySchema,
} from '../src/routes/dashboard.schemas.js';

describe('parseDuration', () => {
  it('parses <int><unit>', () => {
    expect(parseDuration('30s')).toBe(30_000);
    expect(parseDuration('5m')).toBe(300_000);
    expect(parseDuration('2h')).toBe(7_200_000);
    expect(parseDuration('7d')).toBe(604_800_000);
  });

  it('rejects anything else', () => {
    for (const bad of ['', '5', 'm', '1.5h', '-1m', '1w', '1H', ' 1m', '123456s']) {
      expect(parseDuration(bad)).toBeNull();
    }
  });
});

describe('throughputQuerySchema', () => {
  it('defaults to a 1h window in 1m buckets', () => {
    expect(throughputQuerySchema.parse({})).toEqual({ window: 3_600_000, bucket: 60_000 });
  });

  it('accepts aligned windows up to 1440 buckets', () => {
    expect(throughputQuerySchema.parse({ window: '1d', bucket: '1m' })).toEqual({
      window: 86_400_000,
      bucket: 60_000,
    });
    expect(throughputQuerySchema.parse({ window: '7d', bucket: '1h' }).window).toBe(604_800_000);
  });

  it.each([
    [{ window: '1d', bucket: '30s' }, /at most 1440/],
    [{ window: '1h', bucket: '7m' }, /multiple of bucket/],
    [{ window: '5m', bucket: '1h' }, /larger than window/],
    [{ window: '8d' }, /between/],
    [{ bucket: '5s' }, /between/],
    [{ window: 'forever' }, /integer followed by/],
    [{ window: '1h', extra: '1' }, /Unrecognized key/],
  ])('rejects %o', (query, message) => {
    const result = throughputQuerySchema.safeParse(query);
    expect(result.success).toBe(false);
    expect(JSON.stringify(result.error?.issues)).toMatch(message);
  });
});

describe('latency and failure-rate windows', () => {
  it('latency defaults to 1h and caps at 24h (percentiles sort the window)', () => {
    expect(latencyQuerySchema.parse({})).toEqual({ window: 3_600_000 });
    expect(latencyQuerySchema.parse({ window: '1d' })).toEqual({ window: 86_400_000 });
    expect(latencyQuerySchema.safeParse({ window: '2d' }).success).toBe(false);
  });

  it('failure rate allows up to 7d', () => {
    expect(failureRateQuerySchema.parse({ window: '7d' })).toEqual({ window: 604_800_000 });
    expect(failureRateQuerySchema.safeParse({ window: '8d' }).success).toBe(false);
  });
});

describe('dashboardQueuesQuerySchema', () => {
  it('coerces limit and validates the cursor', () => {
    expect(dashboardQueuesQuerySchema.parse({})).toEqual({ limit: 100 });
    expect(dashboardQueuesQuerySchema.parse({ limit: '5', after: 'emails' })).toEqual({
      limit: 5,
      after: 'emails',
    });
    expect(dashboardQueuesQuerySchema.safeParse({ limit: '501' }).success).toBe(false);
    expect(dashboardQueuesQuerySchema.safeParse({ after: 'not a queue!' }).success).toBe(false);
  });
});
