import { describe, expect, it } from 'vitest';
import { PAGINATION_LIMITS } from '../src/domain/job.js';
import {
  createScheduleBodySchema,
  listSchedulesQuerySchema,
  scheduleParamsSchema,
} from '../src/routes/schedules.schemas.js';

const valid = { name: 'nightly-report', cron: '0 2 * * *', payload: { report: 'daily' } };

function fieldErrors(body: unknown): Record<string, string[] | undefined> {
  const result = createScheduleBodySchema.safeParse(body);
  if (result.success) throw new Error('expected a validation error');
  return result.error.flatten().fieldErrors;
}

describe('createScheduleBodySchema', () => {
  it('accepts a minimal body and leaves defaults to the service', () => {
    expect(createScheduleBodySchema.parse(valid)).toEqual(valid);
  });

  it('accepts every optional field at valid values', () => {
    const body = {
      ...valid,
      timezone: 'Asia/Kolkata',
      priority: 100,
      maxAttempts: 25,
      misfirePolicy: 'catch_up',
      enabled: false,
    };
    expect(createScheduleBodySchema.parse(body)).toEqual(body);
  });

  it('reports a bad cron expression on the cron field', () => {
    expect(fieldErrors({ ...valid, cron: '61 * * * *' }).cron?.[0]).toMatch(/Invalid cron/);
    expect(fieldErrors({ ...valid, cron: '*/10 * * * * *' }).cron?.[0]).toMatch(/Sub-minute/);
    expect(fieldErrors({ ...valid, cron: '0 0 30 2 *' }).cron?.[0]).toMatch(/day of month/);
  });

  it('reports an unknown time zone on the timezone field', () => {
    expect(fieldErrors({ ...valid, timezone: 'Mars/Base' }).timezone?.[0]).toMatch(
      /Unknown time zone/,
    );
  });

  const invalid: Array<[string, unknown]> = [
    ['missing name', { cron: valid.cron, payload: {} }],
    ['missing cron', { name: 'x', payload: {} }],
    ['missing payload', { name: 'x', cron: valid.cron }],
    ['empty cron', { ...valid, cron: '' }],
    ['name with spaces', { ...valid, name: 'nightly report' }],
    ['name starting with a dash', { ...valid, name: '-x' }],
    ['name too long', { ...valid, name: 'a'.repeat(129) }],
    ['unknown misfire policy', { ...valid, misfirePolicy: 'fire_all' }],
    ['priority out of range', { ...valid, priority: 101 }],
    ['maxAttempts zero', { ...valid, maxAttempts: 0 }],
    ['enabled as string', { ...valid, enabled: 'true' }],
    ['unknown field (typo)', { ...valid, timeZone: 'UTC' }],
  ];

  it.each(invalid)('rejects %s', (_label, body) => {
    expect(createScheduleBodySchema.safeParse(body).success).toBe(false);
  });
});

describe('listSchedulesQuerySchema', () => {
  it('defaults the limit and parses the enabled filter', () => {
    expect(listSchedulesQuerySchema.parse({})).toEqual({ limit: PAGINATION_LIMITS.DEFAULT_LIMIT });
    expect(listSchedulesQuerySchema.parse({ enabled: 'false', limit: '5' })).toEqual({
      enabled: false,
      limit: 5,
    });
    expect(listSchedulesQuerySchema.parse({ enabled: 'true' }).enabled).toBe(true);
  });

  it.each([
    ['enabled not a boolean', { enabled: 'yes' }],
    ['limit above max', { limit: String(PAGINATION_LIMITS.MAX_LIMIT + 1) }],
    ['unknown parameter', { status: 'queued' }],
  ])('rejects %s', (_label, query) => {
    expect(listSchedulesQuerySchema.safeParse(query).success).toBe(false);
  });
});

describe('scheduleParamsSchema', () => {
  it('requires a UUID', () => {
    expect(scheduleParamsSchema.safeParse({ id: 'nope' }).success).toBe(false);
    expect(
      scheduleParamsSchema.safeParse({ id: '6f1c2a8e-3d4b-4c5d-9e6f-7a8b9c0d1e2f' }).success,
    ).toBe(true);
  });
});
