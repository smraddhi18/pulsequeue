import { describe, expect, it } from 'vitest';
import type { MisfirePolicy } from '../../src/domain/schedule.js';
import { compileCron } from '../../src/lib/cron.js';
import { MAX_ON_TIME_FIRES, planFires } from '../../src/scheduler/misfire.js';

const at = (iso: string) => new Date(iso);
const isos = (ds: Date[]) => ds.map((d) => d.toISOString());
const everyMinute = compileCron('* * * * *', 'UTC');
const hourly = compileCron('0 * * * *', 'UTC');
const GRACE = 60_000;

function plan(
  policy: MisfirePolicy,
  nextRunAt: string,
  now: string,
  opts: { cron?: typeof hourly; maxCatchUp?: number; graceMs?: number } = {},
) {
  return planFires({
    cron: opts.cron ?? hourly,
    nextRunAt: at(nextRunAt),
    now: at(now),
    policy,
    graceMs: opts.graceMs ?? GRACE,
    maxCatchUp: opts.maxCatchUp ?? 10,
  });
}

describe('planFires: on-time occurrences', () => {
  it.each<MisfirePolicy>(['skip', 'fire_once', 'catch_up'])(
    '%s: fires the due occurrence and advances strictly past now',
    (policy) => {
      const p = plan(policy, '2026-01-01T10:00:00Z', '2026-01-01T10:00:00.400Z');
      expect(isos(p.fireTimes)).toEqual(['2026-01-01T10:00:00.000Z']);
      expect(p.skipped).toBe(0);
      expect(p.skippedMore).toBe(false);
      expect(p.nextRunAt.toISOString()).toBe('2026-01-01T11:00:00.000Z');
    },
  );

  it('fires an occurrence exactly at the grace boundary (inclusive)', () => {
    const p = plan('skip', '2026-01-01T10:00:00Z', '2026-01-01T10:01:00Z');
    expect(isos(p.fireTimes)).toEqual(['2026-01-01T10:00:00.000Z']);
  });

  it('a short failover gap fires every occurrence inside the grace window, oldest first', () => {
    const p = plan('skip', '2026-01-01T10:00:00Z', '2026-01-01T10:02:30Z', {
      cron: everyMinute,
      graceMs: 5 * 60_000,
    });
    expect(isos(p.fireTimes)).toEqual([
      '2026-01-01T10:00:00.000Z',
      '2026-01-01T10:01:00.000Z',
      '2026-01-01T10:02:00.000Z',
    ]);
    expect(p.nextRunAt.toISOString()).toBe('2026-01-01T10:03:00.000Z');
  });

  it('returns nothing for a schedule that is not due yet', () => {
    const p = plan('catch_up', '2026-01-01T11:00:00Z', '2026-01-01T10:59:59Z');
    expect(p.fireTimes).toEqual([]);
    expect(p.nextRunAt.toISOString()).toBe('2026-01-01T11:00:00.000Z');
  });
});

describe('planFires: misfires (late by more than the grace period)', () => {
  // Down from 10:00 to 15:30: 10:00..15:00 (6 occurrences) are all late.
  const down = ['2026-01-01T10:00:00Z', '2026-01-01T15:30:00Z'] as const;

  it('skip drops every late occurrence and resumes at the next one', () => {
    const p = plan('skip', ...down);
    expect(p.fireTimes).toEqual([]);
    expect(p.skipped).toBeGreaterThanOrEqual(1);
    expect(p.nextRunAt.toISOString()).toBe('2026-01-01T16:00:00.000Z');
  });

  it('fire_once coalesces all late occurrences into the most recent one', () => {
    const p = plan('fire_once', ...down);
    expect(isos(p.fireTimes)).toEqual(['2026-01-01T15:00:00.000Z']);
    expect(p.skipped + (p.skippedMore ? 1 : 0)).toBeGreaterThanOrEqual(1);
    expect(p.nextRunAt.toISOString()).toBe('2026-01-01T16:00:00.000Z');
  });

  it('fire_once fires nothing extra when an on-time occurrence fires anyway', () => {
    const p = plan('fire_once', '2026-01-01T10:00:00Z', '2026-01-01T15:00:30Z');
    expect(isos(p.fireTimes)).toEqual(['2026-01-01T15:00:00.000Z']);
  });

  it('catch_up fires every late occurrence within the limit, oldest first', () => {
    const p = plan('catch_up', ...down);
    expect(isos(p.fireTimes)).toEqual([
      '2026-01-01T10:00:00.000Z',
      '2026-01-01T11:00:00.000Z',
      '2026-01-01T12:00:00.000Z',
      '2026-01-01T13:00:00.000Z',
      '2026-01-01T14:00:00.000Z',
      '2026-01-01T15:00:00.000Z',
    ]);
    expect(p.skipped).toBe(0);
    expect(p.skippedMore).toBe(false);
  });

  it('catch_up keeps only the most recent maxCatchUp and reports what it dropped', () => {
    const p = plan('catch_up', ...down, { maxCatchUp: 2 });
    expect(isos(p.fireTimes)).toEqual(['2026-01-01T14:00:00.000Z', '2026-01-01T15:00:00.000Z']);
    expect(p.skipped).toBe(1);
    expect(p.skippedMore).toBe(true);
    expect(p.oldestSkipped?.toISOString()).toBe('2026-01-01T13:00:00.000Z');
  });

  it('catch_up with maxCatchUp = 0 behaves like skip', () => {
    expect(plan('catch_up', ...down, { maxCatchUp: 0 }).fireTimes).toEqual([]);
  });

  it('catch_up adds late occurrences to the on-time ones', () => {
    const p = plan('catch_up', '2026-01-01T13:00:00Z', '2026-01-01T15:00:10Z', { maxCatchUp: 5 });
    expect(isos(p.fireTimes)).toEqual([
      '2026-01-01T13:00:00.000Z',
      '2026-01-01T14:00:00.000Z',
      '2026-01-01T15:00:00.000Z',
    ]);
  });

  it('stays bounded after a very long outage of an every-minute schedule', () => {
    const started = performance.now();
    const p = plan('catch_up', '2025-01-01T00:00:00Z', '2026-01-01T00:00:30Z', {
      cron: everyMinute,
      maxCatchUp: 3,
    });
    expect(performance.now() - started).toBeLessThan(500);
    expect(isos(p.fireTimes)).toEqual([
      '2025-12-31T23:57:00.000Z',
      '2025-12-31T23:58:00.000Z',
      '2025-12-31T23:59:00.000Z',
      '2026-01-01T00:00:00.000Z',
    ]);
    expect(p.skippedMore).toBe(true);
  });

  it('caps on-time fires even with a huge grace window', () => {
    const p = plan('skip', '2026-01-01T00:00:00Z', '2026-01-01T12:00:00Z', {
      cron: everyMinute,
      graceMs: 24 * 3_600_000,
    });
    expect(p.fireTimes).toHaveLength(MAX_ON_TIME_FIRES);
    expect(p.fireTimes.at(-1)?.toISOString()).toBe('2026-01-01T12:00:00.000Z');
    expect(p.skippedMore).toBe(true);
  });

  it('still honours a promised next_run_at that is no longer an occurrence', () => {
    // Expression edited from :30 to :00 after next_run_at was computed.
    const p = plan('skip', '2026-01-01T10:30:00Z', '2026-01-01T10:30:20Z');
    expect(isos(p.fireTimes)).toEqual(['2026-01-01T10:30:00.000Z']);
    expect(p.nextRunAt.toISOString()).toBe('2026-01-01T11:00:00.000Z');
  });

  it('catch-up never fires the repeated fall-back hour twice', () => {
    const cron = compileCron('30 1 * * *', 'America/New_York');
    const p = plan('catch_up', '2026-10-31T05:30:00Z', '2026-11-02T12:00:00Z', { cron });
    expect(isos(p.fireTimes)).toEqual([
      '2026-10-31T05:30:00.000Z',
      '2026-11-01T05:30:00.000Z',
      '2026-11-02T06:30:00.000Z',
    ]);
  });
});
