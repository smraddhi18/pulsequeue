import { describe, expect, it } from 'vitest';
import { compileCron, InvalidCronError, isValidTimeZone } from '../src/lib/cron.js';

const at = (iso: string) => new Date(iso);
const iso = (d: Date) => d.toISOString();

function take<T>(gen: Iterable<T>, n: number): T[] {
  const out: T[] = [];
  for (const v of gen) {
    out.push(v);
    if (out.length === n) break;
  }
  return out;
}

function errorOf(fn: () => unknown): InvalidCronError {
  try {
    fn();
  } catch (err) {
    if (err instanceof InvalidCronError) return err;
    throw err;
  }
  throw new Error('expected InvalidCronError');
}

describe('compileCron: validation', () => {
  it.each([
    ['empty', ''],
    ['whitespace only', '   '],
    ['garbage', 'not a cron'],
    ['minute out of range', '60 * * * *'],
    ['too many fields', '0 0 0 * * * *'],
    ['too few fields (cron-parser would pad it)', '* * *'],
    ['single field', '*'],
    ['never matches (Feb 30)', '0 0 30 2 *'],
    ['sub-minute: every second', '* * * * * *'],
    ['sub-minute: every 15 s', '*/15 * * * * *'],
    ['sub-minute: @secondly', '@secondly'],
    ['too long', `0 ${'1,'.repeat(200)}2 * * *`],
  ])('rejects %s as a cron error', (_label, expr) => {
    expect(errorOf(() => compileCron(expr, 'UTC')).field).toBe('cron');
  });

  it.each([
    ['unknown zone', 'Mars/Olympus_Mons'],
    ['empty zone', ''],
    ['offset string', '+05:30'],
    ['too long', `Europe/${'x'.repeat(80)}`],
  ])('rejects %s as a timezone error', (_label, tz) => {
    expect(errorOf(() => compileCron('0 9 * * *', tz)).field).toBe('timezone');
    expect(isValidTimeZone(tz)).toBe(false);
  });

  it('accepts 5-field, 6-field with seconds = 0 and macros; trims whitespace', () => {
    for (const expr of ['*/5 * * * *', '0 0 9 * * MON-FRI', '@hourly', '@daily', '@weekly']) {
      expect(() => compileCron(expr, 'UTC')).not.toThrow();
    }
    expect(compileCron('  0 9 * * *  ', 'UTC').expression).toBe('0 9 * * *');
  });

  it('accepts IANA zones and their aliases as given (no canonical renaming)', () => {
    expect(compileCron('0 9 * * *', 'Asia/Kolkata').timezone).toBe('Asia/Kolkata');
    expect(compileCron('0 9 * * *', 'Asia/Calcutta').timezone).toBe('Asia/Calcutta');
    expect(isValidTimeZone('America/New_York')).toBe(true);
    expect(isValidTimeZone('UTC')).toBe(true);
  });

  it('defaults to UTC, never the host zone', () => {
    const cron = compileCron('0 9 * * *');
    expect(cron.timezone).toBe('UTC');
    expect(iso(cron.next(at('2026-01-01T00:00:00Z')))).toBe('2026-01-01T09:00:00.000Z');
  });
});

describe('compileCron: next()', () => {
  it('is strictly after the given instant', () => {
    const cron = compileCron('0 * * * *', 'UTC');
    expect(iso(cron.next(at('2026-01-01T01:00:00.000Z')))).toBe('2026-01-01T02:00:00.000Z');
    expect(iso(cron.next(at('2026-01-01T00:59:59.999Z')))).toBe('2026-01-01T01:00:00.000Z');
    expect(iso(cron.next(at('2026-01-01T01:00:00.001Z')))).toBe('2026-01-01T02:00:00.000Z');
  });

  it('evaluates wall-clock times in the schedule zone', () => {
    const cron = compileCron('0 9 * * *', 'Asia/Kolkata');
    // 09:00 IST = 03:30 UTC
    expect(iso(cron.next(at('2026-10-23T00:00:00Z')))).toBe('2026-10-23T03:30:00.000Z');
    expect(iso(cron.next(at('2026-10-23T03:30:00Z')))).toBe('2026-10-24T03:30:00.000Z');
  });

  it('DST spring forward: a skipped local time fires right after the gap, once', () => {
    // 2026-03-08 02:00 -> 03:00 in New York; 02:30 does not exist that day.
    const cron = compileCron('30 2 * * *', 'America/New_York');
    const first = cron.next(at('2026-03-08T05:00:00Z'));
    expect(iso(first)).toBe('2026-03-08T07:30:00.000Z'); // 03:30 EDT
    expect(iso(cron.next(first))).toBe('2026-03-09T06:30:00.000Z'); // 02:30 EDT next day
  });

  it('DST fall back: a repeated local time fires once', () => {
    // 2026-11-01 02:00 EDT -> 01:00 EST; 01:30 happens twice.
    const cron = compileCron('30 1 * * *', 'America/New_York');
    const first = cron.next(at('2026-11-01T04:00:00Z'));
    expect(iso(first)).toBe('2026-11-01T05:30:00.000Z'); // 01:30 EDT
    expect(iso(cron.next(first))).toBe('2026-11-02T06:30:00.000Z'); // 01:30 EST next day
  });
});

describe('compileCron: walkBack()', () => {
  it('yields occurrences at or before the instant, newest first', () => {
    const cron = compileCron('*/15 * * * *', 'UTC');
    expect(take(cron.walkBack(at('2026-01-01T01:00:00.000Z')), 3).map(iso)).toEqual([
      '2026-01-01T01:00:00.000Z',
      '2026-01-01T00:45:00.000Z',
      '2026-01-01T00:30:00.000Z',
    ]);
    expect(take(cron.walkBack(at('2026-01-01T00:59:59.999Z')), 1).map(iso)).toEqual([
      '2026-01-01T00:45:00.000Z',
    ]);
  });

  it('agrees with the forward walk across a fall-back transition (no extra occurrence)', () => {
    const cron = compileCron('30 1 * * *', 'America/New_York');
    const back = take(cron.walkBack(at('2026-11-03T00:00:00Z')), 3).map(iso);
    expect(back).toEqual([
      '2026-11-02T06:30:00.000Z',
      '2026-11-01T05:30:00.000Z', // the EDT 01:30 only, as next() fires it
      '2026-10-31T05:30:00.000Z',
    ]);
  });

  it.each([
    ['hourly, London, spring forward', '15 * * * *', 'Europe/London', '2026-03-28', '2026-03-30'],
    ['hourly, London, fall back', '15 * * * *', 'Europe/London', '2026-10-24', '2026-10-26'],
    [
      'every 20 min, New York, fall back',
      '*/20 * * * *',
      'America/New_York',
      '2026-10-31',
      '2026-11-02',
    ],
    [
      '01:30 daily, New York, fall back',
      '30 1 * * *',
      'America/New_York',
      '2026-10-20',
      '2026-11-10',
    ],
    [
      '02:30 daily, New York, spring forward',
      '30 2 * * *',
      'America/New_York',
      '2026-03-01',
      '2026-03-15',
    ],
    ['weekdays 09:00, Kolkata', '0 9 * * 1-5', 'Asia/Kolkata', '2026-10-01', '2026-11-01'],
  ])('matches the forward walk exactly: %s', (_label, expr, tz, fromDay, toDay) => {
    const cron = compileCron(expr, tz);
    const from = at(`${fromDay}T00:00:00Z`);
    const to = at(`${toDay}T00:00:00Z`);
    const forward: string[] = [];
    for (let t = cron.next(new Date(from.getTime() - 1)); t <= to; t = cron.next(t)) {
      forward.push(iso(t));
    }
    const backward: string[] = [];
    for (const t of cron.walkBack(to)) {
      if (t < from) break;
      backward.push(iso(t));
    }
    expect(backward.reverse()).toEqual(forward);
  });
});
