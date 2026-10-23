import { CronExpressionParser } from 'cron-parser';

/**
 * Cron evaluation for schedules, wrapping cron-parser (maintained, luxon-based
 * IANA time-zone support) behind a small, strict surface:
 *
 *   - Standard 5-field expressions (`m h dom mon dow`) plus the common macros
 *     (`@hourly`, `@daily`, ...). A 6-field expression is accepted only if its
 *     seconds field is exactly 0: the scheduler's resolution is one minute,
 *     and sub-minute "schedules" belong in a worker loop, not a cron row.
 *   - The zone is always explicit. cron-parser falls back to the host's local
 *     zone when none is given, which would make a schedule's meaning depend on
 *     the machine running the scheduler; here `UTC` is the default instead.
 *   - Daylight-saving transitions follow the usual cron conventions: a local
 *     time that does not exist (spring forward) fires at the first instant
 *     after the gap, and a local time that occurs twice (fall back) fires once.
 *     Walking backwards (misfire catch-up) applies the same rule, see
 *     walkBack.
 */

export const CRON_LIMITS = {
  MAX_EXPRESSION_LENGTH: 256,
  MAX_TIMEZONE_LENGTH: 64,
  DEFAULT_TIMEZONE: 'UTC',
} as const;

/** Guard for walkBack's forward fill between two backward anchors. */
const MAX_SEGMENT = 1_000;

export type CronField = 'cron' | 'timezone';

/** An expression or time zone that cannot be evaluated; `field` says which. */
export class InvalidCronError extends Error {
  readonly field: CronField;

  constructor(field: CronField, message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'InvalidCronError';
    this.field = field;
  }
}

/** A validated cron expression bound to a time zone. Immutable and cheap to keep. */
export interface CompiledCron {
  readonly expression: string;
  readonly timezone: string;
  /** First occurrence strictly after `after`. */
  next(after: Date): Date;
  /**
   * Occurrences at or before `atOrBefore`, newest first, lazily and without
   * end; the caller decides when to stop. Yields exactly the instants a
   * forward walk (`next`) would produce, so a repeated fall-back hour is
   * yielded once, like it fires once.
   */
  walkBack(atOrBefore: Date): Generator<Date, void, undefined>;
}

/** True if `tz` is an IANA zone (or alias) both Intl and luxon understand. */
export function isValidTimeZone(tz: string): boolean {
  if (tz.length === 0 || tz.length > CRON_LIMITS.MAX_TIMEZONE_LENGTH) return false;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: tz });
  } catch {
    return false;
  }
  // luxon resolves zones itself; probe it the same way evaluation will.
  try {
    CronExpressionParser.parse('* * * * *', { currentDate: new Date(0), tz }).next();
    return true;
  } catch {
    return false;
  }
}

/**
 * Validate and compile `expression` in `timezone`. Throws InvalidCronError
 * (never a raw cron-parser error) for anything that cannot be evaluated,
 * including expressions that parse but never match (`0 0 30 2 *`).
 */
export function compileCron(
  expression: string,
  timezone: string = CRON_LIMITS.DEFAULT_TIMEZONE,
): CompiledCron {
  const expr = expression.trim();
  // cron-parser reads an empty string as `* * * * *`; never guess.
  if (expr.length === 0) throw new InvalidCronError('cron', 'Cron expression is empty');
  if (expr.length > CRON_LIMITS.MAX_EXPRESSION_LENGTH) {
    throw new InvalidCronError(
      'cron',
      `Cron expression must be at most ${CRON_LIMITS.MAX_EXPRESSION_LENGTH} characters`,
    );
  }
  // cron-parser pads short expressions (`* * *` reads as every hour), which
  // would silently turn a typo into a schedule. Demand the standard shapes.
  if (!expr.startsWith('@')) {
    const fields = expr.split(/\s+/).length;
    if (fields !== 5 && fields !== 6) {
      throw new InvalidCronError(
        'cron',
        `Invalid cron expression: expected 5 fields (minute hour day-of-month month day-of-week), got ${fields}`,
      );
    }
  }
  if (!isValidTimeZone(timezone)) {
    throw new InvalidCronError('timezone', `Unknown time zone "${timezone}"; use an IANA name`);
  }

  const parse = (currentDate: Date) =>
    CronExpressionParser.parse(expr, { currentDate, tz: timezone });

  let probe;
  try {
    probe = parse(new Date());
  } catch (err) {
    throw new InvalidCronError('cron', `Invalid cron expression: ${describe(err)}`, {
      cause: err,
    });
  }
  const seconds = probe.fields.second.values;
  if (seconds.length !== 1 || seconds[0] !== 0) {
    throw new InvalidCronError(
      'cron',
      'Sub-minute schedules are not supported; the seconds field, if present, must be 0',
    );
  }
  try {
    probe.next();
  } catch (err) {
    throw new InvalidCronError('cron', `Cron expression never matches: ${describe(err)}`, {
      cause: err,
    });
  }

  const next = (after: Date): Date => parse(after).next().toDate();

  return {
    expression: expr,
    timezone,
    next,
    *walkBack(atOrBefore: Date) {
      // prev() is strictly before currentDate; +1 ms makes atOrBefore itself
      // eligible (occurrences are whole minutes, so nothing else is added).
      const iter = parse(new Date(atOrBefore.getTime() + 1));
      const prev = (): Date | null => {
        try {
          return iter.prev().toDate();
        } catch {
          return null; // walked past cron-parser's supported range
        }
      };
      // Backward and forward iteration disagree around DST transitions:
      // backwards, cron-parser yields both instants of a repeated fall-back
      // wall time and can skip the shifted instant of a spring-forward gap,
      // while forwards it fires the first and the shifted one. The forward
      // walk is what fires on time, so it is the source of truth: backward
      // candidates are only used as anchors, and every instant yielded is one
      // the forward walk from the next-older anchor lands on.
      let upper = atOrBefore.getTime() + 1; // exclusive
      for (let anchor = prev(); anchor !== null; anchor = prev()) {
        const segment: Date[] = [];
        for (let t = next(anchor); t.getTime() < upper; t = next(t)) {
          segment.push(t);
          if (segment.length > MAX_SEGMENT) break; // cannot happen; guards the loop
        }
        for (let i = segment.length - 1; i >= 0; i--) yield segment[i] as Date;
        upper = anchor.getTime() + 1;
      }
    },
  };
}

function describe(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
