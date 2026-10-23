import type { CompiledCron } from '../lib/cron.js';
import type { MisfirePolicy } from '../domain/schedule.js';

/**
 * Misfire handling: deciding which occurrences of a schedule to fire when the
 * scheduler evaluates it at `now`, given that its next unfired occurrence was
 * `nextRunAt`.
 *
 * Every occurrence in [nextRunAt, now] is classified:
 *   - on time: no older than `graceMs`. Normal operation (ticks run every
 *     second, a leader failover takes a few seconds) only ever produces these,
 *     and they always fire, whatever the policy.
 *   - late (a misfire): older than `graceMs`, e.g. after an outage with no
 *     scheduler running, or a schedule whose evaluation kept failing.
 *
 * Late occurrences are handled by the schedule's policy:
 *   - skip:      none fire.
 *   - fire_once: the most recent one fires, unless an on-time occurrence fires
 *                anyway (that run already "covers" the missed ones).
 *   - catch_up:  the most recent `maxCatchUp` fire; older ones are dropped, so
 *                a long outage cannot flood a queue with stale work.
 *
 * Whatever happens, the schedule then advances to its first occurrence after
 * `now`: an occurrence is fired or dropped exactly once, never re-evaluated.
 */
export interface PlanFiresInput {
  cron: CompiledCron;
  /** The schedule's next unfired occurrence (the claimed row's next_run_at). */
  nextRunAt: Date;
  /** Evaluation time (the database clock of the tick). */
  now: Date;
  policy: MisfirePolicy;
  graceMs: number;
  maxCatchUp: number;
}

export interface FirePlan {
  /** Occurrences to enqueue a job for, oldest first. */
  fireTimes: Date[];
  /** Late occurrences dropped by the policy (a lower bound if skippedMore). */
  skipped: number;
  /** True if the walk stopped early and even more occurrences were dropped. */
  skippedMore: boolean;
  /** Oldest dropped occurrence seen, for logging. */
  oldestSkipped: Date | null;
  /** The schedule's new next_run_at: first occurrence strictly after `now`. */
  nextRunAt: Date;
}

/**
 * Upper bound on on-time occurrences collected per evaluation. With minute
 * resolution and grace <= 1 h this is never reached; it only guards the walk.
 */
export const MAX_ON_TIME_FIRES = 120;

export function planFires(input: PlanFiresInput): FirePlan {
  const { cron, nextRunAt, now, policy, graceMs, maxCatchUp } = input;
  if (nextRunAt.getTime() > now.getTime()) {
    return { fireTimes: [], skipped: 0, skippedMore: false, oldestSkipped: null, nextRunAt };
  }

  const lateBefore = now.getTime() - graceMs;
  const onTime: Date[] = [];
  const late: Date[] = []; // newest first
  // Late occurrences worth collecting: enough to apply the policy plus one
  // more, which tells us whether anything beyond that was dropped.
  const lateWanted = policy === 'catch_up' ? maxCatchUp + 1 : 2;
  let exhausted = true;
  let sawNextRunAt = false;

  for (const t of cron.walkBack(now)) {
    if (t.getTime() < nextRunAt.getTime()) break;
    if (t.getTime() === nextRunAt.getTime()) sawNextRunAt = true;
    if (t.getTime() >= lateBefore) {
      if (onTime.length >= MAX_ON_TIME_FIRES) {
        exhausted = false;
        break;
      }
      onTime.push(t);
    } else {
      if (late.length >= lateWanted) {
        exhausted = false;
        break;
      }
      late.push(t);
    }
  }
  // next_run_at always came from cron.next(), so the walk normally reaches
  // it. If the expression or zone was edited underneath (or a DST rule moved),
  // still honour the occurrence the schedule promised.
  if (exhausted && !sawNextRunAt) {
    (nextRunAt.getTime() >= lateBefore ? onTime : late).push(nextRunAt);
  }

  let firedLate: Date[];
  switch (policy) {
    case 'skip':
      firedLate = [];
      break;
    case 'fire_once':
      firedLate = onTime.length === 0 && late[0] !== undefined ? [late[0]] : [];
      break;
    case 'catch_up':
      firedLate = late.slice(0, maxCatchUp);
      break;
  }

  const dropped = late.slice(firedLate.length);
  const fireTimes = [...firedLate, ...onTime].sort((a, b) => a.getTime() - b.getTime());
  return {
    fireTimes,
    skipped: dropped.length,
    skippedMore: !exhausted,
    oldestSkipped: dropped.at(-1) ?? null,
    nextRunAt: cron.next(now),
  };
}
