import pino from 'pino';
import { describe, expect, it } from 'vitest';
import type { Queryable } from '../../src/db/pool.js';
import type { LeaderLock } from '../../src/scheduler/leader-lock.js';
import { Scheduler, type SchedulerOptions } from '../../src/scheduler/scheduler.js';
import type { TickResult } from '../../src/scheduler/tick.js';

interface LogLine {
  event?: string;
  reason?: string;
  [key: string]: unknown;
}

function capture() {
  const lines: LogLine[] = [];
  const logger = pino(
    { level: 'debug' },
    { write: (s: string) => void lines.push(JSON.parse(s) as LogLine) },
  );
  return { logger, events: () => lines.map((l) => l.event).filter(Boolean) };
}

/** Scriptable in-memory lock: `available` decides whether tryAcquire wins. */
class FakeLock implements LeaderLock {
  available = true;
  holding = false;
  acquireError: Error | null = null;
  releases = 0;
  resets = 0;
  get held(): boolean {
    return this.holding;
  }
  async tryAcquire(): Promise<boolean> {
    if (this.acquireError) throw this.acquireError;
    if (this.holding) return true;
    this.holding = this.available;
    return this.holding;
  }
  async withSession<T>(fn: (db: Queryable) => Promise<T>): Promise<T> {
    if (!this.holding) throw new Error('not held');
    return fn({} as Queryable);
  }
  async release(): Promise<void> {
    this.releases++;
    this.holding = false;
  }
  async reset(): Promise<void> {
    this.resets++;
    this.holding = false;
  }
}

const result = (claimed = 0, fired = 0): TickResult => ({
  claimed,
  fired,
  duplicates: 0,
  skipped: 0,
  errors: 0,
});

function setup(overrides: Partial<SchedulerOptions> = {}, ticks: Array<TickResult | Error> = []) {
  const lock = new FakeLock();
  const log = capture();
  let calls = 0;
  const scheduler = new Scheduler({
    lock,
    logger: log.logger,
    schedulerId: 'test',
    tickIntervalMs: 1_000,
    leaderRetryIntervalMs: 5_000,
    batchSize: 10,
    random: () => 0.5, // jitter() then returns exactly the base value
    tick: async () => {
      const next = ticks[calls++] ?? result();
      if (next instanceof Error) throw next;
      return next;
    },
    ...overrides,
  });
  return { lock, log, scheduler, tickCalls: () => calls };
}

describe('Scheduler', () => {
  it('a follower does not tick and retries after the leader retry interval', async () => {
    const { lock, scheduler, tickCalls } = setup();
    lock.available = false;
    expect(await scheduler.step()).toBe(5_000);
    expect(scheduler.role).toBe('follower');
    expect(tickCalls()).toBe(0);
  });

  it('the leader ticks every tick interval and logs leadership once', async () => {
    const { log, scheduler, tickCalls } = setup();
    expect(await scheduler.step()).toBe(1_000);
    expect(await scheduler.step()).toBe(1_000);
    expect(scheduler.role).toBe('leader');
    expect(tickCalls()).toBe(2);
    expect(log.events().filter((e) => e === 'scheduler.leader_acquired')).toHaveLength(1);
    expect(log.events()).toContain('scheduler.tick');
  });

  it('drains a backlog: full batches tick again immediately, bounded per step', async () => {
    const full = result(10, 10);
    const { scheduler, tickCalls } = setup({ maxBatchesPerTick: 3 }, [full, full, result(4)]);
    expect(await scheduler.step()).toBe(1_000);
    expect(tickCalls()).toBe(3);

    const busy = setup({ maxBatchesPerTick: 2 }, [full, full, full]);
    expect(await busy.scheduler.step()).toBe(0); // still behind: yield, then continue
    expect(busy.tickCalls()).toBe(2);
  });

  it('steps down when the lock session is lost and becomes a follower', async () => {
    const { lock, log, scheduler } = setup();
    await scheduler.step();
    lock.holding = false; // session died (lock noticed the connection error)
    lock.available = false; // someone else took over
    expect(await scheduler.step()).toBe(5_000);
    expect(scheduler.role).toBe('follower');
    expect(lock.resets).toBeGreaterThan(0);
    const lost = log.events().indexOf('scheduler.leader_lost');
    expect(lost).toBeGreaterThan(-1);
  });

  it('a failed tick on a dead session steps down immediately', async () => {
    const { lock, log, scheduler } = setup();
    let first = true;
    scheduler['options'].tick = async () => {
      if (first) {
        first = false;
        lock.holding = false;
        throw new Error('Connection terminated unexpectedly');
      }
      return result();
    };
    expect(await scheduler.step()).toBe(5_000);
    expect(scheduler.role).toBe('follower');
    expect(log.events()).toEqual(
      expect.arrayContaining(['scheduler.tick_failed', 'scheduler.leader_lost']),
    );
  });

  it('backs off on failed ticks and gives up leadership after too many', async () => {
    const boom = new Error('statement timeout');
    const { lock, log, scheduler } = setup({ maxConsecutiveFailures: 3 }, [boom, boom, boom]);
    expect(await scheduler.step()).toBe(2_000);
    expect(await scheduler.step()).toBe(4_000);
    expect(scheduler.role).toBe('leader');
    expect(await scheduler.step()).toBe(5_000);
    expect(scheduler.role).toBe('follower');
    expect(lock.releases).toBe(1);
    expect(log.events().filter((e) => e === 'scheduler.tick_failed')).toHaveLength(3);
  });

  it('a success resets the failure count', async () => {
    const boom = new Error('blip');
    const { scheduler } = setup({ maxConsecutiveFailures: 2 }, [boom, result(), boom]);
    await scheduler.step();
    await scheduler.step();
    await scheduler.step();
    expect(scheduler.role).toBe('leader');
  });

  it('survives a failing lock attempt (database down) as a follower', async () => {
    const { lock, log, scheduler } = setup();
    lock.acquireError = new Error('ECONNREFUSED');
    expect(await scheduler.step()).toBe(5_000);
    expect(log.events()).toContain('scheduler.leader_acquire_failed');
  });

  it('stop() waits for the in-flight tick, then releases leadership', async () => {
    let finishTick: (() => void) | undefined;
    const { lock, log, scheduler } = setup();
    let ticking = false;
    scheduler['options'].tick = () =>
      new Promise<TickResult>((resolve) => {
        ticking = true;
        finishTick = () => resolve(result());
      });
    scheduler.start();
    await expect.poll(() => ticking).toBe(true);

    const stopped = scheduler.stop({ timeoutMs: 5_000 });
    await new Promise((r) => setTimeout(r, 20));
    expect(lock.releases).toBe(0); // still waiting for the tick
    finishTick?.();
    await stopped;
    expect(lock.releases).toBe(1);
    expect(log.events().slice(-2)).toEqual(['scheduler.leader_released', 'scheduler.stopped']);
    expect(scheduler.role).toBe('follower');
  });

  it('stop() abandons a hung tick after the timeout', async () => {
    const { lock, log, scheduler } = setup();
    let ticking = false;
    scheduler['options'].tick = () => {
      ticking = true;
      return new Promise<TickResult>(() => undefined);
    };
    scheduler.start();
    await expect.poll(() => ticking).toBe(true);
    await scheduler.stop({ timeoutMs: 50 });
    expect(lock.releases).toBe(1);
    expect(log.events()).toContain('scheduler.stop_timeout');
  });

  it('stop() on a follower does not log a leadership release', async () => {
    const { lock, log, scheduler } = setup();
    lock.available = false;
    scheduler.start();
    await expect.poll(() => log.events().includes('scheduler.started')).toBe(true);
    await scheduler.stop({ timeoutMs: 1_000 });
    expect(log.events()).not.toContain('scheduler.leader_released');
    expect(log.events()).toContain('scheduler.stopped');
  });
});
