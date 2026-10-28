import { writeFile } from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import type { Queryable } from '../db/pool.js';
import type { Logger } from '../lib/logger.js';
import { NOOP_SCHEDULER_METRICS, type SchedulerMetrics } from '../observability/metrics.js';
import { Sleeper, jitter } from '../worker/sleeper.js';
import type { LeaderLock } from './leader-lock.js';
import type { TickResult } from './tick.js';

export type TickFn = (db: Queryable) => Promise<TickResult>;

export interface SchedulerOptions {
  lock: LeaderLock;
  /** One tick transaction on the leader session (runTick bound to its options). */
  tick: TickFn;
  logger: Logger;
  schedulerId: string;
  tickIntervalMs: number;
  leaderRetryIntervalMs: number;
  /** A tick that claims a full batch is followed by another, up to this many in a row. */
  batchSize: number;
  maxBatchesPerTick?: number;
  /** Consecutive failed ticks (on a live session) before voluntarily stepping down. */
  maxConsecutiveFailures?: number;
  livenessFile?: string;
  random?: () => number;
  /** M7: leadership gauge, tick counters and durations (no-op when omitted). */
  metrics?: SchedulerMetrics;
}

export type SchedulerRole = 'leader' | 'follower';

export interface StopOptions {
  /** How long to wait for the in-flight tick before abandoning it. */
  timeoutMs: number;
}

const DEFAULT_MAX_BATCHES_PER_TICK = 10;
const DEFAULT_MAX_CONSECUTIVE_FAILURES = 3;
const LIVENESS_TOUCH_INTERVAL_MS = 5_000;

/**
 * The scheduler loop. Every replica runs one; leader election makes sure only
 * one of them ticks:
 *
 *   follower: try the leader lock every ~leaderRetryIntervalMs (jittered).
 *   leader:   tick every ~tickIntervalMs; a full batch ticks again at once.
 *
 * Leadership is lost when the lock session dies (detected by the lock or by a
 * failed tick) and given up voluntarily after maxConsecutiveFailures failed
 * ticks, so a sick replica cannot block scheduling. stop() lets the current
 * tick finish (bounded), then releases the lock so a follower takes over
 * within one retry interval.
 */
export class Scheduler {
  private readonly sleeper = new Sleeper();
  private readonly random: () => number;
  private readonly maxBatches: number;
  private readonly maxFailures: number;
  private loop: Promise<void> | null = null;
  private stopping = false;
  private stopPromise: Promise<void> | null = null;
  private leader = false;
  private failures = 0;
  private readonly metrics: SchedulerMetrics;
  private lastLivenessTouch = 0;

  constructor(private readonly options: SchedulerOptions) {
    this.random = options.random ?? Math.random;
    this.maxBatches = options.maxBatchesPerTick ?? DEFAULT_MAX_BATCHES_PER_TICK;
    this.maxFailures = options.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_FAILURES;
    this.metrics = options.metrics ?? NOOP_SCHEDULER_METRICS;
  }

  get role(): SchedulerRole {
    return this.leader ? 'leader' : 'follower';
  }

  start(): void {
    if (this.loop) return;
    this.options.logger.info(
      {
        event: 'scheduler.started',
        schedulerId: this.options.schedulerId,
        tickIntervalMs: this.options.tickIntervalMs,
        leaderRetryIntervalMs: this.options.leaderRetryIntervalMs,
        batchSize: this.options.batchSize,
      },
      'Scheduler started',
    );
    this.loop = this.run();
  }

  stop(options: StopOptions): Promise<void> {
    this.stopPromise ??= this.doStop(options);
    return this.stopPromise;
  }

  /**
   * One iteration of the loop: become leader if possible, tick if leader.
   * Returns how long to wait before the next iteration. Exposed for tests.
   */
  async step(): Promise<number> {
    const { lock, logger, schedulerId } = this.options;
    this.touchLiveness();

    if (this.leader && !lock.held) {
      this.stepDown('session_lost');
      await lock.reset();
    }

    if (!this.leader) {
      let acquired = false;
      try {
        acquired = await lock.tryAcquire();
      } catch (err) {
        logger.warn(
          { event: 'scheduler.leader_acquire_failed', schedulerId, err },
          'Could not try the scheduler leader lock',
        );
      }
      if (!acquired) return this.retryDelay();
      this.leader = true;
      this.failures = 0;
      this.metrics.leadershipChanged('acquired');
      logger.info({ event: 'scheduler.leader_acquired', schedulerId }, 'Became scheduler leader');
    }

    const tickStartedAt = performance.now();
    try {
      const total: TickResult = { claimed: 0, fired: 0, duplicates: 0, skipped: 0, errors: 0 };
      let batches = 0;
      let full = false;
      do {
        const r = await lock.withSession(this.options.tick);
        batches++;
        for (const k of Object.keys(total) as Array<keyof TickResult>) total[k] += r[k];
        full = r.claimed >= this.options.batchSize;
      } while (full && batches < this.maxBatches && !this.stopping);
      this.failures = 0;
      this.metrics.tickCompleted(total, (performance.now() - tickStartedAt) / 1000);
      logger.debug({ event: 'scheduler.tick', schedulerId, batches, ...total }, 'Scheduler tick');
      // Still behind after maxBatches: yield briefly, then continue.
      return full ? 0 : jitter(this.options.tickIntervalMs, 0.1, this.random);
    } catch (err) {
      this.failures++;
      this.metrics.tickFailed((performance.now() - tickStartedAt) / 1000);
      logger.error(
        { event: 'scheduler.tick_failed', schedulerId, err, consecutiveFailures: this.failures },
        'Scheduler tick failed',
      );
      if (!lock.held) {
        this.stepDown('session_lost');
        await lock.reset();
        return this.retryDelay();
      }
      if (this.failures >= this.maxFailures) {
        this.stepDown('too_many_failures');
        await lock.release();
        return this.retryDelay();
      }
      const backoff = Math.min(
        this.options.tickIntervalMs * 2 ** this.failures,
        this.options.leaderRetryIntervalMs,
      );
      return jitter(backoff, 0.1, this.random);
    }
  }

  private async run(): Promise<void> {
    while (!this.stopping) {
      const delay = await this.step();
      if (this.stopping) break;
      if (delay > 0) await this.sleeper.sleep(delay);
      else await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }

  private async doStop({ timeoutMs }: StopOptions): Promise<void> {
    this.stopping = true;
    this.sleeper.wake();
    if (this.loop) {
      let timer: NodeJS.Timeout | undefined;
      const timedOut = await Promise.race([
        this.loop.then(() => false),
        new Promise<boolean>((resolve) => {
          timer = setTimeout(() => resolve(true), timeoutMs);
        }),
      ]);
      clearTimeout(timer);
      if (timedOut) {
        // Closing the session below aborts the tick's transaction: nothing it
        // did is committed, and the next leader re-evaluates those schedules.
        this.options.logger.warn(
          { event: 'scheduler.stop_timeout', schedulerId: this.options.schedulerId, timeoutMs },
          'In-flight tick did not finish in time; abandoning it',
        );
      }
    }
    const wasLeader = this.leader;
    this.leader = false;
    await this.options.lock.release();
    if (wasLeader) {
      this.metrics.leadershipChanged('released');
      this.options.logger.info(
        { event: 'scheduler.leader_released', schedulerId: this.options.schedulerId },
        'Released scheduler leadership',
      );
    }
    this.options.logger.info(
      { event: 'scheduler.stopped', schedulerId: this.options.schedulerId },
      'Scheduler stopped',
    );
  }

  private stepDown(reason: 'session_lost' | 'too_many_failures'): void {
    this.leader = false;
    this.metrics.leadershipChanged('lost');
    this.options.logger.warn(
      { event: 'scheduler.leader_lost', schedulerId: this.options.schedulerId, reason },
      'Lost scheduler leadership',
    );
  }

  private retryDelay(): number {
    return jitter(this.options.leaderRetryIntervalMs, 0.2, this.random);
  }

  private touchLiveness(): void {
    const file = this.options.livenessFile;
    if (!file) return;
    const now = Date.now();
    if (now - this.lastLivenessTouch < LIVENESS_TOUCH_INTERVAL_MS) return;
    this.lastLivenessTouch = now;
    writeFile(file, String(now)).catch((err: unknown) => {
      this.options.logger.warn({ err, file }, 'Could not write liveness file');
    });
  }
}
