import type { ReclaimedJob } from '../domain/lease.js';
import type { Logger } from '../lib/logger.js';
import type { LeaseRepository } from '../repositories/lease.repository.js';
import { Sleeper, jitter } from './sleeper.js';
import type { JobFailedHook } from './worker.js';

export type ReapStore = Pick<LeaseRepository, 'reapExpired'>;

export interface ReaperOptions {
  store: ReapStore;
  logger: Logger;
  intervalMs: number;
  /** Rows reclaimed per statement; keeps each transaction short. */
  batchSize?: number;
  /** Upper bound on batches per tick so one tick cannot run unbounded. */
  maxBatchesPerTick?: number;
  /** Dead-letter a job as poison after this many lease expiries (see LeaseRepository.reapExpired). */
  poisonLeaseExpiries?: number;
  onJobFailed?: JobFailedHook;
  random?: () => number;
}

/**
 * Reclaims jobs whose lease expired because their worker crashed, hung or
 * lost its database connection. Every worker process runs one; FOR UPDATE
 * SKIP LOCKED in the reclaim statement makes concurrent reapers safe without
 * leader election, at the cost of a little redundant polling.
 */
export class Reaper {
  private readonly sleeper = new Sleeper();
  private readonly random: () => number;
  private readonly batchSize: number;
  private readonly maxBatches: number;
  private loopPromise: Promise<void> | null = null;
  private stopped = false;

  constructor(private readonly options: ReaperOptions) {
    this.random = options.random ?? Math.random;
    this.batchSize = options.batchSize ?? 100;
    this.maxBatches = options.maxBatchesPerTick ?? 10;
  }

  start(): void {
    if (this.loopPromise) throw new Error('Reaper already started');
    this.loopPromise = this.run();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.sleeper.wake();
    await this.loopPromise;
  }

  /** One reclaim pass. Never throws: a failed pass is logged and retried next tick. */
  async tick(): Promise<ReclaimedJob[]> {
    const reclaimed: ReclaimedJob[] = [];
    try {
      for (let i = 0; i < this.maxBatches; i++) {
        const batch = await this.options.store.reapExpired(
          this.batchSize,
          this.options.poisonLeaseExpiries !== undefined
            ? { poisonLeaseExpiries: this.options.poisonLeaseExpiries }
            : {},
        );
        reclaimed.push(...batch);
        for (const job of batch) await this.report(job);
        if (batch.length < this.batchSize) break;
      }
    } catch (err) {
      this.options.logger.error({ err }, 'Reaper pass failed');
    }
    return reclaimed;
  }

  private async run(): Promise<void> {
    while (!this.stopped) {
      await this.tick();
      if (this.stopped) break;
      await this.sleeper.sleep(jitter(this.options.intervalMs, 0.2, this.random));
    }
  }

  private async report(job: ReclaimedJob): Promise<void> {
    const fields = {
      jobId: job.jobId,
      queue: job.queue,
      attempt: job.attempt,
      maxAttempts: job.maxAttempts,
      previousWorkerId: job.previousWorkerId,
      leaseExpiries: job.leaseExpiries,
      status: job.status,
    };
    if (job.status === 'queued') {
      this.options.logger.warn(fields, 'Reclaimed expired lease, job requeued');
      return;
    }
    const reason = job.deadLetterReason ?? 'attempts_exhausted';
    this.options.logger.error(
      { ...fields, event: 'dlq.dead_lettered', reason, source: 'reaper' },
      reason === 'poison_lease_expired'
        ? 'Reclaimed expired lease: poison message, job moved to the dead-letter queue'
        : 'Reclaimed expired lease on the last attempt, job moved to the dead-letter queue',
    );
    if (!this.options.onJobFailed) return;
    try {
      await this.options.onJobFailed({
        jobId: job.jobId,
        queue: job.queue,
        attempt: job.attempt,
        maxAttempts: job.maxAttempts,
        error: `Lease expired: worker ${job.previousWorkerId} stopped heartbeating`,
        reason,
        source: 'reaper',
      });
    } catch (err) {
      this.options.logger.error({ err, jobId: job.jobId }, 'onJobFailed hook threw');
    }
  }
}
