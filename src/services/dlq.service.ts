import { JobNotDeadLetteredError, NotFoundError } from '../domain/errors.js';
import type {
  BulkReplayInput,
  BulkReplayResult,
  DeadLetterPage,
  DeadLetteredJob,
  DeadLetteredJobWithAttempts,
  ListDeadLettersInput,
  PurgeDeadLettersInput,
  PurgeResult,
  ReplayJobInput,
} from '../domain/dead-letter.js';
import type { Job, JobId, Queue, QueueName } from '../domain/job.js';
import { NOOP_QUEUE_SIGNALS, type QueueSignals } from '../domain/signals.js';
import { decodeCursor, encodeCursor } from '../lib/cursor.js';
import type {
  DeadLetterRepository,
  StoredDeadLetterJob,
} from '../repositories/dead-letter.repository.js';
import type { QueueStore } from './job.service.js';

/** The slice of DeadLetterRepository DlqService needs; lets tests pass an in-memory fake. */
export type DeadLetterStore = Pick<
  DeadLetterRepository,
  | 'findById'
  | 'listByQueue'
  | 'listAttempts'
  | 'replay'
  | 'replayBatch'
  | 'purge'
  | 'purgeOlderThan'
>;

export interface DlqServiceOptions {
  /** Replays wake workers; purges invalidate stats (Milestone 5). */
  signals?: QueueSignals;
}

/** Result of a single replay: the job as it is now (queued or scheduled). */
export interface ReplayJobResult {
  job: Job;
  /** How many times the job has now been replayed (1 on its first replay). */
  replayCount: number;
  /** Why it had been dead-lettered, for the audit log. */
  previousReason: DeadLetteredJob['deadLetter']['reason'];
}

/**
 * Business rules for the dead-letter queue: inspection, replay and purge.
 * No SQL, no HTTP. Atomicity and at-most-once replay/purge come from the
 * repository's single guarded statements; this layer turns "nothing matched"
 * into the right typed error (404 vs 409) for the caller.
 */
export class DlqService {
  private readonly signals: QueueSignals;

  constructor(
    private readonly queues: QueueStore,
    private readonly deadLetters: DeadLetterStore,
    options: DlqServiceOptions = {},
  ) {
    this.signals = options.signals ?? NOOP_QUEUE_SIGNALS;
  }

  async list(input: ListDeadLettersInput): Promise<DeadLetterPage> {
    // Decode first: a malformed cursor is a 400 regardless of the queue.
    const after = input.cursor !== undefined ? decodeCursor(input.cursor) : undefined;
    const queue = await this.requireQueue(input.queue);

    const rows = await this.deadLetters.listByQueue({
      queueId: queue.id,
      ...(input.reason !== undefined ? { reason: input.reason } : {}),
      limit: input.limit + 1,
      ...(after !== undefined ? { after } : {}),
    });

    const page = rows.slice(0, input.limit);
    const last = page[page.length - 1];
    const nextCursor =
      rows.length > input.limit && last?.deadLetteredAtCursor
        ? encodeCursor({ createdAt: last.deadLetteredAtCursor, id: last.id })
        : null;

    return { jobs: page.map(toDeadLetteredJob), nextCursor };
  }

  /** A dead-lettered job with its full attempt history; 404 if it is not in the DLQ. */
  async get(jobId: string): Promise<DeadLetteredJobWithAttempts> {
    const stored = await this.deadLetters.findById(jobId);
    if (!stored || stored.status !== 'dead') {
      throw new NotFoundError('Dead-lettered job not found', {
        id: jobId,
        ...(stored ? { status: stored.status } : {}),
      });
    }
    const attemptHistory = await this.deadLetters.listAttempts(jobId);
    return { ...toDeadLetteredJob(stored), attemptHistory };
  }

  /**
   * Replay one dead-lettered job: fresh retry budget, runnable now (or after
   * delayMs). Safe under concurrency: of N simultaneous replays exactly one
   * succeeds; the rest get JobNotDeadLetteredError (409) because the job has
   * already left the DLQ.
   */
  async replay(input: ReplayJobInput): Promise<ReplayJobResult> {
    const queue = await this.requireQueue(input.queue);
    // The read is a cheap fast path for clear 404/409s and gives the reason
    // for the audit log; the guarded UPDATE below is what actually decides,
    // so a race between the two is harmless.
    const before = await this.deadLetters.findById(input.jobId);
    if (!before || before.queue !== input.queue) {
      throw new NotFoundError('Job not found in this queue', {
        id: input.jobId,
        queue: input.queue,
      });
    }
    if (before.status !== 'dead' || before.deadReason === null) {
      throw new JobNotDeadLetteredError(undefined, { id: input.jobId, status: before.status });
    }
    const previousReason = before.deadReason;

    const replayed = await this.deadLetters.replay({
      jobId: input.jobId,
      queueId: queue.id,
      delayMs: input.delayMs ?? 0,
    });
    if (!replayed) {
      // Lost a race: another replay (or a purge) got there first.
      const current = await this.deadLetters.findById(input.jobId);
      if (!current) throw new NotFoundError('Job not found', { id: input.jobId });
      throw new JobNotDeadLetteredError(undefined, { id: input.jobId, status: current.status });
    }
    this.signals.jobsRunnable(input.queue);
    return { job: toJob(replayed), replayCount: replayed.replayCount, previousReason };
  }

  async replayBatch(input: BulkReplayInput): Promise<BulkReplayResult> {
    const queue = await this.requireQueue(input.queue);
    const jobIds = await this.deadLetters.replayBatch({
      queueId: queue.id,
      limit: input.limit,
      delayMs: input.delayMs ?? 0,
      ...(input.reason !== undefined ? { reason: input.reason } : {}),
    });
    if (jobIds.length > 0) this.signals.jobsRunnable(input.queue);
    return { replayed: jobIds.length, jobIds };
  }

  /** Permanently delete one dead-lettered job and its attempt history. */
  async purge(queueName: QueueName, jobId: JobId): Promise<void> {
    const queue = await this.requireQueue(queueName);
    if (await this.deadLetters.purge(jobId, queue.id)) {
      this.signals.jobsChanged(queueName);
      return;
    }

    const current = await this.deadLetters.findById(jobId);
    if (!current || current.queue !== queueName) {
      throw new NotFoundError('Job not found in this queue', { id: jobId, queue: queueName });
    }
    throw new JobNotDeadLetteredError(undefined, { id: jobId, status: current.status });
  }

  async purgeOlderThan(input: PurgeDeadLettersInput): Promise<PurgeResult> {
    const queue = await this.requireQueue(input.queue);
    const purged = await this.deadLetters.purgeOlderThan({
      queueId: queue.id,
      olderThan: input.olderThan,
      limit: input.limit,
    });
    if (purged > 0) this.signals.jobsChanged(input.queue);
    return { purged };
  }

  /** DLQ operations never create queues; an unknown queue is a 404. */
  private async requireQueue(name: QueueName): Promise<Queue> {
    const queue = await this.queues.findByName(name);
    if (!queue) throw new NotFoundError('Queue not found', { queue: name });
    return queue;
  }
}

function toJob(stored: StoredDeadLetterJob): Job {
  return {
    id: stored.id,
    queue: stored.queue,
    status: stored.status,
    payload: stored.payload,
    priority: stored.priority,
    attempts: stored.attempts,
    maxAttempts: stored.maxAttempts,
    runAt: stored.runAt,
    idempotencyKey: stored.idempotencyKey,
    lastError: stored.lastError,
    finishedAt: stored.finishedAt,
    createdAt: stored.createdAt,
    updatedAt: stored.updatedAt,
  };
}

function toDeadLetteredJob(stored: StoredDeadLetterJob): DeadLetteredJob {
  if (stored.status !== 'dead' || !stored.deadReason || !stored.deadLetteredAt) {
    // jobs_dead_has_metadata makes this unreachable for rows the DLQ queries return.
    throw new Error(`Job ${stored.id} is not dead-lettered (status ${stored.status})`);
  }
  return {
    ...toJob(stored),
    status: 'dead',
    deadLetter: {
      reason: stored.deadReason,
      deadLetteredAt: stored.deadLetteredAt,
      attempts: stored.attempts,
      lastError: stored.lastError,
      queue: stored.queue,
      replayCount: stored.replayCount,
      lastReplayedAt: stored.lastReplayedAt,
    },
  };
}
