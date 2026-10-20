import {
  IdempotencyKeyReusedError,
  NotFoundError,
  PayloadTooLargeError,
} from '../domain/errors.js';
import {
  JOB_LIMITS,
  type EnqueueJobInput,
  type EnqueueJobResult,
  type Job,
  type JobPage,
  type ListJobsInput,
} from '../domain/job.js';
import { decodeCursor, encodeCursor } from '../lib/cursor.js';
import { requestFingerprint } from '../lib/fingerprint.js';
import type { JobRepository, StoredJob } from '../repositories/job.repository.js';
import type { QueueRepository } from '../repositories/queue.repository.js';

/** The slices of the repositories JobService needs; lets tests pass in-memory fakes. */
export type QueueStore = Pick<QueueRepository, 'ensure' | 'findByName'>;
export type JobStore = Pick<JobRepository, 'insertIdempotent' | 'findById' | 'listByQueue'>;

export interface JobServiceOptions {
  maxPayloadBytes: number;
}

/**
 * Business rules for jobs: defaults, payload budget, initial status,
 * idempotent replay semantics and pagination. No SQL, no HTTP.
 */
export class JobService {
  constructor(
    private readonly queues: QueueStore,
    private readonly jobs: JobStore,
    private readonly options: JobServiceOptions,
  ) {}

  async enqueue(input: EnqueueJobInput): Promise<EnqueueJobResult> {
    const normalized = {
      payload: input.payload,
      priority: input.priority ?? JOB_LIMITS.DEFAULT_PRIORITY,
      delayMs: input.delayMs ?? 0,
      maxAttempts: input.maxAttempts ?? JOB_LIMITS.DEFAULT_MAX_ATTEMPTS,
    };

    const payloadBytes = Buffer.byteLength(JSON.stringify(normalized.payload), 'utf8');
    if (payloadBytes > this.options.maxPayloadBytes) {
      throw new PayloadTooLargeError('Job payload exceeds the size limit', {
        limitBytes: this.options.maxPayloadBytes,
        actualBytes: payloadBytes,
      });
    }

    const idempotencyKey = input.idempotencyKey ?? null;
    // The fingerprint covers what the producer asked for (after defaults),
    // not derived state like run_at, so an honest retry always matches.
    const fingerprint = idempotencyKey !== null ? requestFingerprint(normalized) : null;

    const queue = await this.queues.ensure(input.queue);
    const { job, inserted } = await this.jobs.insertIdempotent({
      queueId: queue.id,
      status: normalized.delayMs > 0 ? 'scheduled' : 'queued',
      priority: normalized.priority,
      payload: normalized.payload,
      maxAttempts: normalized.maxAttempts,
      delayMs: normalized.delayMs,
      idempotencyKey,
      requestFingerprint: fingerprint,
    });

    if (!inserted && job.requestFingerprint !== fingerprint) {
      throw new IdempotencyKeyReusedError(undefined, { idempotencyKey, jobId: job.id });
    }

    // On replay the job is returned in its *current* state (it may already be
    // running or finished); the producer learns the outcome of its first call.
    return { job: toJob(job), created: inserted };
  }

  async getJob(id: string): Promise<Job> {
    const job = await this.jobs.findById(id);
    if (!job) throw new NotFoundError('Job not found', { id });
    return toJob(job);
  }

  async listJobs(input: ListJobsInput): Promise<JobPage> {
    // Decode first: a malformed cursor is a 400 regardless of the queue.
    const after = input.cursor !== undefined ? decodeCursor(input.cursor) : undefined;

    // Reads never create queues; an unknown queue is a 404, not an empty page.
    const queue = await this.queues.findByName(input.queue);
    if (!queue) throw new NotFoundError('Queue not found', { queue: input.queue });

    // Fetch one extra row to know whether another page exists, so the last
    // page reports nextCursor = null instead of forcing an empty round-trip.
    const rows = await this.jobs.listByQueue({
      queueId: queue.id,
      ...(input.status !== undefined ? { status: input.status } : {}),
      limit: input.limit + 1,
      ...(after !== undefined ? { after } : {}),
    });

    const page = rows.slice(0, input.limit);
    const last = page[page.length - 1];
    const nextCursor =
      rows.length > input.limit && last
        ? encodeCursor({ createdAt: last.createdAtCursor, id: last.id })
        : null;

    return { jobs: page.map(toJob), nextCursor };
  }
}

/** Strip storage-only fields before a job leaves the service layer. */
function toJob(stored: StoredJob): Job {
  const { requestFingerprint: _fp, createdAtCursor: _cursor, ...job } = stored;
  return job;
}
