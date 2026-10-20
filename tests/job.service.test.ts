import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  IdempotencyKeyReusedError,
  NotFoundError,
  PayloadTooLargeError,
  ValidationError,
} from '../src/domain/errors.js';
import type { JobId, Queue, QueueName } from '../src/domain/job.js';
import { decodeCursor, encodeCursor } from '../src/lib/cursor.js';
import type {
  InsertJobParams,
  InsertJobResult,
  ListJobsParams,
  StoredJob,
} from '../src/repositories/job.repository.js';
import { JobService, type JobStore, type QueueStore } from '../src/services/job.service.js';

/** In-memory fakes: exercise business rules without Postgres. */
class FakeQueues implements QueueStore {
  readonly queues = new Map<string, Queue>();
  ensureCalls = 0;

  async ensure(name: QueueName): Promise<Queue> {
    this.ensureCalls++;
    const existing = this.queues.get(name);
    if (existing) return existing;
    const queue: Queue = { id: String(this.queues.size + 1), name, createdAt: new Date() };
    this.queues.set(name, queue);
    return queue;
  }

  async findByName(name: QueueName): Promise<Queue | null> {
    return this.queues.get(name) ?? null;
  }
}

class FakeJobs implements JobStore {
  readonly rows: Array<StoredJob & { queueId: string }> = [];
  lastInsert: InsertJobParams | undefined;
  lastList: ListJobsParams | undefined;
  private tick = 0;

  async insertIdempotent(params: InsertJobParams): Promise<InsertJobResult> {
    this.lastInsert = params;
    if (params.idempotencyKey !== null) {
      const existing = this.rows.find(
        (r) => r.queueId === params.queueId && r.idempotencyKey === params.idempotencyKey,
      );
      if (existing) return { job: existing, inserted: false };
    }
    const now = new Date(Date.UTC(2026, 9, 20, 0, 0, 0, this.tick++));
    const job: StoredJob & { queueId: string } = {
      queueId: params.queueId,
      id: randomUUID() as JobId,
      queue: 'q' as QueueName,
      status: params.status,
      payload: params.payload,
      priority: params.priority,
      attempts: 0,
      maxAttempts: params.maxAttempts,
      runAt: new Date(now.getTime() + params.delayMs),
      idempotencyKey: params.idempotencyKey,
      lastError: null,
      finishedAt: null,
      createdAt: now,
      updatedAt: now,
      requestFingerprint: params.requestFingerprint,
      createdAtCursor: now.toISOString().replace('Z', '000Z'),
    };
    this.rows.push(job);
    return { job, inserted: true };
  }

  async findById(id: string): Promise<StoredJob | null> {
    return this.rows.find((r) => r.id === id) ?? null;
  }

  async listByQueue(params: ListJobsParams): Promise<StoredJob[]> {
    this.lastList = params;
    return this.rows.filter((r) => r.queueId === params.queueId).slice(0, params.limit);
  }
}

const queue = 'emails' as QueueName;

describe('JobService.enqueue', () => {
  let queues: FakeQueues;
  let jobs: FakeJobs;
  let service: JobService;

  beforeEach(() => {
    queues = new FakeQueues();
    jobs = new FakeJobs();
    service = new JobService(queues, jobs, { maxPayloadBytes: 64 });
  });

  it('applies defaults and enqueues as queued', async () => {
    const { job, created } = await service.enqueue({ queue, payload: { a: 1 } });
    expect(created).toBe(true);
    expect(jobs.lastInsert).toMatchObject({
      status: 'queued',
      priority: 0,
      maxAttempts: 3,
      delayMs: 0,
      idempotencyKey: null,
      requestFingerprint: null,
    });
    expect(job).not.toHaveProperty('requestFingerprint');
    expect(job).not.toHaveProperty('createdAtCursor');
  });

  it('marks delayed jobs as scheduled', async () => {
    await service.enqueue({ queue, payload: {}, delayMs: 5_000 });
    expect(jobs.lastInsert?.status).toBe('scheduled');
    expect(jobs.lastInsert?.delayMs).toBe(5_000);
  });

  it('rejects payloads over the byte budget before touching storage', async () => {
    const payload = { text: 'x'.repeat(100) };
    await expect(service.enqueue({ queue, payload })).rejects.toBeInstanceOf(PayloadTooLargeError);
    expect(queues.ensureCalls).toBe(0);
    expect(jobs.rows).toHaveLength(0);
  });

  it('counts UTF-8 bytes, not characters', async () => {
    // 20 x "€" = 60 bytes of text + quotes/braces > 64 bytes, though only ~30 chars.
    await expect(service.enqueue({ queue, payload: { t: '€'.repeat(20) } })).rejects.toBeInstanceOf(
      PayloadTooLargeError,
    );
  });

  it('replays the original job for the same key and equivalent request', async () => {
    const first = await service.enqueue({ queue, payload: { a: 1, b: 2 }, idempotencyKey: 'k1' });
    const replay = await service.enqueue({
      queue,
      payload: { b: 2, a: 1 },
      priority: 0, // explicit default == omitted
      idempotencyKey: 'k1',
    });
    expect(first.created).toBe(true);
    expect(replay.created).toBe(false);
    expect(replay.job.id).toBe(first.job.id);
    expect(jobs.rows).toHaveLength(1);
  });

  it('rejects the same key with a different request', async () => {
    await service.enqueue({ queue, payload: { a: 1 }, idempotencyKey: 'k1' });
    await expect(
      service.enqueue({ queue, payload: { a: 2 }, idempotencyKey: 'k1' }),
    ).rejects.toBeInstanceOf(IdempotencyKeyReusedError);
    await expect(
      service.enqueue({ queue, payload: { a: 1 }, priority: 9, idempotencyKey: 'k1' }),
    ).rejects.toBeInstanceOf(IdempotencyKeyReusedError);
  });

  it('stores a fingerprint only when a key is supplied', async () => {
    await service.enqueue({ queue, payload: {}, idempotencyKey: 'k2' });
    expect(jobs.lastInsert?.requestFingerprint).toMatch(/^v1:[0-9a-f]{64}$/);
  });
});

describe('JobService.getJob / listJobs', () => {
  let queues: FakeQueues;
  let jobs: FakeJobs;
  let service: JobService;

  beforeEach(() => {
    queues = new FakeQueues();
    jobs = new FakeJobs();
    service = new JobService(queues, jobs, { maxPayloadBytes: 1024 });
  });

  it('throws NotFoundError for an unknown job', async () => {
    await expect(service.getJob(randomUUID())).rejects.toBeInstanceOf(NotFoundError);
  });

  it('throws NotFoundError for an unknown queue and never creates it', async () => {
    await expect(service.listJobs({ queue, limit: 10 })).rejects.toBeInstanceOf(NotFoundError);
    expect(queues.queues.size).toBe(0);
  });

  it('rejects a malformed cursor before any lookup', async () => {
    await expect(service.listJobs({ queue, limit: 10, cursor: '!!' })).rejects.toBeInstanceOf(
      ValidationError,
    );
  });

  it('asks for limit + 1 rows and emits a cursor only when more rows exist', async () => {
    for (let i = 0; i < 3; i++) await service.enqueue({ queue, payload: { i } });

    const page = await service.listJobs({ queue, limit: 2 });
    expect(jobs.lastList?.limit).toBe(3);
    expect(page.jobs).toHaveLength(2);
    expect(page.nextCursor).not.toBeNull();
    const cursor = decodeCursor(page.nextCursor!);
    expect(cursor.id).toBe(page.jobs[1]?.id);

    const exact = await service.listJobs({ queue, limit: 3 });
    expect(exact.jobs).toHaveLength(3);
    expect(exact.nextCursor).toBeNull();
  });

  it('passes the decoded cursor and status filter to the repository', async () => {
    await service.enqueue({ queue, payload: {} });
    const cursor = encodeCursor({
      createdAt: '2026-10-20T00:00:00.000123Z',
      id: randomUUID(),
    });
    await service.listJobs({ queue, limit: 5, status: 'scheduled', cursor });
    expect(jobs.lastList).toMatchObject({
      status: 'scheduled',
      after: decodeCursor(cursor),
      limit: 6,
    });
  });
});
