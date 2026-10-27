import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  IdempotencyKeyReusedError,
  NotFoundError,
  PayloadTooLargeError,
  ValidationError,
} from '../src/domain/errors.js';
import { JOB_LIMITS, type JobId, type Queue, type QueueName } from '../src/domain/job.js';
import type { TenantId } from '../src/domain/tenant.js';
import { requestFingerprint } from '../src/lib/fingerprint.js';
import { decodeCursor, encodeCursor } from '../src/lib/cursor.js';
import type {
  InsertJobParams,
  InsertJobResult,
  ListJobsParams,
  StoredJob,
} from '../src/repositories/job.repository.js';
import { JobService, type JobStore, type QueueStore } from '../src/services/job.service.js';
import { RecordingSignals } from './support/recording-signals.js';

/** In-memory fakes: exercise business rules without Postgres. */
class FakeQueues implements QueueStore {
  readonly queues = new Map<string, Queue>();
  ensureCalls = 0;

  async ensure(tenantId: TenantId, name: QueueName): Promise<Queue> {
    this.ensureCalls++;
    const existing = this.queues.get(`${tenantId}:${name}`);
    if (existing) return existing;
    // The id encodes the owner so FakeJobs can scope lookups like the SQL does.
    const id = `${tenantId}/${this.queues.size + 1}`;
    const queue: Queue = { id, tenantId, name, createdAt: new Date() };
    this.queues.set(`${tenantId}:${name}`, queue);
    return queue;
  }

  async findByName(tenantId: TenantId, name: QueueName): Promise<Queue | null> {
    return this.queues.get(`${tenantId}:${name}`) ?? null;
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

  async findById(tenantId: TenantId, id: string): Promise<StoredJob | null> {
    return this.rows.find((r) => r.id === id && r.queueId.startsWith(`${tenantId}/`)) ?? null;
  }

  async listByQueue(params: ListJobsParams): Promise<StoredJob[]> {
    this.lastList = params;
    return this.rows.filter((r) => r.queueId === params.queueId).slice(0, params.limit);
  }
}

const queue = 'emails' as QueueName;
const tenant = 'acme' as TenantId;

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
    const { job, created } = await service.enqueue({ tenantId: tenant, queue, payload: { a: 1 } });
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
    await service.enqueue({ tenantId: tenant, queue, payload: {}, delayMs: 5_000 });
    expect(jobs.lastInsert?.status).toBe('scheduled');
    expect(jobs.lastInsert?.delayMs).toBe(5_000);
  });

  describe('runAt', () => {
    const NOW = Date.UTC(2026, 9, 23, 12, 0, 0);
    let pinned: JobService;

    beforeEach(() => {
      pinned = new JobService(queues, jobs, { maxPayloadBytes: 64, now: () => NOW });
    });

    it('enqueues a future runAt as scheduled and passes the instant to storage', async () => {
      const runAt = new Date(NOW + 60_000);
      await pinned.enqueue({ tenantId: tenant, queue, payload: {}, runAt });
      expect(jobs.lastInsert).toMatchObject({ status: 'scheduled', delayMs: 0, runAt });
    });

    it('enqueues a past or present runAt as queued (storage clamps it to now)', async () => {
      await pinned.enqueue({ tenantId: tenant, queue, payload: {}, runAt: new Date(NOW - 5_000) });
      expect(jobs.lastInsert?.status).toBe('queued');
      await pinned.enqueue({ tenantId: tenant, queue, payload: {}, runAt: new Date(NOW) });
      expect(jobs.lastInsert?.status).toBe('queued');
    });

    it('accepts exactly the 30-day horizon and rejects 1 ms beyond it before storage', async () => {
      await pinned.enqueue({
        tenantId: tenant,
        queue,
        payload: {},
        runAt: new Date(NOW + JOB_LIMITS.MAX_DELAY_MS),
      });
      expect(jobs.rows).toHaveLength(1);
      const err = await pinned
        .enqueue({
          tenantId: tenant,
          queue,
          payload: {},
          runAt: new Date(NOW + JOB_LIMITS.MAX_DELAY_MS + 1),
        })
        .catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ValidationError);
      expect((err as ValidationError).details).toMatchObject({
        formErrors: [],
        fieldErrors: { runAt: [expect.stringContaining('30 days')] },
      });
      expect(jobs.rows).toHaveLength(1);
      expect(queues.ensureCalls).toBe(1);
    });

    it('rejects delayMs and runAt together and invalid dates', async () => {
      await expect(
        pinned.enqueue({ tenantId: tenant, queue, payload: {}, delayMs: 1, runAt: new Date(NOW) }),
      ).rejects.toBeInstanceOf(ValidationError);
      await expect(
        pinned.enqueue({ tenantId: tenant, queue, payload: {}, runAt: new Date(Number.NaN) }),
      ).rejects.toBeInstanceOf(ValidationError);
      expect(jobs.rows).toHaveLength(0);
    });

    it('keeps pre-M4 fingerprints stable and makes runAt part of the request identity', async () => {
      await pinned.enqueue({ tenantId: tenant, queue, payload: { a: 1 }, idempotencyKey: 'k1' });
      const withoutRunAt = jobs.lastInsert?.requestFingerprint;
      expect(withoutRunAt).toBe(
        requestFingerprint({ payload: { a: 1 }, priority: 0, delayMs: 0, maxAttempts: 3 }),
      );
      await pinned.enqueue({
        tenantId: tenant,
        queue,
        payload: { a: 1 },
        runAt: new Date(NOW + 1000),
        idempotencyKey: 'k2',
      });
      expect(jobs.lastInsert?.requestFingerprint).not.toBe(withoutRunAt);
      await expect(
        pinned.enqueue({
          tenantId: tenant,
          queue,
          payload: { a: 1 },
          runAt: new Date(NOW + 2000),
          idempotencyKey: 'k2',
        }),
      ).rejects.toBeInstanceOf(IdempotencyKeyReusedError);
    });
  });

  it('rejects payloads over the byte budget before touching storage', async () => {
    const payload = { text: 'x'.repeat(100) };
    await expect(service.enqueue({ tenantId: tenant, queue, payload })).rejects.toBeInstanceOf(
      PayloadTooLargeError,
    );
    expect(queues.ensureCalls).toBe(0);
    expect(jobs.rows).toHaveLength(0);
  });

  it('counts UTF-8 bytes, not characters', async () => {
    // 20 x "€" = 60 bytes of text + quotes/braces > 64 bytes, though only ~30 chars.
    await expect(
      service.enqueue({ tenantId: tenant, queue, payload: { t: '€'.repeat(20) } }),
    ).rejects.toBeInstanceOf(PayloadTooLargeError);
  });

  it('replays the original job for the same key and equivalent request', async () => {
    const first = await service.enqueue({
      tenantId: tenant,
      queue,
      payload: { a: 1, b: 2 },
      idempotencyKey: 'k1',
    });
    const replay = await service.enqueue({
      tenantId: tenant,
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
    await service.enqueue({ tenantId: tenant, queue, payload: { a: 1 }, idempotencyKey: 'k1' });
    await expect(
      service.enqueue({ tenantId: tenant, queue, payload: { a: 2 }, idempotencyKey: 'k1' }),
    ).rejects.toBeInstanceOf(IdempotencyKeyReusedError);
    await expect(
      service.enqueue({
        tenantId: tenant,
        queue,
        payload: { a: 1 },
        priority: 9,
        idempotencyKey: 'k1',
      }),
    ).rejects.toBeInstanceOf(IdempotencyKeyReusedError);
  });

  it('stores a fingerprint only when a key is supplied', async () => {
    await service.enqueue({ tenantId: tenant, queue, payload: {}, idempotencyKey: 'k2' });
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
    await expect(service.getJob(tenant, randomUUID())).rejects.toBeInstanceOf(NotFoundError);
  });

  it('throws NotFoundError for an unknown queue and never creates it', async () => {
    await expect(service.listJobs({ tenantId: tenant, queue, limit: 10 })).rejects.toBeInstanceOf(
      NotFoundError,
    );
    expect(queues.queues.size).toBe(0);
  });

  it('rejects a malformed cursor before any lookup', async () => {
    await expect(
      service.listJobs({ tenantId: tenant, queue, limit: 10, cursor: '!!' }),
    ).rejects.toBeInstanceOf(ValidationError);
  });

  it('asks for limit + 1 rows and emits a cursor only when more rows exist', async () => {
    for (let i = 0; i < 3; i++) await service.enqueue({ tenantId: tenant, queue, payload: { i } });

    const page = await service.listJobs({ tenantId: tenant, queue, limit: 2 });
    expect(jobs.lastList?.limit).toBe(3);
    expect(page.jobs).toHaveLength(2);
    expect(page.nextCursor).not.toBeNull();
    const cursor = decodeCursor(page.nextCursor!);
    expect(cursor.id).toBe(page.jobs[1]?.id);

    const exact = await service.listJobs({ tenantId: tenant, queue, limit: 3 });
    expect(exact.jobs).toHaveLength(3);
    expect(exact.nextCursor).toBeNull();
  });

  it('passes the decoded cursor and status filter to the repository', async () => {
    await service.enqueue({ tenantId: tenant, queue, payload: {} });
    const cursor = encodeCursor({
      createdAt: '2026-10-20T00:00:00.000123Z',
      id: randomUUID(),
    });
    await service.listJobs({ tenantId: tenant, queue, limit: 5, status: 'scheduled', cursor });
    expect(jobs.lastList).toMatchObject({
      status: 'scheduled',
      after: decodeCursor(cursor),
      limit: 6,
    });
  });
});

describe('JobService signals (Milestone 5)', () => {
  it('signals jobsRunnable after an insert, for queued and scheduled jobs alike', async () => {
    const signals = new RecordingSignals();
    const service = new JobService(new FakeQueues(), new FakeJobs(), {
      maxPayloadBytes: 64,
      signals,
    });
    await service.enqueue({ tenantId: tenant, queue, payload: {} });
    await service.enqueue({ tenantId: tenant, queue, payload: {}, delayMs: 5_000 });
    // Scheduled jobs signal too: a push-mode worker re-reads the next due
    // time when woken instead of sleeping through it.
    expect(signals.runnable()).toEqual(['emails', 'emails']);
  });

  it('does not signal an idempotent replay (nothing changed)', async () => {
    const signals = new RecordingSignals();
    const service = new JobService(new FakeQueues(), new FakeJobs(), {
      maxPayloadBytes: 64,
      signals,
    });
    await service.enqueue({ tenantId: tenant, queue, payload: {}, idempotencyKey: 'k1' });
    await service.enqueue({ tenantId: tenant, queue, payload: {}, idempotencyKey: 'k1' });
    expect(signals.calls).toHaveLength(1);
  });

  it('does not signal a rejected enqueue', async () => {
    const signals = new RecordingSignals();
    const service = new JobService(new FakeQueues(), new FakeJobs(), {
      maxPayloadBytes: 8,
      signals,
    });
    await expect(
      service.enqueue({ tenantId: tenant, queue, payload: { big: 'x'.repeat(100) } }),
    ).rejects.toBeInstanceOf(PayloadTooLargeError);
    expect(signals.calls).toEqual([]);
  });
});
