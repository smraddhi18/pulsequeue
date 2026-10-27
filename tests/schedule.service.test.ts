import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  ConflictError,
  NotFoundError,
  PayloadTooLargeError,
  ValidationError,
} from '../src/domain/errors.js';
import type { Queue, QueueName } from '../src/domain/job.js';
import type { ScheduleId } from '../src/domain/schedule.js';
import type { TenantId } from '../src/domain/tenant.js';
import { decodeCursor } from '../src/lib/cursor.js';
import type {
  InsertScheduleParams,
  ListSchedulesParams,
  StoredSchedule,
} from '../src/repositories/schedule.repository.js';
import type { QueueStore } from '../src/services/job.service.js';
import { ScheduleService, type ScheduleStore } from '../src/services/schedule.service.js';

const NOW = new Date('2026-10-23T10:15:30.000Z');

class FakeQueues implements QueueStore {
  readonly queues = new Map<string, Queue>();
  async ensure(tenantId: TenantId, name: QueueName): Promise<Queue> {
    const existing = this.queues.get(`${tenantId}:${name}`);
    if (existing) return existing;
    // The id encodes the owner so FakeSchedules can scope lookups like the SQL does.
    const id = `${tenantId}/${this.queues.size + 1}`;
    const queue: Queue = { id, tenantId, name, createdAt: NOW };
    this.queues.set(`${tenantId}:${name}`, queue);
    return queue;
  }
  async findByName(tenantId: TenantId, name: QueueName): Promise<Queue | null> {
    return this.queues.get(`${tenantId}:${name}`) ?? null;
  }
}

/** In-memory ScheduleStore with the repository's guarded semantics. */
class FakeSchedules implements ScheduleStore {
  readonly rows = new Map<string, StoredSchedule>();
  lastInsert: InsertScheduleParams | undefined;
  lastList: ListSchedulesParams | undefined;
  private tick = 0;

  async now(): Promise<Date> {
    return NOW;
  }

  async create(p: InsertScheduleParams): Promise<StoredSchedule> {
    this.lastInsert = p;
    for (const r of this.rows.values()) {
      if (r.queueId === p.queueId && r.name === p.name) {
        throw new ConflictError('Resource already exists', {
          constraint: 'schedules_queue_name_uq',
        });
      }
    }
    const at = new Date(NOW.getTime() + this.tick++);
    const row: StoredSchedule = {
      id: randomUUID() as ScheduleId,
      queueId: p.queueId,
      tenantId: p.queueId.split('/')[0] as TenantId,
      queue: 'q' as QueueName,
      name: p.name,
      cron: p.cronExpression,
      timezone: p.timezone,
      payload: p.payload,
      priority: p.priority,
      maxAttempts: p.maxAttempts,
      misfirePolicy: p.misfirePolicy,
      enabled: p.nextRunAt !== null,
      nextRunAt: p.nextRunAt,
      lastRunAt: null,
      lastJobId: null,
      fireCount: 0,
      lastError: 'stale error',
      createdAt: at,
      updatedAt: at,
      createdAtCursor: at.toISOString().replace('Z', '000Z'),
    };
    this.rows.set(row.id, row);
    return row;
  }

  private owned(tenantId: TenantId, id: string): StoredSchedule | undefined {
    const row = this.rows.get(id);
    return row?.tenantId === tenantId ? row : undefined;
  }

  async findById(tenantId: TenantId, id: string): Promise<StoredSchedule | null> {
    return this.owned(tenantId, id) ?? null;
  }

  async listByQueue(p: ListSchedulesParams): Promise<StoredSchedule[]> {
    this.lastList = p;
    return [...this.rows.values()].filter((r) => r.queueId === p.queueId).slice(0, p.limit);
  }

  async pause(tenantId: TenantId, id: string): Promise<StoredSchedule | null> {
    const row = this.owned(tenantId, id);
    if (!row?.enabled) return null;
    Object.assign(row, { enabled: false, nextRunAt: null });
    return row;
  }

  async resume(tenantId: TenantId, id: string, nextRunAt: Date): Promise<StoredSchedule | null> {
    const row = this.owned(tenantId, id);
    if (!row || row.enabled) return null;
    Object.assign(row, { enabled: true, nextRunAt, lastError: null });
    return row;
  }

  async delete(tenantId: TenantId, id: string): Promise<boolean> {
    return this.owned(tenantId, id) !== undefined && this.rows.delete(id);
  }
}

const queue = 'reports' as QueueName;
const tenant = 'acme' as TenantId;
const base = { tenantId: tenant, queue, name: 'nightly', cron: '0 2 * * *', payload: { r: 1 } };

describe('ScheduleService', () => {
  let queues: FakeQueues;
  let store: FakeSchedules;
  let service: ScheduleService;

  beforeEach(() => {
    queues = new FakeQueues();
    store = new FakeSchedules();
    service = new ScheduleService(queues, store, { maxPayloadBytes: 64 });
  });

  describe('create', () => {
    it('applies defaults and computes the first occurrence from the database clock', async () => {
      const s = await service.create(base);
      expect(store.lastInsert).toMatchObject({
        cronExpression: '0 2 * * *',
        timezone: 'UTC',
        priority: 0,
        maxAttempts: 3,
        misfirePolicy: 'fire_once',
        nextRunAt: new Date('2026-10-24T02:00:00.000Z'),
      });
      expect(s.enabled).toBe(true);
      expect(s).not.toHaveProperty('queueId');
      expect(s).not.toHaveProperty('createdAtCursor');
    });

    it('evaluates the expression in the schedule time zone', async () => {
      await service.create({
        ...base,
        cron: '0 18 * * *',
        timezone: 'Asia/Kolkata',
      });
      // 18:00 IST on 2026-10-23 = 12:30 UTC, still ahead of NOW (10:15 UTC).
      expect(store.lastInsert?.nextRunAt).toEqual(new Date('2026-10-23T12:30:00.000Z'));
    });

    it('stores the trimmed expression and creates a disabled schedule with no next run', async () => {
      const s = await service.create({
        ...base,
        cron: '  @hourly ',
        enabled: false,
      });
      expect(store.lastInsert?.cronExpression).toBe('@hourly');
      expect(s).toMatchObject({ enabled: false, nextRunAt: null });
    });

    it('rejects an invalid cron or time zone with a field-level ValidationError', async () => {
      const cronErr = await service.create({ ...base, cron: 'nope' }).catch((e: unknown) => e);
      expect(cronErr).toBeInstanceOf(ValidationError);
      expect((cronErr as ValidationError).details).toMatchObject({
        formErrors: [],
        fieldErrors: { cron: [expect.any(String)] },
      });
      const tzErr = await service.create({ ...base, timezone: 'X/Y' }).catch((e: unknown) => e);
      expect((tzErr as ValidationError).details).toMatchObject({
        fieldErrors: { timezone: [expect.any(String)] },
      });
      expect(queues.queues.size).toBe(0);
    });

    it('enforces the payload budget before touching storage', async () => {
      await expect(
        service.create({ ...base, payload: { text: 'x'.repeat(100) } }),
      ).rejects.toBeInstanceOf(PayloadTooLargeError);
      expect(store.rows.size).toBe(0);
    });

    it('turns a duplicate name into a descriptive 409', async () => {
      await service.create(base);
      const err = await service.create(base).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(ConflictError);
      expect((err as ConflictError).message).toMatch(/"nightly" already exists in queue "reports"/);
    });
  });

  describe('pause / resume / delete', () => {
    it('pause is idempotent and resume restarts from the next occurrence after now', async () => {
      const { id } = await service.create(base);
      expect(await service.pause(tenant, id)).toMatchObject({ enabled: false, nextRunAt: null });
      expect(await service.pause(tenant, id)).toMatchObject({ enabled: false, nextRunAt: null });

      const resumed = await service.resume(tenant, id);
      expect(resumed).toMatchObject({
        enabled: true,
        nextRunAt: new Date('2026-10-24T02:00:00.000Z'),
        lastError: null,
      });
      expect(await service.resume(tenant, id)).toEqual(resumed);
    });

    it('404s for unknown ids', async () => {
      const id = randomUUID();
      await expect(service.get(tenant, id)).rejects.toBeInstanceOf(NotFoundError);
      await expect(service.pause(tenant, id)).rejects.toBeInstanceOf(NotFoundError);
      await expect(service.resume(tenant, id)).rejects.toBeInstanceOf(NotFoundError);
      await expect(service.delete(tenant, id)).rejects.toBeInstanceOf(NotFoundError);
    });

    it('delete removes the schedule', async () => {
      const { id } = await service.create(base);
      await service.delete(tenant, id);
      await expect(service.get(tenant, id)).rejects.toBeInstanceOf(NotFoundError);
    });
  });

  describe('list', () => {
    it('404s for an unknown queue without creating it and rejects bad cursors first', async () => {
      await expect(service.list({ tenantId: tenant, queue, limit: 10 })).rejects.toBeInstanceOf(
        NotFoundError,
      );
      expect(queues.queues.size).toBe(0);
      await expect(
        service.list({ tenantId: tenant, queue, limit: 10, cursor: '!!' }),
      ).rejects.toBeInstanceOf(ValidationError);
    });

    it('asks for limit + 1 and emits a cursor only when another page exists', async () => {
      for (const name of ['a', 'b', 'c']) await service.create({ ...base, name });
      const page = await service.list({ tenantId: tenant, queue, limit: 2, enabled: true });
      expect(store.lastList).toMatchObject({ limit: 3, enabled: true });
      expect(page.schedules).toHaveLength(2);
      expect(page.nextCursor).not.toBeNull();
      expect(decodeCursor(page.nextCursor ?? '').id).toBe(page.schedules[1]?.id);

      const all = await service.list({ tenantId: tenant, queue, limit: 5 });
      expect(all.nextCursor).toBeNull();
    });
  });
});
