import { describe, expect, it } from 'vitest';
import { NotFoundError } from '../src/domain/errors.js';
import type { Queue, QueueName } from '../src/domain/job.js';
import type { QueueSettings } from '../src/domain/queue.js';
import type { TenantId } from '../src/domain/tenant.js';
import type { UpdateQueueSettingsParams } from '../src/repositories/queue.repository.js';
import { QueueService, type QueueSettingsStore } from '../src/services/queue.service.js';

const acme = 'acme' as TenantId;
const at = new Date('2026-10-26T10:00:00Z');

class FakeQueues implements QueueSettingsStore {
  readonly rows = new Map<string, Queue & Omit<QueueSettings, 'queue'>>();

  async ensure(tenantId: TenantId, name: QueueName): Promise<Queue> {
    const key = `${tenantId}:${name}`;
    let row = this.rows.get(key);
    if (!row) {
      row = {
        id: key,
        tenantId,
        name,
        createdAt: at,
        maxConcurrency: null,
        rateLimit: null,
        updatedAt: at,
      };
      this.rows.set(key, row);
    }
    return row;
  }

  async findSettings(tenantId: TenantId, name: QueueName): Promise<QueueSettings | null> {
    const row = this.rows.get(`${tenantId}:${name}`);
    return row ? this.toSettings(row) : null;
  }

  async updateSettings(params: UpdateQueueSettingsParams): Promise<QueueSettings> {
    const row = this.rows.get(params.queueId)!;
    row.maxConcurrency = params.maxConcurrency;
    row.rateLimit = params.rateLimit;
    return this.toSettings(row);
  }

  private toSettings(row: Queue & Omit<QueueSettings, 'queue'>): QueueSettings {
    return {
      queue: row.name,
      maxConcurrency: row.maxConcurrency,
      rateLimit: row.rateLimit,
      updatedAt: row.updatedAt,
    };
  }
}

describe('QueueService', () => {
  it('404s settings of a queue the tenant does not own', async () => {
    const queues = new FakeQueues();
    await queues.ensure('globex' as TenantId, 'emails' as QueueName);
    const service = new QueueService(queues);
    await expect(service.getSettings(acme, 'emails' as QueueName)).rejects.toBeInstanceOf(
      NotFoundError,
    );
  });

  it('creates the queue on update and replaces both limits', async () => {
    const queues = new FakeQueues();
    const service = new QueueService(queues);
    const queue = 'emails' as QueueName;
    const set = await service.updateSettings({
      tenantId: acme,
      queue,
      maxConcurrency: 2,
      rateLimit: { perSec: 5, burst: 10 },
    });
    expect(set).toMatchObject({ queue, maxConcurrency: 2, rateLimit: { perSec: 5, burst: 10 } });
    const cleared = await service.updateSettings({
      tenantId: acme,
      queue,
      maxConcurrency: null,
      rateLimit: null,
    });
    expect(cleared).toMatchObject({ maxConcurrency: null, rateLimit: null });
    expect(await service.getSettings(acme, queue)).toEqual(cleared);
    expect(queues.rows.size).toBe(1);
  });

  it('notifies onSettingsChanged after a successful update only', async () => {
    const seen: string[] = [];
    const service = new QueueService(new FakeQueues(), {
      onSettingsChanged: (settings, tenantId) => seen.push(`${tenantId}/${settings.queue}`),
    });
    await service.updateSettings({
      tenantId: acme,
      queue: 'emails' as QueueName,
      maxConcurrency: 3,
      rateLimit: null,
    });
    expect(seen).toEqual(['acme/emails']);
    await expect(service.getSettings(acme, 'nope' as QueueName)).rejects.toBeInstanceOf(
      NotFoundError,
    );
    expect(seen).toHaveLength(1);
  });
});
