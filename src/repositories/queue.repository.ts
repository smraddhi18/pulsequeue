import type { Queue, QueueName } from '../domain/job.js';
import type { QueueSettings } from '../domain/queue.js';
import type { TenantId } from '../domain/tenant.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';

interface QueueRow {
  id: string; // bigint comes back as a string from pg
  tenant_id: string;
  name: string;
  created_at: Date;
}

const QUEUE_COLUMNS = 'id, tenant_id, name, created_at';

interface QueueSettingsRow {
  name: string;
  max_concurrency: number | null;
  rate_limit_per_sec: number | null;
  rate_limit_burst: number | null;
  updated_at: Date;
}

const SETTINGS_COLUMNS = 'name, max_concurrency, rate_limit_per_sec, rate_limit_burst, updated_at';

function toSettings(row: QueueSettingsRow): QueueSettings {
  return {
    queue: row.name as QueueName,
    maxConcurrency: row.max_concurrency,
    // The CHECK guarantees both-or-neither.
    rateLimit:
      row.rate_limit_per_sec !== null && row.rate_limit_burst !== null
        ? { perSec: row.rate_limit_per_sec, burst: row.rate_limit_burst }
        : null,
    updatedAt: row.updated_at,
  };
}

/** What PUT /queues/:name/settings stores (full replacement). */
export interface UpdateQueueSettingsParams {
  queueId: string;
  maxConcurrency: number | null;
  rateLimit: { perSec: number; burst: number } | null;
}

function toQueue(row: QueueRow): Queue {
  return {
    id: row.id,
    tenantId: row.tenant_id as TenantId,
    name: row.name as QueueName,
    createdAt: row.created_at,
  };
}

/**
 * Queues are owned by a tenant (M6) and looked up by (tenant, name): the same
 * name in another tenant is a different queue, and a name that only exists in
 * another tenant is "not found".
 */
export class QueueRepository {
  constructor(private readonly db: Queryable) {}

  async findByName(tenantId: TenantId, name: QueueName): Promise<Queue | null> {
    const rows = await runQuery<QueueRow>(
      this.db,
      `SELECT ${QUEUE_COLUMNS} FROM queues WHERE tenant_id = $1 AND name = $2`,
      [tenantId, name],
    );
    return rows[0] ? toQueue(rows[0]) : null;
  }

  /**
   * Get-or-create without a check-then-insert race. The common case (queue
   * exists) is a single read with no write. On a miss, INSERT ... ON CONFLICT
   * DO NOTHING lets exactly one concurrent caller create the row; a loser gets
   * no row back (its statement snapshot predates the winner's commit) and
   * re-reads in a fresh statement, which does see it.
   */
  async ensure(tenantId: TenantId, name: QueueName): Promise<Queue> {
    const existing = await this.findByName(tenantId, name);
    if (existing) return existing;

    const inserted = await runQuery<QueueRow>(
      this.db,
      `INSERT INTO queues (tenant_id, name) VALUES ($1, $2)
       ON CONFLICT (tenant_id, name) DO NOTHING
       RETURNING ${QUEUE_COLUMNS}`,
      [tenantId, name],
    );
    if (inserted[0]) return toQueue(inserted[0]);

    const winner = await this.findByName(tenantId, name);
    if (!winner) {
      // Only possible if the queue was deleted between the two statements.
      throw new Error(`Queue "${name}" vanished during get-or-create`);
    }
    return winner;
  }

  async findSettings(tenantId: TenantId, name: QueueName): Promise<QueueSettings | null> {
    const rows = await runQuery<QueueSettingsRow>(
      this.db,
      `SELECT ${SETTINGS_COLUMNS} FROM queues WHERE tenant_id = $1 AND name = $2`,
      [tenantId, name],
    );
    return rows[0] ? toSettings(rows[0]) : null;
  }

  /** Replace both limits in one statement (the CHECKs reject half-set rate limits). */
  async updateSettings(params: UpdateQueueSettingsParams): Promise<QueueSettings> {
    const rows = await runQuery<QueueSettingsRow>(
      this.db,
      `UPDATE queues
          SET max_concurrency = $2, rate_limit_per_sec = $3, rate_limit_burst = $4
        WHERE id = $1
        RETURNING ${SETTINGS_COLUMNS}`,
      [
        params.queueId,
        params.maxConcurrency,
        params.rateLimit?.perSec ?? null,
        params.rateLimit?.burst ?? null,
      ],
    );
    if (!rows[0]) throw new Error(`Queue ${params.queueId} vanished during a settings update`);
    return toSettings(rows[0]);
  }
}
