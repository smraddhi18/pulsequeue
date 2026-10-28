import { Counter, Gauge, type Registry } from 'prom-client';
import type { Pool } from '../db/index.js';
import type { StructuredLogger } from '../lib/logger.js';
import { SingleFlight } from '../lib/single-flight.js';
import { METRIC_PREFIX } from './registry.js';

/** Statuses the depth gauge reports. succeeded is history, not depth. */
export const DEPTH_STATUSES = ['queued', 'scheduled', 'running', 'dead'] as const;

export interface QueueDepthRow {
  tenantId: string;
  queue: string;
  status: (typeof DEPTH_STATUSES)[number];
  count: number;
}

export interface QueueDepthOptions {
  pool: Pool;
  registry: Registry;
  logger: StructuredLogger;
  /** Queues included per scrape (the rest are not exported; the dashboard API has them). */
  maxQueues: number;
  /** Reuse a snapshot younger than this (0 = query on every scrape). */
  cacheMs: number;
  /** statement_timeout for the depth query. */
  timeoutMs: number;
  now?: () => number;
}

/*
 * One row per (queue, status), zeros included so a drained queue reads 0
 * rather than disappearing. Cost is bounded twice: by the queue LIMIT, and
 * per queue by an index-only count on jobs_queue_status_created_idx
 * (queue_id, status, ...), which touches only rows in that status, never
 * the succeeded history.
 */
const DEPTH_SQL = `
  SELECT q.tenant_id, q.name AS queue, st.status::text AS status, c.n::int AS count
  FROM (SELECT id, tenant_id, name FROM queues ORDER BY id LIMIT $1) q
  CROSS JOIN unnest($2::job_status[]) AS st(status)
  CROSS JOIN LATERAL (
    SELECT count(*) AS n FROM jobs j WHERE j.queue_id = q.id AND j.status = st.status
  ) c`;

interface DepthRow {
  tenant_id: string;
  queue: string;
  status: QueueDepthRow['status'];
  count: number;
}

/**
 * pulsequeue_queue_depth, read lazily from Postgres when /metrics is scraped
 * (API process only; one exporter is enough because the source is the
 * database, not process memory).
 *
 * Scrape cost is bounded: a snapshot is reused for `cacheMs` (N scrapers or
 * N Prometheus replicas cost one query per interval), concurrent scrapes
 * share one query (single-flight), and the query runs with its own
 * statement_timeout in a read-only transaction. If the database is slow or
 * down the scrape still succeeds with the last snapshot, and
 * pulsequeue_queue_depth_collect_errors_total says why the numbers are stale.
 */
export class QueueDepthCollector {
  private snapshot: QueueDepthRow[] = [];
  private snapshotAt = Number.NEGATIVE_INFINITY;
  private readonly flight = new SingleFlight<QueueDepthRow[]>();
  private readonly errors: Counter;

  constructor(private readonly options: QueueDepthOptions) {
    const read = (): Promise<QueueDepthRow[]> => this.read();
    new Gauge({
      name: `${METRIC_PREFIX}queue_depth`,
      help: 'Jobs per queue and status, read from Postgres at scrape time (cached briefly)',
      labelNames: ['tenant', 'queue', 'status'],
      registers: [options.registry],
      async collect() {
        const rows = await read();
        this.reset();
        for (const row of rows) {
          this.set({ tenant: row.tenantId, queue: row.queue, status: row.status }, row.count);
        }
      },
    });
    this.errors = new Counter({
      name: `${METRIC_PREFIX}queue_depth_collect_errors_total`,
      help: 'Failed queue-depth queries (the gauge then serves the previous snapshot)',
      registers: [options.registry],
    });
  }

  /** The current snapshot, refreshed when older than cacheMs. Never throws. */
  async read(): Promise<QueueDepthRow[]> {
    const now = this.options.now?.() ?? Date.now();
    if (now - this.snapshotAt < this.options.cacheMs) return this.snapshot;
    try {
      const rows = await this.flight.run('depth', () => this.query());
      this.snapshot = rows;
      this.snapshotAt = this.options.now?.() ?? Date.now();
    } catch (err) {
      this.errors.inc();
      this.options.logger.warn(
        { event: 'metrics.queue_depth_failed', err },
        'queue depth query failed; serving the previous snapshot',
      );
    }
    return this.snapshot;
  }

  private async query(): Promise<QueueDepthRow[]> {
    const client = await this.options.pool.connect();
    let broken: Error | undefined;
    try {
      await client.query('BEGIN READ ONLY');
      // SET LOCAL takes no bind parameters; the value is a validated integer.
      await client.query(`SET LOCAL statement_timeout = ${Math.trunc(this.options.timeoutMs)}`);
      const result = await client.query<DepthRow>(DEPTH_SQL, [
        this.options.maxQueues,
        DEPTH_STATUSES,
      ]);
      await client.query('COMMIT');
      return result.rows.map((r) => ({
        tenantId: r.tenant_id,
        queue: r.queue,
        status: r.status,
        count: r.count,
      }));
    } catch (err) {
      await client.query('ROLLBACK').catch((rollbackErr: unknown) => {
        // A connection that cannot even roll back is not returned to the pool.
        broken = rollbackErr instanceof Error ? rollbackErr : new Error(String(rollbackErr));
      });
      throw err;
    } finally {
      client.release(broken);
    }
  }
}
