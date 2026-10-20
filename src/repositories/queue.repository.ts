import type { Queue, QueueName } from '../domain/job.js';
import { runQuery } from '../db/query.js';
import type { Queryable } from '../db/pool.js';

interface QueueRow {
  id: string; // bigint comes back as a string from pg
  name: string;
  created_at: Date;
}

function toQueue(row: QueueRow): Queue {
  return { id: row.id, name: row.name as QueueName, createdAt: row.created_at };
}

export class QueueRepository {
  constructor(private readonly db: Queryable) {}

  async findByName(name: QueueName): Promise<Queue | null> {
    const rows = await runQuery<QueueRow>(
      this.db,
      'SELECT id, name, created_at FROM queues WHERE name = $1',
      [name],
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
  async ensure(name: QueueName): Promise<Queue> {
    const existing = await this.findByName(name);
    if (existing) return existing;

    const inserted = await runQuery<QueueRow>(
      this.db,
      `INSERT INTO queues (name) VALUES ($1)
       ON CONFLICT (name) DO NOTHING
       RETURNING id, name, created_at`,
      [name],
    );
    if (inserted[0]) return toQueue(inserted[0]);

    const winner = await this.findByName(name);
    if (!winner) {
      // Only possible if the queue was deleted between the two statements.
      throw new Error(`Queue "${name}" vanished during get-or-create`);
    }
    return winner;
  }
}
