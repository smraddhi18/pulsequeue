import { mapDatabaseError } from './errors.js';
import type { Queryable, QueryResultRow } from './pool.js';

/**
 * Run a parameterized query and translate failures into domain errors.
 * Repositories go through this instead of calling db.query directly.
 */
export async function runQuery<R extends QueryResultRow>(
  db: Queryable,
  text: string,
  values: unknown[] = [],
): Promise<R[]> {
  try {
    const result = await db.query<R>(text, values);
    return result.rows;
  } catch (err) {
    throw mapDatabaseError(err);
  }
}
