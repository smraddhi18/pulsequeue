import { createHash } from 'node:crypto';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Logger } from '../lib/logger.js';
import type { Pool, PoolClient } from './pool.js';

/**
 * Minimal forward-only SQL migration runner.
 *
 * - Migrations are plain `.sql` files named `NNNN_snake_case.sql`, applied in
 *   version order and recorded in `schema_migrations` (version, name, checksum).
 * - Each file runs in its own transaction together with its bookkeeping row,
 *   so a failed migration leaves no partial schema and no record.
 * - A session-level advisory lock serializes concurrent runners (several
 *   replicas or a migrate job racing a deploy); the loser waits, then finds
 *   nothing pending.
 * - The checksum of every applied file is verified on each run: editing an
 *   applied migration is an error; write a new migration instead.
 * - A file whose first line is `-- pulsequeue:no-transaction` runs outside a
 *   transaction (needed for `CREATE INDEX CONCURRENTLY`); keep such files to a
 *   single statement.
 */

/** Resolves to <repo>/migrations from both src/db (tsx) and dist/db (node). */
export const DEFAULT_MIGRATIONS_DIR = fileURLToPath(new URL('../../migrations/', import.meta.url));

const FILENAME_PATTERN = /^(\d{4})_([a-z0-9_]+)\.sql$/;
const NO_TRANSACTION_DIRECTIVE = '-- pulsequeue:no-transaction';

export interface Migration {
  version: string;
  name: string;
  filename: string;
  sql: string;
  checksum: string;
  transactional: boolean;
}

export interface MigrationResult {
  applied: string[];
  alreadyApplied: number;
}

export interface RunMigrationsOptions {
  dir?: string;
  logger?: Pick<Logger, 'info' | 'warn'>;
  /** lock_timeout for DDL so a migration fails fast instead of queueing behind long transactions. */
  lockTimeoutMs?: number;
}

export class MigrationError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'MigrationError';
  }
}

/** Line endings are normalized so a Windows checkout does not change checksums. */
export function migrationChecksum(sql: string): string {
  return createHash('sha256').update(sql.replace(/\r\n/g, '\n'), 'utf8').digest('hex');
}

export function parseMigrationFilename(filename: string): { version: string; name: string } | null {
  const match = FILENAME_PATTERN.exec(filename);
  if (!match?.[1] || !match[2]) return null;
  return { version: match[1], name: match[2] };
}

export async function loadMigrations(dir: string = DEFAULT_MIGRATIONS_DIR): Promise<Migration[]> {
  const entries = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  const seen = new Map<string, string>();
  const migrations: Migration[] = [];

  for (const filename of entries) {
    const parsed = parseMigrationFilename(filename);
    if (!parsed) {
      throw new MigrationError(
        `Invalid migration filename "${filename}" (expected NNNN_snake_case.sql)`,
      );
    }
    const duplicate = seen.get(parsed.version);
    if (duplicate) {
      throw new MigrationError(
        `Duplicate migration version ${parsed.version}: ${duplicate}, ${filename}`,
      );
    }
    seen.set(parsed.version, filename);

    const sql = await readFile(path.join(dir, filename), 'utf8');
    migrations.push({
      ...parsed,
      filename,
      sql,
      checksum: migrationChecksum(sql),
      transactional: !sql.trimStart().startsWith(NO_TRANSACTION_DIRECTIVE),
    });
  }
  return migrations;
}

interface AppliedRow {
  version: string;
  checksum: string;
}

export async function runMigrations(
  pool: Pool,
  options: RunMigrationsOptions = {},
): Promise<MigrationResult> {
  const { logger, lockTimeoutMs = 10_000 } = options;
  const migrations = await loadMigrations(options.dir);
  const client = await pool.connect();
  let clientBroken = false;

  try {
    // Migrations may legitimately run longer than the API's statement_timeout.
    await client.query('SET statement_timeout = 0');
    // Scoped to the current schema so isolated test schemas do not serialize.
    await client.query(
      "SELECT pg_advisory_lock(hashtext('pulsequeue.migrations:' || current_schema()))",
    );
    await client.query(`SET lock_timeout = ${Math.trunc(lockTimeoutMs)}`);

    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version     text        PRIMARY KEY,
          name        text        NOT NULL,
          checksum    text        NOT NULL,
          applied_at  timestamptz NOT NULL DEFAULT now()
        )`);

      const { rows } = await client.query<AppliedRow>(
        'SELECT version, checksum FROM schema_migrations ORDER BY version',
      );
      const applied = new Map(rows.map((r) => [r.version, r.checksum]));
      verifyApplied(migrations, applied, logger);

      const pending = migrations.filter((m) => !applied.has(m.version));
      for (const migration of pending) {
        await applyMigration(client, migration);
        logger?.info({ version: migration.version, name: migration.name }, 'Applied migration');
      }

      return {
        applied: pending.map((m) => m.filename),
        alreadyApplied: migrations.length - pending.length,
      };
    } finally {
      await client
        .query("SELECT pg_advisory_unlock(hashtext('pulsequeue.migrations:' || current_schema()))")
        .catch(() => {
          clientBroken = true;
        });
    }
  } catch (err) {
    clientBroken = true;
    throw err;
  } finally {
    // A broken session (or one that may still hold the lock) must not go back to the pool.
    client.release(clientBroken);
  }
}

function verifyApplied(
  migrations: Migration[],
  applied: Map<string, string>,
  logger: RunMigrationsOptions['logger'],
): void {
  const byVersion = new Map(migrations.map((m) => [m.version, m]));
  const modified: string[] = [];

  for (const [version, checksum] of applied) {
    const local = byVersion.get(version);
    if (!local) {
      // Newer code may have applied migrations this build does not know about (rolling deploy).
      logger?.warn({ version }, 'Applied migration not found locally');
      continue;
    }
    if (local.checksum !== checksum) modified.push(local.filename);
  }

  if (modified.length > 0) {
    throw new MigrationError(
      `Applied migrations were modified after being applied: ${modified.join(', ')}. ` +
        'Add a new migration instead of editing an applied one.',
    );
  }
}

async function applyMigration(client: PoolClient, migration: Migration): Promise<void> {
  const record = () =>
    client.query('INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)', [
      migration.version,
      migration.name,
      migration.checksum,
    ]);

  if (!migration.transactional) {
    try {
      await client.query(migration.sql);
      await record();
    } catch (err) {
      throw new MigrationError(`Migration ${migration.filename} failed`, { cause: err });
    }
    return;
  }

  try {
    await client.query('BEGIN');
    // No bind parameters -> simple query protocol, so a file may hold many statements.
    await client.query(migration.sql);
    await record();
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw new MigrationError(`Migration ${migration.filename} failed`, { cause: err });
  }
}
