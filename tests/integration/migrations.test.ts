import { cp, mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { DEFAULT_MIGRATIONS_DIR, MigrationError, runMigrations } from '../../src/db/migrate.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

const INITIAL = '0001_create_queues_jobs_attempts.sql';

describeWithDb('migration runner (Postgres)', () => {
  const databases: TestDatabase[] = [];
  const tempDirs: string[] = [];

  async function freshDb(): Promise<TestDatabase> {
    const db = await createTestDatabase({ migrate: false });
    databases.push(db);
    return db;
  }

  async function tempMigrationsDir(): Promise<string> {
    const dir = await mkdtemp(path.join(tmpdir(), 'pq-mig-int-'));
    tempDirs.push(dir);
    await cp(path.join(DEFAULT_MIGRATIONS_DIR, INITIAL), path.join(dir, INITIAL));
    return dir;
  }

  afterEach(async () => {
    await Promise.all(databases.splice(0).map((db) => db.destroy()));
    await Promise.all(tempDirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
  });

  it('applies pending migrations once and records version + checksum', async () => {
    const db = await freshDb();

    const first = await runMigrations(db.pool);
    expect(first.applied).toContain(INITIAL);
    expect(first.alreadyApplied).toBe(0);

    const second = await runMigrations(db.pool);
    expect(second.applied).toEqual([]);
    expect(second.alreadyApplied).toBe(first.applied.length);

    const { rows } = await db.pool.query<{ version: string; checksum: string }>(
      'SELECT version, checksum FROM schema_migrations ORDER BY version',
    );
    expect(rows[0]?.version).toBe('0001');
    expect(rows[0]?.checksum).toMatch(/^[0-9a-f]{64}$/);

    const { rows: tables } = await db.pool.query<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = current_schema() ORDER BY table_name`,
    );
    expect(tables.map((t) => t.table_name)).toEqual([
      'job_attempts',
      'jobs',
      'queues',
      'schema_migrations',
    ]);
  });

  it('serializes concurrent runners with an advisory lock (each migration applied exactly once)', async () => {
    const db = await freshDb();
    const results = await Promise.all(Array.from({ length: 5 }, () => runMigrations(db.pool)));

    const appliedCounts = results.map((r) => r.applied.length);
    expect(appliedCounts.filter((n) => n > 0)).toHaveLength(1);
    const { rows } = await db.pool.query<{ n: string }>(
      'SELECT count(*) AS n FROM schema_migrations',
    );
    expect(Number(rows[0]?.n)).toBe(results.find((r) => r.applied.length > 0)?.applied.length);
  });

  it('refuses to run when an applied migration was edited', async () => {
    const db = await freshDb();
    const dir = await tempMigrationsDir();
    await runMigrations(db.pool, { dir });

    const file = path.join(dir, INITIAL);
    await writeFile(file, `${await readFile(file, 'utf8')}\n-- sneaky edit\n`);

    await expect(runMigrations(db.pool, { dir })).rejects.toThrow(MigrationError);
    await expect(runMigrations(db.pool, { dir })).rejects.toThrow(/modified after being applied/);
  });

  it('rolls back a failing migration completely (no partial schema, no record)', async () => {
    const db = await freshDb();
    const dir = await tempMigrationsDir();
    await writeFile(
      path.join(dir, '0002_broken.sql'),
      'CREATE TABLE half_done (id int);\nSELECT * FROM table_that_does_not_exist;\n',
    );

    await expect(runMigrations(db.pool, { dir })).rejects.toThrow(/0002_broken.sql failed/);

    const { rows } = await db.pool.query<{ version: string }>(
      'SELECT version FROM schema_migrations ORDER BY version',
    );
    expect(rows.map((r) => r.version)).toEqual(['0001']);
    const { rows: half } = await db.pool.query(
      "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'half_done'",
    );
    expect(half).toHaveLength(0);
  });

  it('runs no-transaction migrations (e.g. CREATE INDEX CONCURRENTLY)', async () => {
    const db = await freshDb();
    const dir = await tempMigrationsDir();
    await writeFile(
      path.join(dir, '0002_concurrent_index.sql'),
      '-- pulsequeue:no-transaction\nCREATE INDEX CONCURRENTLY jobs_finished_at_idx ON jobs (finished_at);\n',
    );

    const result = await runMigrations(db.pool, { dir });
    expect(result.applied).toEqual([INITIAL, '0002_concurrent_index.sql']);
  });
});

describeWithDb('schema invariants (Postgres)', () => {
  let db: TestDatabase;
  let queueId: string;

  beforeAll(async () => {
    db = await createTestDatabase();
    const { rows } = await db.pool.query<{ id: string }>(
      "INSERT INTO queues (name) VALUES ('schema-tests') RETURNING id",
    );
    queueId = rows[0]!.id;
  });

  afterAll(async () => {
    await db.destroy();
  });

  async function insertJob(columns: Record<string, unknown>): Promise<void> {
    const merged: Record<string, unknown> = { queue_id: queueId, payload: '{}', ...columns };
    const keys = Object.keys(merged);
    await db.pool.query(
      `INSERT INTO jobs (${keys.join(', ')}) VALUES (${keys.map((_, i) => `$${i + 1}`).join(', ')})`,
      Object.values(merged),
    );
  }

  it.each([
    ['priority out of range', { priority: 101 }, 'jobs_priority_range'],
    ['max_attempts out of range', { max_attempts: 0 }, 'jobs_max_attempts_range'],
    ['attempts above max_attempts', { attempts: 4, max_attempts: 3 }, 'jobs_attempts_within_max'],
    ['key without fingerprint', { idempotency_key: 'k' }, 'jobs_idempotency_key_has_fingerprint'],
  ])('rejects %s at the database level', async (_label, columns, constraint) => {
    await expect(insertJob(columns)).rejects.toMatchObject({ code: '23514', constraint });
  });

  it('rejects invalid queue names at the database level', async () => {
    await expect(
      db.pool.query("INSERT INTO queues (name) VALUES ('Bad Name')"),
    ).rejects.toMatchObject({
      code: '23514',
      constraint: 'queues_name_format',
    });
  });

  it('enforces one job per (queue, idempotency key) but allows many keyless jobs', async () => {
    await insertJob({ idempotency_key: 'dup', request_fingerprint: 'v1:x' });
    await expect(
      insertJob({ idempotency_key: 'dup', request_fingerprint: 'v1:y' }),
    ).rejects.toMatchObject({ code: '23505', constraint: 'jobs_queue_idempotency_key_uq' });
    await insertJob({});
    await insertJob({});
  });

  it('maintains updated_at via trigger', async () => {
    const { rows } = await db.pool.query<{ id: string; updated_at: Date }>(
      `INSERT INTO jobs (queue_id, payload, created_at, updated_at)
       VALUES ($1, '{}', now() - interval '1 hour', now() - interval '1 hour')
       RETURNING id, updated_at`,
      [queueId],
    );
    const before = rows[0]!;
    const { rows: after } = await db.pool.query<{ updated_at: Date }>(
      'UPDATE jobs SET priority = 5 WHERE id = $1 RETURNING updated_at',
      [before.id],
    );
    expect(after[0]!.updated_at.getTime()).toBeGreaterThan(before.updated_at.getTime());
  });

  describe('query plans', () => {
    async function plan(sql: string, params: unknown[]): Promise<string> {
      const client = await db.pool.connect();
      try {
        // Tiny tables always favour a seq scan; disable it to prove the index *can* serve the query.
        await client.query('SET enable_seqscan = off');
        const { rows } = await client.query<{ 'QUERY PLAN': string }>(`EXPLAIN ${sql}`, params);
        return rows.map((r) => r['QUERY PLAN']).join('\n');
      } finally {
        await client.query('RESET enable_seqscan');
        client.release();
      }
    }

    it('lease query (M2) walks jobs_lease_idx in lease order without a sort', async () => {
      const text = await plan(
        `SELECT id FROM jobs
         WHERE queue_id = $1 AND status IN ('queued', 'scheduled') AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         LIMIT 10
         FOR UPDATE SKIP LOCKED`,
        [queueId],
      );
      expect(text).toContain('jobs_lease_idx');
      expect(text).not.toMatch(/\bSort\b/);
    });

    it('keyset page query uses jobs_queue_created_idx without a sort', async () => {
      const text = await plan(
        `SELECT id FROM jobs
         WHERE queue_id = $1 AND (created_at, id) < ($2::timestamptz, $3::uuid)
         ORDER BY created_at DESC, id DESC
         LIMIT 50`,
        [queueId, new Date().toISOString(), '00000000-0000-0000-0000-000000000000'],
      );
      expect(text).toContain('jobs_queue_created_idx');
      expect(text).not.toMatch(/\bSort\b/);
    });

    it('status-filtered page query uses jobs_queue_status_created_idx without a sort', async () => {
      const text = await plan(
        `SELECT id FROM jobs
         WHERE queue_id = $1 AND status = 'queued' AND (created_at, id) < ($2::timestamptz, $3::uuid)
         ORDER BY created_at DESC, id DESC
         LIMIT 50`,
        [queueId, new Date().toISOString(), '00000000-0000-0000-0000-000000000000'],
      );
      expect(text).toContain('jobs_queue_status_created_idx');
      expect(text).not.toMatch(/\bSort\b/);
    });
  });
});
