import { cp, mkdtemp, readdir, rm, writeFile, readFile } from 'node:fs/promises';
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
      'api_keys',
      'job_attempts',
      'jobs',
      'queues',
      'schedules',
      'schema_migrations',
      'tenants',
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

  it('upgrades an M5 database: existing queues and jobs move to the default tenant', async () => {
    const db = await freshDb();
    const dir = await mkdtemp(path.join(tmpdir(), 'pq-mig-0006-'));
    tempDirs.push(dir);
    const files = (await readdir(DEFAULT_MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql')).sort();
    for (const f of files.filter((f) => f < '0006')) {
      await cp(path.join(DEFAULT_MIGRATIONS_DIR, f), path.join(dir, f));
    }
    await runMigrations(db.pool, { dir });
    const { rows: queues } = await db.pool.query<{ id: string }>(
      "INSERT INTO queues (name) VALUES ('legacy') RETURNING id",
    );
    await db.pool.query("INSERT INTO jobs (queue_id, payload) VALUES ($1, '{}')", [queues[0]!.id]);

    const result = await runMigrations(db.pool);
    expect(result.applied[0]).toMatch(/^0006_/);
    const { rows } = await db.pool.query<{ tenant_id: string; jobs: string }>(
      `SELECT q.tenant_id, (SELECT count(*) FROM jobs j WHERE j.queue_id = q.id)::text AS jobs
       FROM queues q WHERE q.name = 'legacy'`,
    );
    expect(rows).toEqual([{ tenant_id: 'default', jobs: '1' }]);
  });

  it('upgrades an M6 database: 0008 backfills job_attempts.queue_id, leaves ready_at unknown', async () => {
    const db = await freshDb();
    const dir = await mkdtemp(path.join(tmpdir(), 'pq-mig-0008-'));
    tempDirs.push(dir);
    const files = (await readdir(DEFAULT_MIGRATIONS_DIR)).filter((f) => f.endsWith('.sql')).sort();
    for (const f of files.filter((f) => f < '0008')) {
      await cp(path.join(DEFAULT_MIGRATIONS_DIR, f), path.join(dir, f));
    }
    await runMigrations(db.pool, { dir });
    const { rows: queues } = await db.pool.query<{ id: string }>(
      "INSERT INTO queues (name) VALUES ('history') RETURNING id",
    );
    const { rows: jobs } = await db.pool.query<{ id: string }>(
      "INSERT INTO jobs (queue_id, payload, status, attempts) VALUES ($1, '{}', 'succeeded', 1) RETURNING id",
      [queues[0]!.id],
    );
    await db.pool.query(
      `INSERT INTO job_attempts (job_id, attempt_number, status, started_at, finished_at)
       VALUES ($1, 1, 'succeeded', now() - interval '2 seconds', now())`,
      [jobs[0]!.id],
    );

    const result = await runMigrations(db.pool);
    expect(result.applied).toHaveLength(1);
    expect(result.applied[0]).toMatch(/^0008_/);
    const { rows } = await db.pool.query<{ queue_id: string; ready_at: Date | null }>(
      'SELECT queue_id::text, ready_at FROM job_attempts',
    );
    expect(rows).toEqual([{ queue_id: queues[0]!.id, ready_at: null }]);
    const { rows: traces } = await db.pool.query<{ traceparent: string | null }>(
      'SELECT traceparent FROM jobs',
    );
    expect(traces).toEqual([{ traceparent: null }]);
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

  describe('tenants and API keys (0005)', () => {
    const hash = Buffer.alloc(32, 7);

    it('seeds the default tenant and validates tenant ids and rate-limit pairs', async () => {
      const { rows } = await db.pool.query<{ id: string }>(
        "SELECT id FROM tenants WHERE id = 'default'",
      );
      expect(rows).toHaveLength(1);
      await expect(
        db.pool.query("INSERT INTO tenants (id, name) VALUES ('Bad:Id', 'x')"),
      ).rejects.toMatchObject({ code: '23514', constraint: 'tenants_id_format' });
      await expect(
        db.pool.query(
          "INSERT INTO tenants (id, name, rate_limit_per_sec) VALUES ('half-limit', 'x', 10)",
        ),
      ).rejects.toMatchObject({ code: '23514', constraint: 'tenants_rate_limit_positive' });
    });

    it('stores 32-byte hashes under unique, well-formed prefixes', async () => {
      const insert = (prefix: string, secretHash: Buffer) =>
        db.pool.query(
          `INSERT INTO api_keys (tenant_id, name, prefix, secret_hash, role)
           VALUES ('default', 'k', $1, $2, 'reader')`,
          [prefix, secretHash],
        );
      await insert('abcdefghij01', hash);
      await expect(insert('abcdefghij01', hash)).rejects.toMatchObject({
        code: '23505',
        constraint: 'api_keys_prefix_uq',
      });
      await expect(insert('ABCDEFGHIJ02', hash)).rejects.toMatchObject({
        constraint: 'api_keys_prefix_format',
      });
      await expect(insert('abcdefghij03', Buffer.alloc(16))).rejects.toMatchObject({
        constraint: 'api_keys_secret_hash_sha256',
      });
    });
  });

  describe('queue tenancy (0006)', () => {
    it('scopes queue names per tenant and defaults to the default tenant', async () => {
      await db.pool.query("INSERT INTO tenants (id, name) VALUES ('t-0006', 'x')");
      const { rows } = await db.pool.query<{ tenant_id: string }>(
        "INSERT INTO queues (name) VALUES ('shared-0006') RETURNING tenant_id",
      );
      expect(rows[0]!.tenant_id).toBe('default');
      await db.pool.query("INSERT INTO queues (tenant_id, name) VALUES ('t-0006', 'shared-0006')");
      await expect(
        db.pool.query("INSERT INTO queues (tenant_id, name) VALUES ('t-0006', 'shared-0006')"),
      ).rejects.toMatchObject({ code: '23505', constraint: 'queues_tenant_name_uq' });
      await expect(
        db.pool.query("INSERT INTO queues (tenant_id, name) VALUES ('nobody', 'q')"),
      ).rejects.toMatchObject({ code: '23503' });
      // A tenant that still owns queues cannot be deleted.
      await expect(db.pool.query("DELETE FROM tenants WHERE id = 't-0006'")).rejects.toMatchObject({
        code: '23503',
      });
    });
  });

  describe('queue limits (0007)', () => {
    it('defaults to unlimited and rejects non-positive or half-set limits', async () => {
      const { rows } = await db.pool.query<Record<string, unknown>>(
        `INSERT INTO queues (name) VALUES ('limits-0007')
         RETURNING max_concurrency, rate_limit_per_sec, rate_limit_burst`,
      );
      expect(rows[0]).toEqual({
        max_concurrency: null,
        rate_limit_per_sec: null,
        rate_limit_burst: null,
      });
      const update = (set: string) =>
        db.pool.query(`UPDATE queues SET ${set} WHERE name = 'limits-0007'`);
      await update('max_concurrency = 3, rate_limit_per_sec = 5, rate_limit_burst = 10');
      await expect(update('max_concurrency = 0')).rejects.toMatchObject({
        code: '23514',
        constraint: 'queues_max_concurrency_positive',
      });
      await expect(update('rate_limit_burst = NULL')).rejects.toMatchObject({
        code: '23514',
        constraint: 'queues_rate_limit_positive',
      });
      await expect(update('rate_limit_per_sec = -1')).rejects.toMatchObject({
        code: '23514',
        constraint: 'queues_rate_limit_positive',
      });
      await update('max_concurrency = NULL, rate_limit_per_sec = NULL, rate_limit_burst = NULL');
    });
  });

  describe('observability (0008)', () => {
    it('accepts well-formed W3C traceparents only', async () => {
      await insertJob({ traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' });
      await insertJob({ traceparent: null });
      for (const bad of [
        'garbage',
        '00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01',
        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7',
        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01; drop',
      ]) {
        await expect(insertJob({ traceparent: bad })).rejects.toMatchObject({
          code: '23514',
          constraint: 'jobs_traceparent_format',
        });
      }
    });
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

    it('dashboard attempt scans (M7) are index-only on job_attempts_queue_finished_idx', async () => {
      await db.pool.query('VACUUM ANALYZE job_attempts');
      const text = await plan(
        `SELECT count(*) FILTER (WHERE status = 'succeeded'),
                percentile_cont(0.5) WITHIN GROUP (ORDER BY started_at - ready_at)
         FROM job_attempts
         WHERE queue_id = $1 AND finished_at IS NOT NULL
           AND finished_at >= now() - interval '1 hour' AND finished_at < now()`,
        [queueId],
      );
      expect(text).toContain('Index Only Scan using job_attempts_queue_finished_idx');
    });

    it('running count per queue (M6 concurrency cap) uses jobs_running_queue_idx', async () => {
      const text = await plan(
        `SELECT count(*) FROM jobs WHERE queue_id = $1 AND status = 'running'`,
        [1],
      );
      expect(text).toContain('jobs_running_queue_idx');
    });

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

    it('lease query (M4) walks jobs_ready_idx (queued rows only) without a sort', async () => {
      const text = await plan(
        `SELECT id FROM jobs
         WHERE queue_id = $1 AND status = 'queued' AND run_at <= now()
         ORDER BY priority DESC, run_at, id
         LIMIT 10
         FOR UPDATE SKIP LOCKED`,
        [queueId],
      );
      expect(text).toContain('jobs_ready_idx');
      expect(text).not.toMatch(/\bSort\b/);
    });

    it('promotion (M4) range-scans jobs_scheduled_due_idx by due time without a sort', async () => {
      const text = await plan(
        `SELECT id FROM jobs
         WHERE queue_id = $1 AND status = 'scheduled' AND run_at <= now()
         ORDER BY run_at, id
         LIMIT 500
         FOR UPDATE SKIP LOCKED`,
        [queueId],
      );
      expect(text).toContain('jobs_scheduled_due_idx');
      expect(text).toMatch(/Index Cond: .*run_at <= now\(\)/);
      expect(text).not.toMatch(/\bSort\b/);
    });

    it('scheduler claim (M4) walks schedules_due_idx in due order without a sort', async () => {
      const text = await plan(
        `SELECT id FROM schedules
         WHERE enabled AND next_run_at <= now()
         ORDER BY next_run_at, id
         LIMIT 100
         FOR UPDATE SKIP LOCKED`,
        [],
      );
      expect(text).toContain('schedules_due_idx');
    });

    it('schedule keyset page query uses schedules_queue_created_idx without a sort', async () => {
      const text = await plan(
        `SELECT id FROM schedules
         WHERE queue_id = $1 AND (created_at, id) < ($2::timestamptz, $3::uuid)
         ORDER BY created_at DESC, id DESC
         LIMIT 50`,
        [queueId, new Date().toISOString(), '00000000-0000-0000-0000-000000000000'],
      );
      expect(text).toContain('schedules_queue_created_idx');
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
