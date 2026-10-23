import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import {
  DEFAULT_MIGRATIONS_DIR,
  MigrationError,
  loadMigrations,
  migrationChecksum,
  parseMigrationFilename,
} from '../src/db/migrate.js';

describe('parseMigrationFilename', () => {
  it('parses NNNN_snake_case.sql', () => {
    expect(parseMigrationFilename('0001_create_jobs.sql')).toEqual({
      version: '0001',
      name: 'create_jobs',
    });
  });

  it.each(['1_init.sql', '0001-init.sql', '0001_Init.sql', '0001_init.SQL', 'init.sql'])(
    'rejects %s',
    (name) => {
      expect(parseMigrationFilename(name)).toBeNull();
    },
  );
});

describe('migrationChecksum', () => {
  it('ignores CRLF vs LF so checkouts on Windows do not look modified', () => {
    expect(migrationChecksum('SELECT 1;\r\nSELECT 2;\r\n')).toBe(
      migrationChecksum('SELECT 1;\nSELECT 2;\n'),
    );
  });

  it('changes when content changes', () => {
    expect(migrationChecksum('SELECT 1;')).not.toBe(migrationChecksum('SELECT 2;'));
  });
});

describe('loadMigrations', () => {
  let dir: string;

  beforeEach(async () => {
    dir = await mkdtemp(path.join(tmpdir(), 'pq-migrations-'));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it('returns migrations sorted by version and ignores non-SQL files', async () => {
    await writeFile(path.join(dir, '0002_second.sql'), 'SELECT 2;');
    await writeFile(path.join(dir, '0001_first.sql'), 'SELECT 1;');
    await writeFile(path.join(dir, 'README.md'), '# notes');

    const migrations = await loadMigrations(dir);
    expect(migrations.map((m) => m.filename)).toEqual(['0001_first.sql', '0002_second.sql']);
    expect(migrations[0]?.transactional).toBe(true);
    expect(migrations[0]?.checksum).toBe(migrationChecksum('SELECT 1;'));
  });

  it('detects the no-transaction directive', async () => {
    await writeFile(
      path.join(dir, '0001_concurrent_index.sql'),
      '-- pulsequeue:no-transaction\nCREATE INDEX CONCURRENTLY x ON t (c);',
    );
    const [migration] = await loadMigrations(dir);
    expect(migration?.transactional).toBe(false);
  });

  it('fails on badly named SQL files instead of silently skipping them', async () => {
    await writeFile(path.join(dir, '1_init.sql'), 'SELECT 1;');
    await expect(loadMigrations(dir)).rejects.toBeInstanceOf(MigrationError);
  });

  it('fails on duplicate versions', async () => {
    await writeFile(path.join(dir, '0001_a.sql'), 'SELECT 1;');
    await writeFile(path.join(dir, '0001_b.sql'), 'SELECT 1;');
    await expect(loadMigrations(dir)).rejects.toThrow(/Duplicate migration version 0001/);
  });

  it('loads the repository migrations', async () => {
    const migrations = await loadMigrations(DEFAULT_MIGRATIONS_DIR);
    expect(migrations.length).toBeGreaterThanOrEqual(1);
    expect(migrations[0]?.filename).toBe('0001_create_queues_jobs_attempts.sql');
    expect(migrations.map((m) => m.filename)).toContain('0002_worker_leasing.sql');
    expect(migrations.map((m) => m.filename)).toContain('0003_dead_letter_queue.sql');
    expect(migrations.map((m) => m.filename)).toContain('0004_scheduling.sql');
    expect(migrations.every((m) => m.transactional)).toBe(true);
  });
});
