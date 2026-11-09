import { describe, expect, it } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { runMigrations, splitStatements } from '../../src/db/sql.js';
import { MIGRATIONS_DIR, PgliteExecutor, freshDb } from './helpers.js';

describe('runMigrations', () => {
  it('applies migrations in order and records them', async () => {
    const exec = new PgliteExecutor(new PGlite());
    const applied = await runMigrations(exec, MIGRATIONS_DIR, {
      skip: (f) => f.startsWith('0001_'),
    });
    expect(applied).toEqual(['0002_devices_places.sql', '0003_alerts.sql']);
    const rows = await exec.query<{ name: string }>(
      'SELECT name FROM schema_migrations ORDER BY name',
      [],
    );
    expect(rows.map((r) => r.name)).toEqual(applied);
  });

  it('is idempotent: a second run applies nothing', async () => {
    const exec = await freshDb();
    expect(
      await runMigrations(exec, MIGRATIONS_DIR, { skip: (f) => f.startsWith('0001_') }),
    ).toEqual([]);
  });

  it('keeps the PostGIS extension alone in 0001', async () => {
    const { readFile } = await import('node:fs/promises');
    const sql = await readFile(`${MIGRATIONS_DIR}/0001_extensions.sql`, 'utf8');
    expect(splitStatements(sql)).toEqual(['CREATE EXTENSION IF NOT EXISTS postgis']);
  });

  it('rejects out-of-range coordinates and bad languages', async () => {
    const exec = await freshDb();
    await expect(
      exec.query("INSERT INTO devices VALUES (gen_random_uuid(), 'h', null, 'fr', now())", []),
    ).rejects.toThrow();
    await exec.query(
      "INSERT INTO devices VALUES ('11111111-1111-4111-8111-111111111111', 'h', null, 'en', now())",
      [],
    );
    await expect(
      exec.query(
        "INSERT INTO places VALUES (gen_random_uuid(), '11111111-1111-4111-8111-111111111111', 'x', 'y', 91, 0, 'c', now())",
        [],
      ),
    ).rejects.toThrow();
  });
});
