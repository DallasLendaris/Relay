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
    expect(applied).toEqual([
      '0002_devices_places.sql',
      '0003_alerts.sql',
      '0004_push_token_unique.sql',
      '0005_alert_cells.sql',
      '0006_push_targets.sql',
      '0007_push_token_lookup_index.sql',
      '0008_push_token_moves.sql',
      '0009_cell_index_outbox.sql',
      '0010_minimize_places_tombstones.sql',
      '0011_roles.sql',
      '0012_push_project.sql',
      '0013_strip_house_numbers.sql',
    ]);
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

  it('0013 strips a leading house number from short_address, and twice is the same as once', async () => {
    const { readFile } = await import('node:fs/promises');
    const exec = new PgliteExecutor(new PGlite());
    await runMigrations(exec, MIGRATIONS_DIR, {
      skip: (f) => f.startsWith('0001_') || f.startsWith('0013_'),
    });
    await exec.query(
      "INSERT INTO devices (id, credential_hash, language, created_at) VALUES ('11111111-1111-4111-8111-111111111111', 'h', 'en', now())",
      [],
    );
    const rows: Array<[string, string | null]> = [
      ['1600 Amphitheatre Pkwy, Mountain View', 'Amphitheatre Pkwy, Mountain View'],
      ['12-B Main St, San Jose', 'Main St, San Jose'],
      ['221B Baker St, Oakland', 'Baker St, Oakland'],
      ['12-14 Elm St, Fresno', 'Elm St, Fresno'],
      ['1st St, San Jose', '1st St, San Jose'],
      ['5th Ave, San Francisco', '5th Ave, San Francisco'],
      ['Main St, San Jose', 'Main St, San Jose'],
      ['4176 Bascom Ave, Santa Clara', 'Bascom Ave, Santa Clara'],
      ['', null],
    ];
    for (const [i, [before]] of rows.entries()) {
      await exec.query(
        "INSERT INTO places (id, device_id, label, short_address, cell_center_lat, cell_center_lng, h3_cell, created_at) VALUES ($1, '11111111-1111-4111-8111-111111111111', 'x', $2, 37, -121, 'c', now())",
        [`00000000-0000-4000-8000-00000000000${i}`, before],
      );
    }
    const sql = await readFile(`${MIGRATIONS_DIR}/0013_strip_house_numbers.sql`, 'utf8');
    const apply = async () => {
      for (const stmt of splitStatements(sql)) await exec.query(stmt, []);
      return (
        await exec.query<{ short_address: string | null }>(
          'SELECT short_address FROM places ORDER BY id',
          [],
        )
      ).map((r) => r.short_address);
    };
    const expected = rows.map(([, after]) => after);
    expect(await apply()).toEqual(expected);
    expect(await apply()).toEqual(expected);
  });
});
