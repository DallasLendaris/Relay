/**
 * alert_dropped_cells (migration 0015, docs/cancellation.md): when a new version replaces an
 * alert's cells, the cells it no longer covers are recorded under the new version, in the save's
 * transaction. Ingest writes them, the fan-out planner reads them; nothing else may.
 */
import { readFile } from 'node:fs/promises';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { splitStatements } from '../../src/db/sql.js';
import { MIGRATIONS_DIR, alert, freshDb, type PgliteExecutor } from './helpers.js';

let db: PgliteExecutor;

async function as<T>(role: string, fn: () => Promise<T>): Promise<T> {
  await db.pg.exec(`SET ROLE ${role}`);
  try {
    return await fn();
  } finally {
    await db.pg.exec('RESET ROLE');
  }
}

beforeAll(async () => {
  db = await freshDb();
});

afterEach(async () => {
  await db.pg.exec('RESET ROLE');
});

const A = '872830828ffffff';
const B = '872830829ffffff';
const C = '87283082affffff';

describe('alert_dropped_cells', () => {
  it('records the cells a new version dropped, only for that version, and not on a re-save', async () => {
    const store = new PostgresAlertStore(db);
    const v1 = alert({ area: { ...alert().area, h3Cells: [A, B] } });
    await store.save(v1);
    await store.save({ ...v1, event: 'same version re-saved' });
    expect(await store.listDroppedCells(v1.id, v1.sourceId)).toEqual([]);

    const v2 = { ...v1, sourceId: 'urn:oid:v2', area: { ...v1.area, h3Cells: [B, C] } };
    await store.save(v2);
    expect(await store.listDroppedCells(v1.id, 'urn:oid:v2')).toEqual([A]);
    expect((await store.get(v1.id))?.area.h3Cells).toEqual([B, C]);

    // A Cancel keeps the area: nothing dropped.
    await store.save({ ...v2, sourceId: 'urn:oid:v3', state: 'cancelled' });
    expect(await store.listDroppedCells(v1.id, 'urn:oid:v3')).toEqual([]);
    expect(await store.listDroppedCells(v1.id, 'urn:oid:v2')).toEqual([A]);
  });

  it('ingest writes it through the store, the planner reads it, neither can do more', async () => {
    // As db-roles.ts does after migrating: re-run the grants (they name the table now).
    const sql = await readFile(`${MIGRATIONS_DIR}/0011_roles.sql`, 'utf8');
    for (const statement of splitStatements(sql)) await db.query(statement, []);
    const id = '55555555-5555-4555-8555-555555555555';
    const v1 = alert({ id, sourceId: 'urn:r1', area: { ...alert().area, h3Cells: [A, B] } });
    await as('relay_ingest', async () => {
      const store = new PostgresAlertStore(db);
      await store.save(v1);
      await store.save({ ...v1, sourceId: 'urn:r2', area: { ...v1.area, h3Cells: [A] } });
    });
    expect(
      await as('relay_fanout', () => new PostgresAlertStore(db).listDroppedCells(id, 'urn:r2')),
    ).toEqual([B]);
    await expect(
      as('relay_fanout', () =>
        db.pg.exec(`INSERT INTO alert_dropped_cells VALUES ('${id}', 'x', 'y')`),
      ),
    ).rejects.toThrow(/permission denied/);
    for (const role of ['relay_api', 'relay_sender', 'relay_latejoin', 'relay_ops']) {
      await expect(
        as(role, () => db.pg.exec('SELECT * FROM alert_dropped_cells')),
        role,
      ).rejects.toThrow(/permission denied/);
    }
  });
});
