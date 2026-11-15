import type { Alert } from '@relay/shared';
import { cellToChildren, cellToParent, gridDisk, latLngToCell } from 'h3-js';
import { describe, expect, it } from 'vitest';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import type { SqlExecutor } from '../../src/db/sql.js';
import { AlertPublisher, type PublicObjectWriter } from '../../src/scale/publish/index.js';
import { alert as dbAlert, freshDb } from './helpers.js';

/** PERF-11: every query the publisher sends to the database counts as one Data API call. */
describe('publish reads (Data API call count)', () => {
  it('a many-shard alert is read with far fewer queries than one read per shard', async () => {
    const db = await freshDb();
    const center = latLngToCell(34.05, -118.25, 7);
    const cells = gridDisk(center, 40); // about 4,900 cells across a few dozen res-4 shards
    const shards = [...new Set(cells.map((c) => cellToParent(c, 4)))];
    expect(shards.length).toBeGreaterThan(8);

    const now = new Date('2026-11-10T10:05:00Z');
    const big: Alert = dbAlert({
      area: { description: 'big', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: cells },
      expires: '2026-11-10T18:00:00Z',
    });
    await new PostgresAlertStore(db).save(big);

    let queries = 0;
    const counting: SqlExecutor = {
      query: (sql, params) => {
        queries++;
        return db.query(sql, params);
      },
      transaction: (fn) => db.transaction(fn),
    };
    const store = new PostgresAlertStore(counting);
    const writer: PublicObjectWriter = { putJson: async () => {} };

    // Before: one listActiveForCells per shard, each re-reading the big alert's cells.
    for (const shard of shards) await store.listActiveForCells(cellToChildren(shard, 7), now);
    const before = queries;

    queries = 0;
    const publisher = new AlertPublisher({
      writer,
      alerts: store,
      cellResolution: 7,
      clock: { now: () => now },
    });
    await publisher.publish(big);
    const after = queries;

    console.log(JSON.stringify({ perf11: { shards: shards.length, before, after } }));
    expect(after).toBeLessThan(before / 3);
    // One cell-chunk query per 2,000 cells asked, plus one cell read for the alert found.
    expect(after).toBeLessThanOrEqual(Math.ceil((shards.length * 343) / 2000) + 2);
  });
});
