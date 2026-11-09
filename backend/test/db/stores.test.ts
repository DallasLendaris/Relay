import type { Alert } from '@relay/shared';
import { beforeEach, describe, expect, it } from 'vitest';
import type { SqlExecutor } from '../../src/db/sql.js';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import { DEVICE_ID, alert, device, freshDb, place, type PgliteExecutor } from './helpers.js';

const NOW = new Date('2026-01-02T05:00:00.000Z'); // before the fixture alert's 09:00 expiry

/** n distinct, valid-looking res-7 H3 indexes. */
const manyCells = (n: number): string[] =>
  Array.from({ length: n }, (_, i) => `872830${(i * 104729).toString(16).padStart(9, '0')}`);

let db: PgliteExecutor;
beforeEach(async () => {
  db = await freshDb();
});

describe('PostgresDeviceStore', () => {
  it('creates, gets and updates a device', async () => {
    const store = new PostgresDeviceStore(db);
    expect(await store.get(DEVICE_ID)).toBeNull();
    await store.create(device());
    // A device with only the legacy Expo token also gets the matching native target.
    expect(await store.get(DEVICE_ID)).toEqual(
      device({ pushPlatform: 'expo', pushToken: 'ExponentPushToken[abc]', pushProject: null }),
    );
    await store.update(device({ language: 'es', expoPushToken: null }));
    expect(await store.get(DEVICE_ID)).toEqual(
      device({
        language: 'es',
        expoPushToken: null,
        pushPlatform: null,
        pushToken: null,
        pushProject: null,
      }),
    );
  });

  it('rejects a duplicate credential hash', async () => {
    const store = new PostgresDeviceStore(db);
    await store.create(device());
    await expect(
      store.create(device({ id: '99999999-9999-4999-8999-999999999999' })),
    ).rejects.toThrow();
  });
});

describe('PostgresDeviceStore push tokens', () => {
  const B = '99999999-9999-4999-8999-999999999999';
  const tokenHolders = async (token: string) =>
    (
      await db.query<{ id: string }>('SELECT id FROM devices WHERE expo_push_token = $1', [token])
    ).map((r) => r.id);

  it('a reinstall (second device, same token) leaves exactly one holder', async () => {
    const store = new PostgresDeviceStore(db);
    await store.create(device({ expoPushToken: null }));
    await store.create(device({ id: B, credentialHash: 'b'.repeat(64), expoPushToken: null }));
    await store.assignPushToken(DEVICE_ID, 'ExponentPushToken[t]', 'en');
    expect(await tokenHolders('ExponentPushToken[t]')).toEqual([DEVICE_ID]);
    await store.assignPushToken(B, 'ExponentPushToken[t]', 'es');
    expect(await tokenHolders('ExponentPushToken[t]')).toEqual([B]);
    expect(await store.get(B)).toMatchObject({ language: 'es' });
    expect((await store.get(DEVICE_ID))?.expoPushToken).toBeNull();
  });

  it('the unique index refuses one token on two devices through update', async () => {
    const store = new PostgresDeviceStore(db);
    await store.create(device());
    await store.create(device({ id: B, credentialHash: 'b'.repeat(64), expoPushToken: null }));
    await expect(store.update(device({ id: B, credentialHash: 'b'.repeat(64) }))).rejects.toThrow();
  });

  it('assignPushToken for an unknown device throws and takes no token from others', async () => {
    const store = new PostgresDeviceStore(db);
    await store.create(device());
    await expect(store.assignPushToken(B, 'ExponentPushToken[abc]', 'en')).rejects.toThrow(
      /not found/,
    );
    expect((await store.get(DEVICE_ID))?.expoPushToken).toBe('ExponentPushToken[abc]');
  });

  it('clearPushToken forgets the token', async () => {
    const store = new PostgresDeviceStore(db);
    await store.create(device());
    await store.clearPushToken('ExponentPushToken[abc]');
    expect((await store.get(DEVICE_ID))?.expoPushToken).toBeNull();
    await store.clearPushToken('never-held'); // no error
  });
});

describe('PostgresPlaceStore', () => {
  beforeEach(async () => {
    await new PostgresDeviceStore(db).create(device());
  });

  it('adds, lists and removes places', async () => {
    const store = new PostgresPlaceStore(db);
    await store.add(place());
    expect(await store.listForDevice(DEVICE_ID)).toEqual([place()]);
    expect(await store.remove(DEVICE_ID, place().id)).toBe(true);
    expect(await store.remove(DEVICE_ID, place().id)).toBe(false);
    expect(await store.listForDevice(DEVICE_ID)).toEqual([]);
  });

  it("does not remove another device's place", async () => {
    const store = new PostgresPlaceStore(db);
    await store.add(place());
    expect(await store.remove('99999999-9999-4999-8999-999999999999', place().id)).toBe(false);
    expect(await store.listForDevice(DEVICE_ID)).toHaveLength(1);
  });

  it('listForCells returns only places in those cells', async () => {
    const store = new PostgresPlaceStore(db);
    await store.add(place());
    await store.add(
      place({ id: '44444444-4444-4444-8444-444444444444', h3Cell: '872830829ffffff' }),
    );
    await store.add(
      place({ id: '55555555-5555-4555-8555-555555555555', h3Cell: '87283082affffff' }),
    );
    const found = await store.listForCells(['872830828ffffff', '872830829ffffff', 'nope']);
    expect(found.map((p) => p.h3Cell).sort()).toEqual(['872830828ffffff', '872830829ffffff']);
    expect(await store.listForCells([])).toEqual([]);
  });

  it('listMatchesForCells pages with keyset pagination and returns only ids', async () => {
    const small = new PostgresPlaceStore(db, 3);
    const queries: unknown[][] = [];
    const spy = new PostgresPlaceStore(
      {
        query: async (sql, params) => {
          queries.push(params);
          return db.query(sql, params);
        },
        transaction: (fn) => db.transaction(fn),
      },
      3,
    );
    const ids: string[] = [];
    for (let i = 1; i <= 7; i++) {
      const id = `00000000-0000-4000-8000-00000000000${i}`;
      ids.push(id);
      await small.add(place({ id, h3Cell: i === 7 ? 'other' : '872830828ffffff' }));
    }
    const found = await spy.listMatchesForCells(['872830828ffffff', '872830828ffffff']);
    expect(found).toEqual(
      ids
        .slice(0, 6)
        .map((id) => ({ placeId: id, deviceId: DEVICE_ID, h3Cell: '872830828ffffff' })),
    );
    expect(queries).toHaveLength(3); // 3 + 3 + an empty page (6 is a multiple of 3)
    expect(await small.listMatchesForCells([])).toEqual([]);
    // The default page size works too.
    expect(await new PostgresPlaceStore(db).listMatchesForCells(['other'])).toHaveLength(1);
  });

  it('cascades: deleting a device deletes its places', async () => {
    const store = new PostgresPlaceStore(db);
    await store.add(place());
    await db.query('DELETE FROM devices WHERE id = $1', [DEVICE_ID]);
    expect(await store.listForDevice(DEVICE_ID)).toEqual([]);
  });

  it('rejects a place for an unknown device', async () => {
    const store = new PostgresPlaceStore(db);
    await expect(
      store.add(place({ deviceId: '99999999-9999-4999-8999-999999999999' })),
    ).rejects.toThrow();
  });
});

describe('PostgresAlertStore', () => {
  it('saves and gets an alert, round-tripping the full record', async () => {
    const store = new PostgresAlertStore(db);
    expect(await store.get(alert().id)).toBeNull();
    await store.save(alert());
    expect(await store.get(alert().id)).toEqual(alert());
  });

  it('save replaces by id', async () => {
    const store = new PostgresAlertStore(db);
    await store.save(alert());
    await store.save(alert({ state: 'cancelled', event: 'Changed' }));
    const got = await store.get(alert().id);
    expect(got?.state).toBe('cancelled');
    expect(got?.event).toBe('Changed');
  });

  it('findBySourceId finds the alert by its own id and by every referenced id', async () => {
    const store = new PostgresAlertStore(db);
    const updated = alert({
      sourceId: 'urn:oid:2.49.0.1.840.0.bbb',
      messageType: 'Update',
      state: 'updated',
      references: [
        { sourceId: 'urn:oid:2.49.0.1.840.0.aaa', sent: '2026-01-02T03:00:00.000Z' },
        { sourceId: 'urn:oid:2.49.0.1.840.0.zzz', sent: '2026-01-02T02:00:00.000Z' },
      ],
    });
    await store.save(alert());
    await store.save(updated);
    for (const id of ['aaa', 'bbb', 'zzz']) {
      expect((await store.findBySourceId(`urn:oid:2.49.0.1.840.0.${id}`))?.state).toBe('updated');
    }
    expect(await store.findBySourceId('urn:oid:unknown')).toBeNull();
  });

  it('listActiveForCells uses overlap and ignores cancelled or expired alerts', async () => {
    const store = new PostgresAlertStore(db);
    const idN = (n: number) => `00000000-0000-4000-8000-00000000000${n}`;
    const withCells = (
      n: number,
      cells: string[],
      state: 'active' | 'updated' | 'cancelled' | 'expired',
    ) =>
      alert({
        id: idN(n),
        sourceId: `urn:oid:${n}`,
        state,
        area: { ...alert().area, h3Cells: cells },
      });
    await store.save(withCells(1, ['c1', 'c2'], 'active'));
    await store.save(withCells(2, ['c2', 'c3'], 'updated'));
    await store.save(withCells(3, ['c2'], 'cancelled'));
    await store.save(withCells(4, ['c2'], 'expired'));
    await store.save(withCells(5, ['c9'], 'active'));

    const ids = async (cells: string[]) =>
      (await store.listActiveForCells(cells, NOW)).map((a) => a.id).sort();
    expect(await ids(['c2'])).toEqual([idN(1), idN(2)]);
    expect(await ids(['c3', 'c9'])).toEqual([idN(2), idN(5)]);
    expect(await ids(['c1', 'c2', 'c3'])).toEqual([idN(1), idN(2)]); // no duplicates
    expect(await ids(['nope'])).toEqual([]);
    expect(await ids([])).toEqual([]);
  });

  it('listActiveForCells excludes ended alerts by time, honouring a later ends', async () => {
    const store = new PostgresAlertStore(db);
    const a = (n: number, over: Partial<Alert>) =>
      alert({ id: `00000000-0000-4000-8000-00000000000${n}`, sourceId: `urn:oid:t${n}`, ...over });
    await store.save(a(1, {})); // expires 09:00Z, no ends
    await store.save(a(2, { ends: '2026-01-02T12:00:00.000Z' })); // runs on past expires
    await store.save(a(3, { ends: '2026-01-02T07:00:00.000Z' })); // ends earlier than expires
    const ids = async (iso: string) =>
      (await store.listActiveForCells(['872830828ffffff'], new Date(iso)))
        .map((x) => x.id.slice(-1))
        .sort();
    expect(await ids('2026-01-02T08:00:00.000Z')).toEqual(['1', '2', '3']);
    expect(await ids('2026-01-02T10:00:00.000Z')).toEqual(['2']);
    expect(await ids('2026-01-02T13:00:00.000Z')).toEqual([]);
  });

  it('round-trips cells (returned sorted), including an empty list, via alert_cells', async () => {
    const store = new PostgresAlertStore(db);
    const cells = manyCells(300);
    await store.save(alert({ area: { ...alert().area, h3Cells: cells } }));
    expect((await store.get(alert().id))?.area.h3Cells).toEqual([...cells].sort());
    const rows = await db.query<{ n: number }>('SELECT count(*)::int AS n FROM alert_cells', []);
    expect(rows[0]?.n).toBe(300);
    const stored = await db.query<{ record: Alert }>('SELECT record FROM alerts', []);
    expect(stored[0]?.record.area.h3Cells).toEqual([]); // the record itself carries no cells
    await store.save(alert({ area: { ...alert().area, h3Cells: [] } }));
    expect((await store.get(alert().id))?.area.h3Cells).toEqual([]);
  });

  /** Executor that records every statement's size and can fail on demand. */
  const measuring = (failOn?: (n: number) => boolean) => {
    const sizes: number[] = [];
    let n = 0;
    const wrap = (inner: SqlExecutor): SqlExecutor => ({
      query: async (sql, params) => {
        sizes.push(Buffer.byteLength(sql) + Buffer.byteLength(JSON.stringify(params)));
        if (failOn?.(++n)) throw new Error('boom');
        return inner.query(sql, params);
      },
      transaction: (fn) => inner.transaction((tx) => fn(wrap(tx))),
    });
    return { exec: wrap(db), sizes };
  };

  it('saves and round-trips a 60,000-cell alert, every request under 64 KB', async () => {
    const { exec, sizes } = measuring();
    const cells = manyCells(60_000);
    const big = alert({ area: { ...alert().area, h3Cells: cells } });
    await new PostgresAlertStore(exec).save(big);
    expect(sizes.length).toBeGreaterThan(10); // chunked
    expect(Math.max(...sizes)).toBeLessThan(64 * 1024);
    const got = await new PostgresAlertStore(db).get(big.id);
    expect(got?.area.h3Cells).toEqual([...cells].sort());
    expect({ ...got, area: { ...got?.area, h3Cells: [] } }).toEqual({
      ...big,
      area: { ...big.area, h3Cells: [] },
    });
    // Saving again replaces the cells rather than adding to them.
    const smaller = { ...big, area: { ...big.area, h3Cells: cells.slice(0, 5) } };
    await new PostgresAlertStore(db).save(smaller);
    expect((await new PostgresAlertStore(db).get(big.id))?.area.h3Cells).toHaveLength(5);
  });

  it('rolls back everything when a statement fails part-way', async () => {
    const original = alert({ area: { ...alert().area, h3Cells: manyCells(10) } });
    await new PostgresAlertStore(db).save(original);
    // Statements: upsert (1), delete (2), then chunk inserts (3, 4...). Fail on the 2nd chunk.
    const { exec } = measuring((n) => n === 4);
    const replacement = alert({
      event: 'Changed',
      area: { ...alert().area, h3Cells: manyCells(9000) },
    });
    await expect(new PostgresAlertStore(exec).save(replacement)).rejects.toThrow('boom');
    const got = await new PostgresAlertStore(db).get(original.id);
    expect(got?.event).toBe(original.event);
    expect(got?.area.h3Cells).toEqual([...original.area.h3Cells].sort());
  });

  it('refuses, with a clear error, a record too big for one request', async () => {
    const huge = alert({ official: { ...alert().official, description: 'x'.repeat(70_000) } });
    await expect(new PostgresAlertStore(db).save(huge)).rejects.toThrow(/too large/);
    expect(await new PostgresAlertStore(db).get(huge.id)).toBeNull();
  });

  it('listActiveForCells finds a huge alert by one of its cells and returns all its cells', async () => {
    const store = new PostgresAlertStore(db);
    const cells = manyCells(20_000);
    await store.save(alert({ area: { ...alert().area, h3Cells: cells } }));
    const found = await store.listActiveForCells([cells[19_999] ?? '', 'nope'], NOW);
    expect(found).toHaveLength(1);
    expect(found[0]?.area.h3Cells).toHaveLength(20_000);
  });

  it('deleting an alert cascades to its source ids', async () => {
    const store = new PostgresAlertStore(db);
    await store.save(alert());
    await db.query('DELETE FROM alerts WHERE id = $1', [alert().id]);
    expect(await store.findBySourceId(alert().sourceId)).toBeNull();
  });
});
