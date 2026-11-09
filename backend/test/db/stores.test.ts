import { beforeEach, describe, expect, it } from 'vitest';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import { DEVICE_ID, alert, device, freshDb, place, type PgliteExecutor } from './helpers.js';

let db: PgliteExecutor;
beforeEach(async () => {
  db = await freshDb();
});

describe('PostgresDeviceStore', () => {
  it('creates, gets and updates a device', async () => {
    const store = new PostgresDeviceStore(db);
    expect(await store.get(DEVICE_ID)).toBeNull();
    await store.create(device());
    expect(await store.get(DEVICE_ID)).toEqual(device());
    await store.update(device({ language: 'es', expoPushToken: null }));
    expect(await store.get(DEVICE_ID)).toEqual(device({ language: 'es', expoPushToken: null }));
  });

  it('rejects a duplicate credential hash', async () => {
    const store = new PostgresDeviceStore(db);
    await store.create(device());
    await expect(
      store.create(device({ id: '99999999-9999-4999-8999-999999999999' })),
    ).rejects.toThrow();
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
      (await store.listActiveForCells(cells)).map((a) => a.id).sort();
    expect(await ids(['c2'])).toEqual([idN(1), idN(2)]);
    expect(await ids(['c3', 'c9'])).toEqual([idN(2), idN(5)]);
    expect(await ids(['c1', 'c2', 'c3'])).toEqual([idN(1), idN(2)]); // no duplicates
    expect(await ids(['nope'])).toEqual([]);
    expect(await ids([])).toEqual([]);
  });

  it('deleting an alert cascades to its source ids', async () => {
    const store = new PostgresAlertStore(db);
    await store.save(alert());
    await db.query('DELETE FROM alerts WHERE id = $1', [alert().id]);
    expect(await store.findBySourceId(alert().sourceId)).toBeNull();
  });
});
