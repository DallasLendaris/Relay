import { cellToLatLng, latLngToCell } from 'h3-js';
import { beforeEach, describe, expect, it } from 'vitest';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import type { DeviceErasure, DeviceStore, PlaceErasure, PlaceStore } from '../../src/ports.js';
import { DEVICE_ID, device, freshDb, place, type PgliteExecutor } from './helpers.js';

const OTHER = '99999999-9999-4999-8999-999999999999';
const PLACE_2 = '55555555-5555-4555-8555-555555555555';
const WHEN = new Date('2026-11-09T12:00:00.000Z');

let db: PgliteExecutor;
beforeEach(async () => {
  db = await freshDb();
});

describe('migration 0010 and PostgresPlaceStore: no exact coordinates', () => {
  it('drops the latitude and longitude columns and adds the cell center', async () => {
    const cols = (
      await db.query<{ column_name: string; is_nullable: string }>(
        `SELECT column_name, is_nullable FROM information_schema.columns
          WHERE table_name = 'places'`,
        [],
      )
    ).map((r) => r.column_name);
    expect(cols).not.toContain('latitude');
    expect(cols).not.toContain('longitude');
    expect(cols).toEqual(expect.arrayContaining(['cell_center_lat', 'cell_center_lng']));
  });

  it('stores the H3 cell center, derived from the cell (the caller cannot pass a point)', async () => {
    await new PostgresDeviceStore(db).create(device());
    const places = new PostgresPlaceStore(db);
    const cell = latLngToCell(37.3352, -121.8811, 7);
    await places.add(place({ h3Cell: cell }));
    const [row] = await db.query<{ cell_center_lat: number; cell_center_lng: number }>(
      'SELECT cell_center_lat, cell_center_lng FROM places',
      [],
    );
    const [lat, lng] = cellToLatLng(cell);
    expect(row?.cell_center_lat).toBeCloseTo(lat, 9);
    expect(row?.cell_center_lng).toBeCloseTo(lng, 9);
    // The center is not the point the person gave.
    expect(row?.cell_center_lat).not.toBe(37.3352);
  });

  it('a place without a shortAddress is stored with NULL and read back without the field', async () => {
    await new PostgresDeviceStore(db).create(device());
    const places = new PostgresPlaceStore(db);
    const { shortAddress: _drop, ...pin } = place();
    await places.add(pin);
    const [row] = await db.query<{ short_address: string | null }>(
      'SELECT short_address FROM places',
      [],
    );
    expect(row?.short_address).toBeNull();
    const back = await places.listForDevice(DEVICE_ID);
    expect(back).toEqual([pin]);
    expect(back[0]).not.toHaveProperty('shortAddress');
    expect(back[0]).not.toHaveProperty('latitude');
    // An address place keeps its text.
    await places.add(place({ id: PLACE_2 }));
    expect((await places.listForDevice(DEVICE_ID)).map((p) => p.shortAddress)).toEqual([
      undefined,
      '1 Main St, San Jose',
    ]);
  });

  it('the database itself refuses a label over 60 characters (or an empty one)', async () => {
    await new PostgresDeviceStore(db).create(device());
    const places = new PostgresPlaceStore(db);
    await places.add(place({ label: 'a'.repeat(60) }));
    await expect(places.add(place({ id: PLACE_2, label: 'a'.repeat(61) }))).rejects.toThrow();
    await expect(places.add(place({ id: PLACE_2, label: '' }))).rejects.toThrow();
  });
});

describe.each([
  [
    'Postgres',
    async () => {
      const exec = await freshDb();
      return {
        devices: new PostgresDeviceStore(exec),
        places: new PostgresPlaceStore(exec),
      };
    },
  ],
  ['memory', async () => ({ devices: new MemoryDeviceStore(), places: new MemoryPlaceStore() })],
] as const)('device erasure: %s stores', (_name, make) => {
  type Stores = { devices: DeviceStore & DeviceErasure; places: PlaceStore & PlaceErasure };
  let s: Stores;
  beforeEach(async () => {
    s = await make();
    await s.devices.create(device());
    await s.devices.create(
      device({ id: OTHER, credentialHash: 'b'.repeat(64), expoPushToken: null }),
    );
    await s.places.add(place());
    await s.places.add(place({ id: PLACE_2 }));
    await s.places.add(place({ id: '66666666-6666-4666-8666-666666666666', deviceId: OTHER }));
  });

  it('removes the device and records a tombstone; other devices and their places stay', async () => {
    expect(await s.devices.listDeletedDevices()).toEqual([]);
    expect(await s.places.removeAllForDevice(DEVICE_ID)).toBeGreaterThanOrEqual(0);
    expect(await s.devices.deleteDevice(DEVICE_ID, WHEN)).toBe(true);

    expect(await s.devices.get(DEVICE_ID)).toBeNull();
    expect(await s.places.listForDevice(DEVICE_ID)).toEqual([]);
    expect(await s.devices.listDeletedDevices()).toEqual([DEVICE_ID]);
    expect(await s.devices.get(OTHER)).not.toBeNull();
    expect(await s.places.listForDevice(OTHER)).toHaveLength(1);
  });

  it('removeAllForDevice returns how many places went, and none for another device', async () => {
    expect(await s.places.removeAllForDevice(DEVICE_ID)).toBe(2);
    expect(await s.places.removeAllForDevice(DEVICE_ID)).toBe(0);
    expect(await s.places.listForDevice(OTHER)).toHaveLength(1);
  });

  it('is idempotent: a second delete returns false and keeps one tombstone', async () => {
    expect(await s.devices.deleteDevice(DEVICE_ID, WHEN)).toBe(true);
    expect(await s.devices.deleteDevice(DEVICE_ID, new Date(WHEN.getTime() + 1000))).toBe(false);
    expect(await s.devices.listDeletedDevices()).toEqual([DEVICE_ID]);
  });

  it('a tombstone is recorded even if the device row was already gone', async () => {
    expect(await s.devices.deleteDevice('77777777-7777-4777-8777-777777777777', WHEN)).toBe(false);
    expect(await s.devices.listDeletedDevices()).toEqual(['77777777-7777-4777-8777-777777777777']);
  });
});

describe('PostgresDeviceStore.deleteDevice (SQL specifics)', () => {
  it('cascades the places through the foreign key and keeps the deleted_at time', async () => {
    const devices = new PostgresDeviceStore(db);
    await devices.create(device());
    await new PostgresPlaceStore(db).add(place());
    expect(await devices.deleteDevice(DEVICE_ID, WHEN)).toBe(true);
    expect(await db.query('SELECT 1 FROM places', [])).toEqual([]);
    const [t] = await db.query<{ device_id: string; deleted_at: string }>(
      'SELECT device_id, deleted_at FROM deleted_devices',
      [],
    );
    expect(t?.device_id).toBe(DEVICE_ID);
    expect(new Date(t?.deleted_at ?? '').toISOString()).toBe(WHEN.toISOString());
  });
});
