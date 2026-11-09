import { beforeEach, describe, expect, it } from 'vitest';
import { PostgresDeviceStore } from '../../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../../src/adapters/postgres/place-store.js';
import { MemoryCellIndex, backfillCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndex, CellIndexEntry } from '../../../src/scale/contracts.js';
import { PgliteExecutor, device, freshDb, place } from '../../db/helpers.js';

const APNS = 'ab12'.repeat(16);
const id = (prefix: string, n: number) =>
  `${prefix.repeat(8)}-${prefix.repeat(4)}-4${prefix.repeat(3)}-8${prefix.repeat(3)}-${String(n).padStart(12, '0')}`;

let db: PgliteExecutor;
let devices: PostgresDeviceStore;
let places: PostgresPlaceStore;

beforeEach(async () => {
  db = await freshDb();
  devices = new PostgresDeviceStore(db);
  places = new PostgresPlaceStore(db);
});

async function seed(): Promise<void> {
  // 1: expo (legacy), 2: native iOS, 3: no token. Seven places across two cells.
  await devices.create(
    device({ id: id('1', 1), credentialHash: 'h1', expoPushToken: 'ExponentPushToken[a]' }),
  );
  await devices.create(
    device({ id: id('2', 2), credentialHash: 'h2', expoPushToken: null, language: 'es' }),
  );
  await devices.assignPushTarget(id('2', 2), { platform: 'ios', token: APNS }, 'es');
  await devices.create(device({ id: id('3', 3), credentialHash: 'h3', expoPushToken: null }));
  const cells = ['872830828ffffff', '872830829ffffff'];
  for (let n = 1; n <= 7; n++) {
    await places.add(
      place({
        id: id('a', n),
        deviceId: id(String(((n - 1) % 3) + 1), ((n - 1) % 3) + 1),
        h3Cell: cells[n % 2] ?? '',
      }),
    );
  }
}

async function dump(index: CellIndex): Promise<CellIndexEntry[]> {
  const out: CellIndexEntry[] = [];
  for (const c of ['872830828ffffff', '872830829ffffff'])
    out.push(...(await index.listForCell(c)).entries);
  return out.sort((a, b) => a.placeId.localeCompare(b.placeId));
}

describe('backfillCellIndex on PGlite', () => {
  it('copies every place with its device target and language, across several pages', async () => {
    await seed();
    const index = new MemoryCellIndex();
    const pagesSeen: number[] = [];
    const counts = await backfillCellIndex({
      db,
      index,
      pageSize: 3,
      onPage: (c) => pagesSeen.push(c.pages),
    });
    expect(counts).toEqual({
      pages: 3,
      placesScanned: 7,
      entriesPut: 7,
      withTarget: 5, // devices 1 and 2 own 3 + 2 places; device 3 owns 2 without a target
      withoutTarget: 2,
      dryRun: false,
    });
    expect(pagesSeen).toEqual([1, 2, 3]);
    const entries = await dump(index);
    expect(entries).toHaveLength(7);
    const byDevice = (d: string) => entries.filter((e) => e.deviceId === d);
    expect(
      byDevice(id('1', 1)).every((e) => e.target?.platform === 'expo' && e.language === 'en'),
    ).toBe(true);
    expect(
      byDevice(id('2', 2)).every(
        (e) => e.target?.platform === 'ios' && e.target.token === APNS && e.language === 'es',
      ),
    ).toBe(true);
    expect(byDevice(id('3', 3)).every((e) => e.target === null)).toBe(true);
  });

  it('handles an exact multiple of the page size and an empty database', async () => {
    const empty = await backfillCellIndex({ db, index: new MemoryCellIndex(), pageSize: 3 });
    expect(empty).toMatchObject({ pages: 0, placesScanned: 0 });
    await seed();
    await places.remove(id('3', 3), id('a', 3));
    await places.remove(id('3', 3), id('a', 6));
    await places.remove(id('1', 1), id('a', 7));
    // 4 places left: two full pages, then an empty one that ends the loop.
    const counts = await backfillCellIndex({ db, index: new MemoryCellIndex(), pageSize: 2 });
    expect(counts).toMatchObject({ pages: 2, placesScanned: 4, entriesPut: 4 });
  });

  it('is idempotent: a second run changes nothing, and picks up target changes', async () => {
    await seed();
    const index = new MemoryCellIndex();
    await backfillCellIndex({ db, index, pageSize: 4 });
    const first = await dump(index);
    await backfillCellIndex({ db, index, pageSize: 4 });
    expect(await dump(index)).toEqual(first);
    await devices.assignPushTarget(
      id('3', 3),
      { platform: 'android', token: 'f'.repeat(120) },
      'en',
    );
    await backfillCellIndex({ db, index });
    const after = await dump(index);
    expect(after).toHaveLength(7);
    expect(after.filter((e) => e.target?.platform === 'android')).toHaveLength(2);
  });

  it('writes nothing in a dry run but reports the same counts', async () => {
    await seed();
    const index = new MemoryCellIndex();
    const counts = await backfillCellIndex({ db, index, pageSize: 3, dryRun: true });
    expect(counts).toMatchObject({ dryRun: true, placesScanned: 7, entriesPut: 7, withTarget: 5 });
    expect(await dump(index)).toEqual([]);
  });

  it('reconciles an index that missed writes', async () => {
    await seed();
    const index = new MemoryCellIndex();
    await backfillCellIndex({ db, index });
    await index.remove('872830829ffffff', id('1', 1), id('a', 1)); // a lost put
    await index.updateDevice(id('2', 2), [{ h3Cell: '872830828ffffff', placeId: id('a', 2) }], {
      target: null,
      language: 'en',
    }); // a lost update
    await backfillCellIndex({ db, index });
    const entries = await dump(index);
    expect(entries).toHaveLength(7);
    expect(entries.find((e) => e.placeId === id('a', 2))?.target?.token).toBe(APNS);
  });
});
