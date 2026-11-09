import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import { createApi, type ApiEvent } from '../../src/api/index.js';
import { cellForPoint } from '../../src/geo/index.js';
import {
  MemoryCellIndex,
  PostgresCellIndexOutbox,
  drainOutbox,
  type CellIndexOutbox,
} from '../../src/scale/cell-index/index.js';
import type { CellIndex } from '../../src/scale/contracts.js';
import { DEVICE_ID, freshDb, place, device, type PgliteExecutor } from './helpers.js';

const NOW = new Date('2026-11-09T12:00:00Z');
const APNS = 'ab12'.repeat(16);
const SJ = { label: 'Home', latitude: 37.3352, longitude: -121.8811 };
const SJ_CELL = cellForPoint(SJ.latitude, SJ.longitude);
const LA = { label: 'Work', latitude: 34.05, longitude: -118.25 };

/** A cell index that can be told to fail, like DynamoDB being down or the Lambda dying. */
class FlakyIndex extends MemoryCellIndex {
  down = false;
  override async put(entries: Parameters<CellIndex['put']>[0]) {
    if (this.down) throw new Error('dynamo down');
    return super.put(entries);
  }
  override async remove(...a: Parameters<CellIndex['remove']>) {
    if (this.down) throw new Error('dynamo down');
    return super.remove(...a);
  }
  override async updateDevice(...a: Parameters<CellIndex['updateDevice']>) {
    if (this.down) throw new Error('dynamo down');
    return super.updateDevice(...a);
  }
}

let db: PgliteExecutor;
let index: FlakyIndex;
let outbox: PostgresCellIndexOutbox;
let outboxForApi: CellIndexOutbox;
let route: ReturnType<typeof createApi>;

const rows = () =>
  db.query<{ device_id: string; op: string; attempts: number; payload: unknown }>(
    'SELECT device_id, op, attempts, payload FROM cell_index_outbox ORDER BY created_at, op',
    [],
  );

const ev = (method: string, path: string, credential?: string, body?: unknown): ApiEvent => ({
  rawPath: path,
  headers: credential ? { authorization: `Bearer ${credential}` } : {},
  ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  requestContext: { http: { method } },
});

const register = async () => {
  const r = JSON.parse((await route(ev('POST', '/devices'))).body) as {
    deviceId: string;
    credential: string;
  };
  return r;
};

const repairDeps = () => ({
  devices: new PostgresDeviceStore(db),
  places: new PostgresPlaceStore(db),
  cellIndex: index,
  outbox,
});

/** A drain run 31 s from now: the rows are past the 30 s minimum age. */
const later = () => new Date(Date.now() + 31_000);
const drain = () => drainOutbox({ ...repairDeps(), now: later, log: () => {}, metric: () => {} });

const indexed = async (cell: string) => (await index.listForCell(cell)).entries;

beforeEach(async () => {
  db = await freshDb();
  index = new FlakyIndex();
  outbox = new PostgresCellIndexOutbox(db);
  outboxForApi = outbox;
  route = createApi({
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    alerts: new PostgresAlertStore(db),
    geocoder: { geocode: async () => null },
    checkins: new MemoryCheckInStore(),
    cellIndex: index,
    outbox: outboxForApi,
    tokenKeys: new Map(),
    clock: { now: () => NOW },
    newId: randomUUID,
  });
});

describe('REL-3: cell-index outbox', () => {
  it('writes the outbox row in the SAME transaction as the place: a failed insert leaves no row', async () => {
    const places = new PostgresPlaceStore(db);
    await new PostgresDeviceStore(db).create(device());
    const p = place();
    await places.add(p, {
      outbox: { id: randomUUID(), op: 'put', h3Cell: p.h3Cell, placeId: p.id },
    });
    expect(await rows()).toHaveLength(1);

    // Same place id again: the INSERT fails, so the whole transaction (incl. its outbox row) rolls back.
    await expect(
      places.add(p, { outbox: { id: randomUUID(), op: 'put', h3Cell: p.h3Cell, placeId: p.id } }),
    ).rejects.toThrow();
    expect(await rows()).toHaveLength(1);

    // And the other way: an outbox row that fails (duplicate id) rolls the place back.
    const q = place({ id: '33333333-3333-4333-8333-333333333333' });
    const dupId = randomUUID();
    await db.query(
      `INSERT INTO cell_index_outbox (id, device_id, op) VALUES ($1::uuid, $2::uuid, 'put')`,
      [dupId, DEVICE_ID],
    );
    await expect(
      places.add(q, { outbox: { id: dupId, op: 'put', h3Cell: q.h3Cell, placeId: q.id } }),
    ).rejects.toThrow();
    expect((await places.listForDevice(DEVICE_ID)).map((x) => x.id)).toEqual([p.id]);
  });

  it('after the commit the API writes through and deletes the outbox row (happy path)', async () => {
    const { credential, deviceId } = await register();
    await route(
      ev('PUT', '/devices/me/push-token', credential, {
        platform: 'ios',
        token: APNS,
        language: 'en',
      }),
    );
    const res = await route(ev('POST', '/places', credential, SJ));
    expect(res.statusCode).toBe(201);
    const entries = await indexed(SJ_CELL);
    expect(entries.map((e) => e.deviceId)).toEqual([deviceId]);
    expect(entries[0]?.target).toEqual({ platform: 'ios', token: APNS });
    expect(await rows()).toEqual([]);

    const placeId = (JSON.parse(res.body) as { id: string }).id;
    expect((await route(ev('DELETE', `/places/${placeId}`, credential))).statusCode).toBe(204);
    expect(await indexed(SJ_CELL)).toEqual([]);
    expect(await rows()).toEqual([]);
  });

  it('a crash between the Aurora commit and the write-through leaves a row the drainer fixes (place add)', async () => {
    const { credential, deviceId } = await register();
    await route(
      ev('PUT', '/devices/me/push-token', credential, {
        platform: 'ios',
        token: APNS,
        language: 'es',
      }),
    );
    index.down = true; // the API survives but cannot write; a real crash is the same from Aurora's side
    const res = await route(ev('POST', '/places', credential, SJ));
    expect(res.statusCode).toBe(201);
    expect(await indexed(SJ_CELL)).toEqual([]); // device is NOT indexed: would miss alerts
    const left = await rows();
    expect(left).toHaveLength(1);
    expect(left[0]).toMatchObject({ device_id: deviceId, op: 'put', attempts: 0 });

    index.down = false;
    // Too young: the drainer leaves it to the API's own write-through.
    const young = await drainOutbox({
      ...repairDeps(),
      now: () => new Date(),
      log: () => {},
      metric: () => {},
    });
    expect(young.claimed).toBe(0);

    const result = await drain();
    expect(result).toMatchObject({ claimed: 1, applied: 1, failed: 0 });
    const entries = await indexed(SJ_CELL);
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      deviceId,
      language: 'es',
      target: { platform: 'ios', token: APNS },
      deviceCells: [SJ_CELL],
    });
    expect(await rows()).toEqual([]);
  });

  it('a process death before any write-through (row only) is also fixed by the drainer', async () => {
    const devices = new PostgresDeviceStore(db);
    const places = new PostgresPlaceStore(db);
    await devices.create(device());
    await devices.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
    const p = place();
    // What the API's transaction did before it died: place + outbox row committed, nothing else.
    await places.add(p, {
      outbox: { id: randomUUID(), op: 'put', h3Cell: p.h3Cell, placeId: p.id },
    });
    expect(await indexed(p.h3Cell)).toEqual([]);
    await drain();
    expect((await indexed(p.h3Cell)).map((e) => e.placeId)).toEqual([p.id]);
    expect(await rows()).toEqual([]);
  });

  it('a crash after a place delete leaves a remove row; the drainer removes the stale entry', async () => {
    const { credential } = await register();
    await route(
      ev('PUT', '/devices/me/push-token', credential, {
        platform: 'ios',
        token: APNS,
        language: 'en',
      }),
    );
    const a = JSON.parse((await route(ev('POST', '/places', credential, SJ))).body) as {
      id: string;
    };
    await route(ev('POST', '/places', credential, LA));
    index.down = true;
    expect((await route(ev('DELETE', `/places/${a.id}`, credential))).statusCode).toBe(204);
    index.down = false;
    expect(await indexed(SJ_CELL)).toHaveLength(1); // stale: would push for a deleted place
    expect((await rows()).map((r) => r.op)).toEqual(['remove']);
    await drain();
    expect(await indexed(SJ_CELL)).toEqual([]);
    expect(await indexed(cellForPoint(LA.latitude, LA.longitude))).toHaveLength(1);
    expect(await rows()).toEqual([]);
  });

  it('push-target changes write outbox rows for the device and any displaced device with places', async () => {
    const a = await register();
    const b = await register();
    const c = await register(); // no places: needs no index update
    const put = (credential: string, token = APNS) =>
      route(
        ev('PUT', '/devices/me/push-token', credential, { platform: 'ios', token, language: 'en' }),
      );
    await put(a.credential);
    await route(ev('POST', '/places', a.credential, SJ));
    await route(ev('POST', '/places', b.credential, LA));
    await put(c.credential, 'cd34'.repeat(16)); // c has a target but no places: no outbox row
    expect(await rows()).toEqual([]);

    // The index is down while b takes a's push token (a is displaced, b gains the target).
    index.down = true;
    expect((await put(b.credential)).statusCode).toBe(204);
    const left = await rows();
    expect(left.map((r) => r.device_id).sort()).toEqual([a.deviceId, b.deviceId].sort());
    expect(left.every((r) => r.op === 'update')).toBe(true);

    index.down = false;
    await drain();
    const aEntry = (await indexed(SJ_CELL))[0];
    const bEntry = (await indexed(cellForPoint(LA.latitude, LA.longitude)))[0];
    expect(aEntry?.target).toBeNull();
    expect(bEntry?.target).toEqual({ platform: 'ios', token: APNS });
    expect(await rows()).toEqual([]);
  });

  it('a row that keeps failing stays, counts attempts, and reports attempts >= 10 for the alarm', async () => {
    const { credential } = await register();
    index.down = true;
    await route(ev('POST', '/places', credential, SJ));
    const lines: string[] = [];
    const metrics: Array<Record<string, number>> = [];
    for (let i = 1; i <= 10; i++) {
      const r = await drainOutbox({
        ...repairDeps(),
        now: later,
        log: (l) => void lines.push(String(l['msg'])),
        metric: (m) => void metrics.push(JSON.parse(m) as Record<string, number>),
      });
      expect(r).toMatchObject({ claimed: 1, applied: 0, failed: 1, maxAttempts: i });
    }
    expect((await rows())[0]?.attempts).toBe(10);
    expect(metrics.map((m) => m['OutboxMaxAttempts'])).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    expect(metrics.at(-1)).toMatchObject({ OutboxPending: 1 });
    expect(
      lines.every((l) => l === 'cellindex.outbox_apply_failed' || l === 'cellindex.outbox_drain'),
    ).toBe(true);
    // Dynamo recovers: the next run fixes it and the metric drops to 0.
    index.down = false;
    await drain();
    expect(await rows()).toEqual([]);
    const final: Array<Record<string, number>> = [];
    await drainOutbox({
      ...repairDeps(),
      now: later,
      log: () => {},
      metric: (m) => void final.push(JSON.parse(m) as Record<string, number>),
    });
    expect(final[0]).toMatchObject({ OutboxMaxAttempts: 0, OutboxPending: 0 });
  });

  it('a device deleted before the drainer runs does not wedge the outbox', async () => {
    const p = place();
    await new PostgresDeviceStore(db).create(device());
    await new PostgresPlaceStore(db).add(p, {
      outbox: { id: randomUUID(), op: 'put', h3Cell: p.h3Cell, placeId: p.id },
    });
    await db.query('DELETE FROM devices WHERE id = $1::uuid', [DEVICE_ID]);
    const r = await drain();
    expect(r).toMatchObject({ applied: 1, failed: 0 });
    expect(await rows()).toEqual([]);
  });

  it('removing outbox rows by id ignores unknown ids and empty lists', async () => {
    await outbox.remove([]);
    await outbox.remove([randomUUID()]);
    expect(await rows()).toEqual([]);
  });
});
