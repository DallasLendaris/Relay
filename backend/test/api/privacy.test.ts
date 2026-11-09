import { createHash, randomUUID } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { createApi, type ApiDeps, type ApiEvent } from '../../src/api/index.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import { cellForPoint } from '../../src/geo/index.js';
import type { AlertStore, Geocoder } from '../../src/ports.js';
import { createCheckInToken, createDeviceCredential } from '../../src/tokens/index.js';

const TOKEN_KEY = { id: 't1', secret: 'token-secret-one' };
const CRED_KEY = { id: 'c1', secret: 'credential-secret-one' };
const NOW = new Date('2026-11-09T12:00:00Z');
const ALERT_END = new Date('2026-11-09T18:00:00Z');
const DAY = 24 * 3600;
const SJ = { lat: 37.3352, lng: -121.8811 };
const SF = { lat: 37.7749, lng: -122.4194 };
const SJ_CELL = cellForPoint(SJ.lat, SJ.lng);
const SF_CELL = cellForPoint(SF.lat, SF.lng);

const geocoder: Geocoder = {
  async geocode(address) {
    if (address === '1 Main St, San Jose') {
      return {
        latitude: SJ.lat,
        longitude: SJ.lng,
        shortAddress: 'Main St, San Jose',
        state: 'CA',
      };
    }
    if (address === '1 Broadway, New York') {
      return { latitude: 40.7, longitude: -74, shortAddress: 'Broadway, New York', state: 'NY' };
    }
    return null;
  },
};

function setup(over: Partial<ApiDeps> = {}) {
  const devices = new MemoryDeviceStore();
  const places = new MemoryPlaceStore();
  const checkins = new MemoryCheckInStore();
  const cellIndex = new MemoryCellIndex();
  let now = NOW;
  const deps: ApiDeps = {
    devices,
    places,
    alerts: {} as AlertStore,
    geocoder,
    checkins,
    cellIndex,
    tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
    deviceCredentialKeys: new Map([[CRED_KEY.id, CRED_KEY.secret]]),
    deviceCredentialKey: CRED_KEY,
    clock: { now: () => now },
    newId: randomUUID,
    ...over,
  };
  return {
    deps,
    devices,
    places,
    checkins,
    cellIndex,
    route: createApi(deps),
    setNow: (d: Date) => {
      now = d;
    },
  };
}
type Ctx = ReturnType<typeof setup>;

const call = (
  method: string,
  path: string,
  credential: string | undefined,
  body?: unknown,
): ApiEvent => ({
  rawPath: path,
  headers: credential ? { authorization: `Bearer ${credential}` } : {},
  ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  requestContext: { http: { method } },
});
const parse = (res: { body: string }) => JSON.parse(res.body) as Record<string, unknown>;

async function register(c: Ctx) {
  const res = await c.route(call('POST', '/devices', undefined, {}));
  expect(res.statusCode).toBe(201);
  const { deviceId, credential } = parse(res) as { deviceId: string; credential: string };
  return { deviceId, credential };
}

async function addPlace(c: Ctx, credential: string, body: Record<string, unknown>) {
  const res = await c.route(call('POST', '/places', credential, body));
  expect(res.statusCode, res.body).toBe(201);
  return parse(res) as { id: string; h3Cell: string; label: string; shortAddress?: string };
}

const checkInBody = (token: string, key = 'k1') => ({ status: 'ok', token, idempotencyKey: key });
const tokenFor = (alertId: string, deviceId: string, h3Cell: string) =>
  createCheckInToken({ alertId, deviceId, h3Cell, expiresAt: ALERT_END }, TOKEN_KEY);

async function checkIn(c: Ctx, alertId: string, d: { deviceId: string; credential: string }) {
  return c.route(
    call(
      'POST',
      `/alerts/${alertId}/checkins`,
      d.credential,
      checkInBody(tokenFor(alertId, d.deviceId, SJ_CELL)),
    ),
  );
}

describe('DELETE /devices/me', () => {
  async function populated(c: Ctx) {
    const d = await register(c);
    const home = await addPlace(c, d.credential, {
      label: 'Home',
      latitude: SJ.lat,
      longitude: SJ.lng,
    });
    const work = await addPlace(c, d.credential, { label: 'Work', address: '1 Main St, San Jose' });
    const a1 = randomUUID();
    const a2 = randomUUID();
    expect((await checkIn(c, a1, d)).statusCode).toBe(200);
    expect((await checkIn(c, a2, d)).statusCode).toBe(200);
    return { ...d, home, work, alerts: [a1, a2] };
  }

  it('erases the cell index entries, places, device row and check-ins, and answers 204', async () => {
    const c = setup();
    const other = await register(c);
    await addPlace(c, other.credential, { label: 'Home', latitude: SJ.lat, longitude: SJ.lng });
    const otherAlert = randomUUID();
    await checkIn(c, otherAlert, other);
    const d = await populated(c);
    expect((await c.cellIndex.listForCell(SJ_CELL)).entries.map((e) => e.deviceId)).toContain(
      d.deviceId,
    );

    const res = await c.route(call('DELETE', '/devices/me', d.credential));
    expect(res.statusCode).toBe(204);
    expect(res.body).toBe('');

    // Cell index: no entry of this device anywhere; the other device is untouched.
    const entries = (await c.cellIndex.listForCell(SJ_CELL)).entries;
    expect(entries.map((e) => e.deviceId)).toEqual([other.deviceId]);
    // Places and device row are gone.
    expect(await c.places.listForDevice(d.deviceId)).toEqual([]);
    expect(await c.devices.get(d.deviceId)).toBeNull();
    expect(await c.places.listForDevice(other.deviceId)).toHaveLength(1);
    // Check-ins are gone (and only this device's).
    for (const a of d.alerts) expect(await c.checkins.get(a, d.deviceId)).toBeNull();
    expect(await c.checkins.get(otherAlert, other.deviceId)).not.toBeNull();
    // A tombstone was recorded.
    expect(await c.devices.listDeletedDevices()).toEqual([d.deviceId]);
  });

  it('after the erasure a second delete is an idempotent 204 and every other route is 401', async () => {
    const c = setup();
    const d = await populated(c);
    expect((await c.route(call('DELETE', '/devices/me', d.credential))).statusCode).toBe(204);
    // Same credential after the erasure finished: the device is gone, nothing to finish.
    // (Tombstoned stateless credential: accepted only to finish a partial erasure, so it is
    // idempotent and still answers 204 without bringing anything back.)
    expect((await c.route(call('DELETE', '/devices/me', d.credential))).statusCode).toBe(204);
    expect((await c.route(call('GET', '/places', d.credential))).statusCode).toBe(401);
    expect(await c.devices.get(d.deviceId)).toBeNull();
  });

  it('requires device authentication', async () => {
    const c = setup();
    expect((await c.route(call('DELETE', '/devices/me', undefined))).statusCode).toBe(401);
    const forged = createDeviceCredential(randomUUID(), { id: 'c1', secret: 'wrong' });
    expect((await c.route(call('DELETE', '/devices/me', forged))).statusCode).toBe(401);
    // A valid stateless credential for a device that never existed is not tombstoned: 401.
    const ghost = createDeviceCredential(randomUUID(), CRED_KEY);
    expect((await c.route(call('DELETE', '/devices/me', ghost))).statusCode).toBe(401);
  });

  it("a deleted device's stateless credential can no longer check in (same container, at once)", async () => {
    const c = setup();
    const d = await populated(c);
    const alertId = randomUUID();
    expect((await checkIn(c, alertId, d)).statusCode).toBe(200);
    await c.route(call('DELETE', '/devices/me', d.credential));
    const after = await checkIn(c, randomUUID(), d);
    expect(after.statusCode).toBe(401);
    expect(parse(after)['code']).toBe('unauthorized');
    expect(await c.checkins.deleteForDevice(d.deviceId)).toBe(0); // nothing was written
  });

  it("a deleted device's credential is refused by ANOTHER container once its tombstone cache refreshes", async () => {
    const c = setup({ tombstoneCacheMs: 60_000 });
    const d = await populated(c);
    // A second container (its own API instance and cache) that already served a check-in.
    const second = createApi({ ...c.deps, clock: { now: () => c.deps.clock.now() } });
    const alertId = randomUUID();
    const ok = await second(
      call(
        'POST',
        `/alerts/${alertId}/checkins`,
        d.credential,
        checkInBody(tokenFor(alertId, d.deviceId, SJ_CELL)),
      ),
    );
    expect(ok.statusCode).toBe(200);

    await c.route(call('DELETE', '/devices/me', d.credential));
    const again = (a: string) =>
      second(
        call(
          'POST',
          `/alerts/${a}/checkins`,
          d.credential,
          checkInBody(tokenFor(a, d.deviceId, SJ_CELL), 'k2'),
        ),
      );
    // Documented trade-off: inside the cache window the other container still accepts it...
    expect((await again(randomUUID())).statusCode).toBe(200);
    // ...and after the window it refuses.
    c.setNow(new Date(NOW.getTime() + 61_000));
    expect((await again(randomUUID())).statusCode).toBe(401);
  });

  it('reads the tombstones once per cache window, not once per check-in', async () => {
    const c = setup({ tombstoneCacheMs: 60_000 });
    const list = vi.spyOn(c.devices, 'listDeletedDevices');
    const d1 = await register(c);
    const d2 = await register(c);
    for (const d of [d1, d2]) {
      const a = randomUUID();
      expect((await checkIn(c, a, d)).statusCode).toBe(200);
    }
    expect(list).toHaveBeenCalledTimes(1);
    c.setNow(new Date(NOW.getTime() + 61_000));
    expect((await checkIn(c, randomUUID(), d1)).statusCode).toBe(200);
    expect(list).toHaveBeenCalledTimes(2);
  });

  it('fails open when the tombstone read fails (an outage must not block "I\'m okay")', async () => {
    const c = setup();
    vi.spyOn(c.devices, 'listDeletedDevices').mockRejectedValue(new Error('db down'));
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    const d = await register(c);
    expect((await checkIn(c, randomUUID(), d)).statusCode).toBe(200);
    const logged = warn.mock.calls.map((x) => String(x[0])).join('\n');
    expect(logged).toContain('tombstone_refresh_failed');
    expect(logged).not.toContain('db down');
    warn.mockRestore();
  });

  it('removes cell index entries FIRST: if that fails nothing else is deleted and the client retries', async () => {
    const c = setup();
    const d = await populated(c);
    const remove = vi.spyOn(c.cellIndex, 'remove').mockRejectedValueOnce(new Error('ddb down'));
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const res = await c.route(call('DELETE', '/devices/me', d.credential));
    expect(res.statusCode).toBe(500);
    // The device, its places and its check-ins are all still there; no tombstone yet.
    expect(await c.devices.get(d.deviceId)).not.toBeNull();
    expect(await c.places.listForDevice(d.deviceId)).toHaveLength(2);
    expect(await c.checkins.get(d.alerts[0] ?? '', d.deviceId)).not.toBeNull();
    expect(await c.devices.listDeletedDevices()).toEqual([]);
    // The retry works and leaves no entry behind.
    remove.mockRestore();
    expect((await c.route(call('DELETE', '/devices/me', d.credential))).statusCode).toBe(204);
    expect((await c.cellIndex.listForCell(SJ_CELL)).entries).toEqual([]);
    vi.restoreAllMocks();
  });

  it('never leaves an index entry pointing at a deleted device, at any step', async () => {
    // Record the order of the calls: every cellIndex.remove precedes deviceStore.deleteDevice.
    const c = setup();
    const d = await populated(c);
    const order: string[] = [];
    const rm = c.cellIndex.remove.bind(c.cellIndex);
    c.cellIndex.remove = async (...a) => {
      order.push('index');
      return rm(...a);
    };
    const del = c.devices.deleteDevice.bind(c.devices);
    c.devices.deleteDevice = async (...a) => {
      order.push('device');
      return del(...a);
    };
    const purge = c.checkins.deleteForDevice.bind(c.checkins);
    c.checkins.deleteForDevice = async (...a) => {
      order.push('checkins');
      return purge(...a);
    };
    await c.route(call('DELETE', '/devices/me', d.credential));
    expect(order).toEqual(['index', 'index', 'device', 'checkins']);
  });

  it('retries the check-in purge inline, and a persistent failure is finished by the client retry', async () => {
    const c = setup();
    const d = await populated(c);
    const purge = vi.spyOn(c.checkins, 'deleteForDevice').mockRejectedValue(new Error('boom'));
    const err = vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const res = await c.route(call('DELETE', '/devices/me', d.credential));
    expect(res.statusCode).toBe(500);
    expect(purge).toHaveBeenCalledTimes(3); // inline retries
    // The device row is already gone and tombstoned; its check-ins are still there.
    expect(await c.devices.get(d.deviceId)).toBeNull();
    expect(await c.checkins.get(d.alerts[0] ?? '', d.deviceId)).not.toBeNull();
    const logged = err.mock.calls.map((x) => String(x[0])).join('\n');
    expect(logged).toContain('checkins_incomplete');
    expect(logged).not.toContain('boom');

    // The client retries with the same credential: the tombstone lets it finish the purge.
    purge.mockRestore();
    expect((await c.route(call('DELETE', '/devices/me', d.credential))).statusCode).toBe(204);
    for (const a of d.alerts) expect(await c.checkins.get(a, d.deviceId)).toBeNull();
    vi.restoreAllMocks();
  });

  it('a transient purge failure is retried inline and the request still answers 204', async () => {
    const c = setup();
    const d = await populated(c);
    const purge = vi
      .spyOn(c.checkins, 'deleteForDevice')
      .mockRejectedValueOnce(new Error('throttled'));
    const res = await c.route(call('DELETE', '/devices/me', d.credential));
    expect(res.statusCode).toBe(204);
    expect(purge).toHaveBeenCalledTimes(2);
    for (const a of d.alerts) expect(await c.checkins.get(a, d.deviceId)).toBeNull();
    vi.restoreAllMocks();
  });

  it('erases a legacy (hash) credential device too', async () => {
    const c = setup();
    const id = randomUUID();
    const credential = `${id}.legacy-secret`;
    await c.devices.create({
      id,
      credentialHash: createHash('sha256').update(credential).digest('hex'),
      expoPushToken: null,
      language: 'en',
      createdAt: NOW.toISOString(),
    });
    expect((await c.route(call('DELETE', '/devices/me', credential))).statusCode).toBe(204);
    expect(await c.devices.get(id)).toBeNull();
    // Its credential cannot authenticate again (no device row, and no stateless fallback).
    expect((await c.route(call('DELETE', '/devices/me', credential))).statusCode).toBe(401);
  });
});

describe('POST /places: data minimization', () => {
  it('a place added by coordinates keeps only the cell: no coordinates, no address', async () => {
    const c = setup();
    const d = await register(c);
    const body = await addPlace(c, d.credential, {
      label: 'Home',
      latitude: SJ.lat,
      longitude: SJ.lng,
    });
    expect(body.h3Cell).toBe(SJ_CELL);
    expect(body).not.toHaveProperty('shortAddress');
    const stored = (await c.places.listForDevice(d.deviceId))[0];
    expect(stored).toBeDefined();
    expect(Object.keys(stored ?? {}).sort()).toEqual(
      ['createdAt', 'deviceId', 'h3Cell', 'id', 'label'].sort(),
    );
    // The exact point appears nowhere in what was stored or answered.
    const everything = JSON.stringify([
      stored,
      body,
      await c.route(call('GET', '/places', d.credential)),
    ]);
    expect(everything).not.toContain(String(SJ.lat));
    expect(everything).not.toContain(String(SJ.lng));
    expect(everything).not.toContain('Dropped pin');
  });

  it('a place added by address keeps the short street text but never the coordinates', async () => {
    const c = setup();
    const d = await register(c);
    const body = await addPlace(c, d.credential, { label: 'Work', address: '1 Main St, San Jose' });
    expect(body.shortAddress).toBe('Main St, San Jose');
    expect(body.h3Cell).toBe(SJ_CELL);
    const stored = (await c.places.listForDevice(d.deviceId))[0];
    expect(stored).toMatchObject({ shortAddress: 'Main St, San Jose', h3Cell: SJ_CELL });
    expect(stored).not.toHaveProperty('latitude');
    expect(stored).not.toHaveProperty('longitude');
    expect(JSON.stringify(stored)).not.toContain(String(SJ.lat));
  });

  it('still checks California before the exact point is discarded', async () => {
    const c = setup();
    const d = await register(c);
    const pin = await c.route(
      call('POST', '/places', d.credential, { label: 'x', latitude: 40.7, longitude: -74 }),
    );
    expect(pin.statusCode).toBe(422);
    const addr = await c.route(
      call('POST', '/places', d.credential, { label: 'x', address: '1 Broadway, New York' }),
    );
    expect(addr.statusCode).toBe(422);
    expect(await c.places.listForDevice(d.deviceId)).toEqual([]);
  });

  it('caps the label at 60 characters (60 is accepted, 61 is a 400)', async () => {
    const c = setup();
    const d = await register(c);
    const ok = await c.route(
      call('POST', '/places', d.credential, {
        label: 'a'.repeat(60),
        latitude: SF.lat,
        longitude: SF.lng,
      }),
    );
    expect(ok.statusCode).toBe(201);
    expect(parse(ok)['h3Cell']).toBe(SF_CELL);
    const long = await c.route(
      call('POST', '/places', d.credential, {
        label: 'a'.repeat(61),
        latitude: SF.lat,
        longitude: SF.lng,
      }),
    );
    expect(long.statusCode).toBe(400);
    expect(parse(long)['code']).toBe('invalid_request');
  });
});

describe('check-in retention (TTL)', () => {
  it('writes ttl = the alert end + 30 days by default', async () => {
    const c = setup();
    const d = await register(c);
    const alertId = randomUUID();
    expect((await checkIn(c, alertId, d)).statusCode).toBe(200);
    expect(c.checkins.ttlOf(alertId, d.deviceId)).toBe(ALERT_END.getTime() / 1000 + 30 * DAY);
  });

  it('retention is configurable', async () => {
    const c = setup({ checkInRetentionDays: 7 });
    const d = await register(c);
    const alertId = randomUUID();
    await checkIn(c, alertId, d);
    expect(c.checkins.ttlOf(alertId, d.deviceId)).toBe(ALERT_END.getTime() / 1000 + 7 * DAY);
  });

  it('a repeat check-in leaves the first one’s ttl alone', async () => {
    const c = setup();
    const d = await register(c);
    const alertId = randomUUID();
    await checkIn(c, alertId, d);
    const first = c.checkins.ttlOf(alertId, d.deviceId);
    await checkIn(c, alertId, d);
    expect(c.checkins.ttlOf(alertId, d.deviceId)).toBe(first);
  });
});
