import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createApi, type ApiDeps, type ApiEvent } from '../../src/api/index.js';
import type { DeviceErasure, DeviceStore } from '../../src/ports.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import {
  createCheckInToken,
  createCheckInTokenV1,
  createDeviceCredential,
} from '../../src/tokens/index.js';

const TOKEN_KEY = { id: 't1', secret: 'token-secret-one' };
const CRED_KEY = { id: 'c1', secret: 'credential-secret-one' };
const TOKEN_KEYS = new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]);
const CRED_KEYS = new Map([[CRED_KEY.id, CRED_KEY.secret]]);
const CELL = '872830828ffffff';
const EXPIRES = new Date('2026-11-09T18:00:00Z');
const NOW = new Date('2026-11-09T12:00:00Z');

/** A store that fails the test on ANY call: proves a path reads no database. */
function forbidden<T extends object>(name: string): T {
  return new Proxy({} as T, {
    get: (_t, prop) => () => {
      throw new Error(`${name}.${String(prop)} must not be called on this path`);
    },
  });
}

/** Counts calls to every method of a real store. */
function counting<T extends object>(store: T): { store: T; calls: string[] } {
  const calls: string[] = [];
  const proxy = new Proxy(store, {
    get(target, prop, receiver) {
      const v: unknown = Reflect.get(target, prop, receiver);
      if (typeof v !== 'function') return v;
      return (...args: unknown[]) => {
        calls.push(String(prop));
        return (v as (...a: unknown[]) => unknown).apply(target, args);
      };
    },
  });
  return { store: proxy, calls };
}

/**
 * No per-request database call, but the tombstone list (a cached read of erased devices, once per
 * cache window) is allowed: it is what keeps a deleted device's stateless credential out.
 */
function tombstonesOnly(): DeviceStore & DeviceErasure {
  return new Proxy(forbidden<DeviceStore & DeviceErasure>('devices'), {
    get: (target, prop, receiver) =>
      prop === 'listDeletedDevices' ? async () => [] : Reflect.get(target, prop, receiver),
  });
}

function stateless(over: Partial<ApiDeps> = {}, checkins = new MemoryCheckInStore()) {
  let now = NOW;
  const deps: ApiDeps = {
    devices: tombstonesOnly(),
    places: forbidden('places'),
    alerts: forbidden('alerts'),
    geocoder: forbidden('geocoder'),
    cellIndex: forbidden('cellIndex'),
    checkins,
    tokenKeys: TOKEN_KEYS,
    deviceCredentialKeys: CRED_KEYS,
    clock: { now: () => now },
    newId: randomUUID,
    ...over,
  };
  return {
    route: createApi(deps),
    checkins,
    setNow: (d: Date) => {
      now = d;
    },
  };
}

const post = (alertId: string, credential: string | undefined, body: unknown): ApiEvent => ({
  rawPath: `/alerts/${alertId}/checkins`,
  headers: credential ? { authorization: `Bearer ${credential}` } : {},
  body: JSON.stringify(body),
  requestContext: { http: { method: 'POST' } },
});
const body = (token: string, over: Record<string, unknown> = {}) => ({
  status: 'ok',
  token,
  idempotencyKey: 'key-1',
  ...over,
});
const codeOf = (res: { body: string }) => (JSON.parse(res.body) as { code: string }).code;

function newDevice(alertId: string, h3Cell = CELL) {
  const deviceId = randomUUID();
  return {
    deviceId,
    credential: createDeviceCredential(deviceId, CRED_KEY),
    token: createCheckInToken({ alertId, deviceId, h3Cell, expiresAt: EXPIRES }, TOKEN_KEY),
  };
}

describe('POST /alerts/{id}/checkins on the v2 path (stateless credential + v2 token)', () => {
  it('succeeds with NO database call: devices, places, alerts and geocoder all throw if touched', async () => {
    const alertId = randomUUID();
    const { route, checkins } = stateless();
    const d = newDevice(alertId);
    const res = await route(post(alertId, d.credential, body(d.token)));
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ checkedInAt: NOW.toISOString() });
    // The cell comes from the token, not from any place.
    expect(await checkins.get(alertId, d.deviceId)).toMatchObject({
      h3Cell: CELL,
      idempotencyKey: 'key-1',
      checkedInAt: NOW.toISOString(),
    });
    expect(checkins.total(alertId, CELL)).toBe(1);
  });

  it('does one conditional put for a first check-in and no read', async () => {
    const alertId = randomUUID();
    const c = counting(new MemoryCheckInStore());
    const { route } = stateless({}, c.store as MemoryCheckInStore);
    const d = newDevice(alertId);
    expect((await route(post(alertId, d.credential, body(d.token)))).statusCode).toBe(200);
    expect(c.calls).toEqual(['save']);
  });

  it('answers a repeat with the ORIGINAL checkedInAt, even with a new idempotency key', async () => {
    const alertId = randomUUID();
    const { route, setNow, checkins } = stateless();
    const d = newDevice(alertId);
    const first = await route(post(alertId, d.credential, body(d.token)));
    setNow(new Date('2026-11-09T12:03:00Z'));
    const retry = await route(post(alertId, d.credential, body(d.token)));
    const other = await route(post(alertId, d.credential, body(d.token, { idempotencyKey: 'k2' })));
    for (const res of [retry, other]) {
      expect(res.statusCode).toBe(200);
      expect(JSON.parse(res.body)).toEqual(JSON.parse(first.body));
    }
    expect(checkins.total(alertId, CELL)).toBe(1);
  });

  it('rejects an expired token (beyond the 60 s skew) with 403 invalid_token and writes nothing', async () => {
    const alertId = randomUUID();
    const { route, setNow, checkins } = stateless();
    const d = newDevice(alertId);
    setNow(new Date(EXPIRES.getTime() + 61_000));
    const res = await route(post(alertId, d.credential, body(d.token)));
    expect(res.statusCode).toBe(403);
    expect(codeOf(res)).toBe('invalid_token');
    expect(await checkins.get(alertId, d.deviceId)).toBeNull();
  });

  it('rejects a token for another alert, another device, or another key, all identically', async () => {
    const alertId = randomUUID();
    const { route } = stateless();
    const d = newDevice(alertId);
    const otherAlert = newDevice(randomUUID());
    const otherDevice = newDevice(alertId);
    const wrongKey = createCheckInToken(
      { alertId, deviceId: d.deviceId, h3Cell: CELL, expiresAt: EXPIRES },
      { id: 't1', secret: 'someone-elses-secret' },
    );
    const answers = new Set<string>();
    for (const token of [otherAlert.token, otherDevice.token, wrongKey, 'garbage']) {
      const res = await route(post(alertId, d.credential, body(token)));
      expect(res.statusCode).toBe(403);
      answers.add(res.body);
    }
    expect(answers.size).toBe(1);
  });

  it('rejects a token whose cell was changed (the cell is signed)', async () => {
    const alertId = randomUUID();
    const { route } = stateless();
    const d = newDevice(alertId);
    const [payload = '', sig = ''] = d.token.split('.');
    const claims = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')) as object;
    const moved = Buffer.from(JSON.stringify({ ...claims, c: '872830829ffffff' })).toString(
      'base64url',
    );
    const res = await route(post(alertId, d.credential, body(`${moved}.${sig}`)));
    expect(res.statusCode).toBe(403);
  });

  it('rejects bad credentials with 401, still with no database call', async () => {
    const alertId = randomUUID();
    const { route, checkins } = stateless();
    const d = newDevice(alertId);
    const [id = '', kid = '', mac = ''] = d.credential.split('.');
    const bad = [
      undefined,
      `${id}.${kid}.${mac.slice(0, -2)}AA`, // tampered mac
      `${id}.c9.${mac}`, // unknown kid
      `${randomUUID()}.${kid}.${mac}`, // another device's id on this mac
      `${id}.${kid}.`,
      createDeviceCredential(d.deviceId, { id: 'c1', secret: 'not-the-secret' }),
    ];
    for (const credential of bad) {
      const res = await route(post(alertId, credential, body(d.token)));
      expect(res.statusCode, String(credential)).toBe(401);
    }
    expect(await checkins.get(alertId, d.deviceId)).toBeNull();
  });

  it('rejects a three-part credential when no credential keys are configured', async () => {
    const alertId = randomUUID();
    const { route } = stateless({ deviceCredentialKeys: undefined });
    const d = newDevice(alertId);
    expect((await route(post(alertId, d.credential, body(d.token)))).statusCode).toBe(401);
  });

  it('validates the body before writing: need_help, missing token, long key, malformed alert id', async () => {
    const alertId = randomUUID();
    const { route } = stateless();
    const d = newDevice(alertId);
    const send = (b: unknown, id: string = alertId) => route(post(id, d.credential, b));
    expect((await send(body(d.token, { status: 'need_help' }))).statusCode).toBe(400);
    expect((await send({ status: 'ok', idempotencyKey: 'k' })).statusCode).toBe(400);
    expect((await send(body(d.token, { idempotencyKey: 'x'.repeat(201) }))).statusCode).toBe(400);
    expect((await send(body(d.token), 'not-a-uuid')).statusCode).toBe(404);
  });
});

describe('check-in credentials and tokens from before the v2 rollout', () => {
  async function legacySetup() {
    const devices = new MemoryDeviceStore();
    const places = new MemoryPlaceStore();
    const alertId = randomUUID();
    const alertCells = [CELL];
    const alerts = {
      get: async (id: string) => (id === alertId ? { id, area: { h3Cells: alertCells } } : null),
    };
    // Register through the API with no credential key configured: the old random credential.
    const reg = stateless({
      devices,
      places,
      alerts: alerts as unknown as ApiDeps['alerts'],
      deviceCredentialKey: undefined,
    });
    const created = await reg.route({
      rawPath: '/devices',
      headers: {},
      body: '{}',
      requestContext: { http: { method: 'POST' } },
    });
    const { deviceId, credential } = JSON.parse(created.body) as {
      deviceId: string;
      credential: string;
    };
    return { devices, places, alertId, alerts, deviceId, credential };
  }

  it('an old two-part credential is verified through the stored hash, and a v2 token needs no place or alert lookup', async () => {
    const s = await legacySetup();
    expect(s.credential.split('.')).toHaveLength(2);
    const dev = counting(s.devices);
    const { route, checkins } = stateless({ devices: dev.store });
    const token = createCheckInToken(
      { alertId: s.alertId, deviceId: s.deviceId, h3Cell: CELL, expiresAt: EXPIRES },
      TOKEN_KEY,
    );
    const res = await route(post(s.alertId, s.credential, body(token)));
    expect(res.statusCode).toBe(200);
    expect(dev.calls).toEqual(['get']); // the hash lookup only; places and alerts throw if touched
    expect(checkins.total(s.alertId, CELL)).toBe(1);
    // A wrong secret on the old form is still refused.
    const forged = `${s.deviceId}.${'A'.repeat(43)}`;
    expect((await route(post(s.alertId, forged, body(token)))).statusCode).toBe(401);
  });

  it('a v1 token falls back to the place lookup for its cell', async () => {
    const s = await legacySetup();
    await s.places.add({
      id: randomUUID(),
      deviceId: s.deviceId,
      label: 'Home',
      shortAddress: 'x',
      h3Cell: CELL,
      createdAt: NOW.toISOString(),
    });
    const { route, checkins } = stateless({
      devices: s.devices,
      places: s.places,
      alerts: s.alerts as unknown as ApiDeps['alerts'],
    });
    const v1 = createCheckInTokenV1(
      { alertId: s.alertId, deviceId: s.deviceId, expiresAt: EXPIRES },
      TOKEN_KEY,
    );
    const res = await route(post(s.alertId, s.credential, body(v1)));
    expect(res.statusCode).toBe(200);
    expect((await checkins.get(s.alertId, s.deviceId))?.h3Cell).toBe(CELL);
    // v1 still hits the alert lookup (404 for an unknown alert) and the area check (403).
    const unknown = randomUUID();
    const v1Unknown = createCheckInTokenV1(
      { alertId: unknown, deviceId: s.deviceId, expiresAt: EXPIRES },
      TOKEN_KEY,
    );
    expect((await route(post(unknown, s.credential, body(v1Unknown)))).statusCode).toBe(404);
  });

  it('a v1 token is rejected with 403 not_in_area when the device has no place in the alert', async () => {
    const s = await legacySetup();
    const { route } = stateless({
      devices: s.devices,
      places: s.places,
      alerts: s.alerts as unknown as ApiDeps['alerts'],
    });
    const v1 = createCheckInTokenV1(
      { alertId: s.alertId, deviceId: s.deviceId, expiresAt: EXPIRES },
      TOKEN_KEY,
    );
    const res = await route(post(s.alertId, s.credential, body(v1)));
    expect(res.statusCode).toBe(403);
    expect(codeOf(res)).toBe('not_in_area');
  });

  it('POST /devices issues a stateless credential when a credential key is configured, and it checks in with no device lookup', async () => {
    const devices = new MemoryDeviceStore();
    const { route, checkins } = stateless({ devices, deviceCredentialKey: CRED_KEY });
    const created = await route({
      rawPath: '/devices',
      headers: {},
      body: '{}',
      requestContext: { http: { method: 'POST' } },
    });
    const { deviceId, credential } = JSON.parse(created.body) as {
      deviceId: string;
      credential: string;
    };
    expect(credential).toBe(createDeviceCredential(deviceId, CRED_KEY));
    // The hash is still stored, so the other routes keep their database lookup.
    expect(await devices.get(deviceId)).not.toBeNull();

    const alertId = randomUUID();
    const token = createCheckInToken(
      { alertId, deviceId, h3Cell: CELL, expiresAt: EXPIRES },
      TOKEN_KEY,
    );
    // A route with a throwing device store proves the check-in needs no lookup.
    const strict = stateless({}, checkins);
    expect((await strict.route(post(alertId, credential, body(token)))).statusCode).toBe(200);
  });
});
