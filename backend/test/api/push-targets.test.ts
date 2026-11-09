import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import SwaggerParser from '@apidevtools/swagger-parser';
import { Ajv2020 } from 'ajv/dist/2020.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { createApi, type ApiEvent } from '../../src/api/index.js';
import type { AlertStore } from '../../src/ports.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import type { CellIndex, CellIndexEntry } from '../../src/scale/contracts.js';

const SPEC_PATH = fileURLToPath(
  new URL('../../../packages/shared/openapi/relay.yaml', import.meta.url),
);

const APNS = 'ab12'.repeat(16); // 64 hex
const FCM = 'dXk3_Z:' + 'a-B9_'.repeat(30); // 157 chars
const EXPO = 'ExponentPushToken[abc123]';

let devices: MemoryDeviceStore;
let places: MemoryPlaceStore;
let index: CellIndex;
let route: ReturnType<typeof createApi>;
let logs: string[];

function build(cellIndex: CellIndex) {
  index = cellIndex;
  route = createApi({
    devices,
    places,
    alerts: { get: async () => null } as unknown as AlertStore,
    geocoder: { geocode: async () => null },
    checkins: new MemoryCheckInStore(),
    cellIndex,
    tokenKeys: new Map(),
    clock: { now: () => new Date('2026-11-09T12:00:00Z') },
    newId: randomUUID,
  });
}

beforeEach(() => {
  logs = [];
  vi.spyOn(console, 'warn').mockImplementation((...a: unknown[]) => void logs.push(a.join(' ')));
  vi.spyOn(console, 'error').mockImplementation((...a: unknown[]) => void logs.push(a.join(' ')));
  devices = new MemoryDeviceStore();
  places = new MemoryPlaceStore();
  build(new MemoryCellIndex());
});
afterEach(() => vi.restoreAllMocks());

async function call(method: string, path: string, body?: unknown, credential?: string) {
  const event: ApiEvent = {
    rawPath: path,
    headers: credential ? { authorization: `Bearer ${credential}` } : {},
    body: body === undefined ? null : JSON.stringify(body),
    requestContext: { http: { method } },
  };
  const res = await route(event);
  return {
    status: res.statusCode,
    body: res.body ? (JSON.parse(res.body) as Record<string, any>) : null,
  };
}

async function register() {
  const res = await call('POST', '/devices', {});
  return res.body as { deviceId: string; credential: string };
}

const SJ = { label: 'Home', latitude: 37.3352, longitude: -121.8811 };
const SF = { label: 'Work', latitude: 37.7749, longitude: -122.4194 };
const addPlace = async (credential: string, body: object = SJ) =>
  (await call('POST', '/places', body, credential)).body as { id: string; h3Cell: string };

async function cell(h3Cell: string): Promise<CellIndexEntry[]> {
  return (await index.listForCell(h3Cell)).entries;
}

describe('PUT /devices/me/push-token shapes', () => {
  it('accepts the old Expo body and keeps expoPushToken and pushToken in step', async () => {
    const { deviceId, credential } = await register();
    const res = await call(
      'PUT',
      '/devices/me/push-token',
      { expoPushToken: EXPO, language: 'es' },
      credential,
    );
    expect(res.status).toBe(204);
    expect(await devices.get(deviceId)).toMatchObject({
      expoPushToken: EXPO,
      pushPlatform: 'expo',
      pushToken: EXPO,
      language: 'es',
    });
  });

  it('accepts {platform: expo, token} and sets expoPushToken too', async () => {
    const { deviceId, credential } = await register();
    await call(
      'PUT',
      '/devices/me/push-token',
      { platform: 'expo', token: EXPO, language: 'en' },
      credential,
    );
    expect(await devices.get(deviceId)).toMatchObject({ expoPushToken: EXPO, pushToken: EXPO });
  });

  it('stores an APNs token (lowercased) with no Expo token', async () => {
    const { deviceId, credential } = await register();
    const res = await call(
      'PUT',
      '/devices/me/push-token',
      { platform: 'ios', token: APNS.toUpperCase(), language: 'en' },
      credential,
    );
    expect(res.status).toBe(204);
    expect(await devices.get(deviceId)).toMatchObject({
      pushPlatform: 'ios',
      pushToken: APNS,
      expoPushToken: null,
    });
  });

  it('stores an FCM token', async () => {
    const { deviceId, credential } = await register();
    await call(
      'PUT',
      '/devices/me/push-token',
      { platform: 'android', token: FCM, language: 'es' },
      credential,
    );
    expect(await devices.get(deviceId)).toMatchObject({
      pushPlatform: 'android',
      pushToken: FCM,
      expoPushToken: null,
      language: 'es',
    });
  });

  it('validates every shape', async () => {
    const { credential } = await register();
    const put = (b: unknown) => call('PUT', '/devices/me/push-token', b, credential);
    const lang = { language: 'en' };
    const bad: unknown[] = [
      { ...lang, platform: 'ios', token: 'a'.repeat(63) }, // too short
      { ...lang, platform: 'ios', token: 'a'.repeat(201) }, // too long
      { ...lang, platform: 'ios', token: 'g'.repeat(64) }, // not hex
      { ...lang, platform: 'android', token: 'a'.repeat(99) }, // too short
      { ...lang, platform: 'android', token: 'a'.repeat(4097) }, // too long
      { ...lang, platform: 'android', token: 'a'.repeat(99) + ' ' }, // bad character
      { ...lang, platform: 'expo', token: 'nope' },
      { ...lang, platform: 'windows', token: APNS },
      { ...lang, platform: 'ios' }, // no token
      { ...lang, platform: 'ios', token: 42 },
      { ...lang, token: APNS }, // no platform
      { language: 'en' }, // neither shape
      { ...lang, expoPushToken: EXPO, platform: 'ios', token: APNS }, // both shapes
      { platform: 'ios', token: APNS, language: 'fr' },
      { platform: 'ios', token: APNS },
      { expoPushToken: 'nope', language: 'en' },
    ];
    for (const body of bad) {
      expect((await put(body)).status, JSON.stringify(body)).toBe(400);
    }
    expect((await put({ ...lang, platform: 'android', token: 'a'.repeat(100) })).status).toBe(204);
    expect((await put({ ...lang, platform: 'android', token: 'a'.repeat(4096) })).status).toBe(204);
    expect((await put({ ...lang, platform: 'ios', token: 'A'.repeat(200) })).status).toBe(204);
  });

  it('matches the OpenAPI oneOf', async () => {
    const spec = (await SwaggerParser.dereference(SPEC_PATH)) as unknown as {
      components: { schemas: { PushTokenRequest: object } };
    };
    const validate = new Ajv2020({ strict: false }).compile(
      spec.components.schemas.PushTokenRequest,
    );
    expect(validate({ expoPushToken: EXPO, language: 'en' })).toBe(true);
    expect(validate({ platform: 'ios', token: APNS, language: 'es' })).toBe(true);
    expect(validate({ platform: 'ios', language: 'es' })).toBe(false);
    expect(validate({ expoPushToken: EXPO, platform: 'ios', token: APNS, language: 'en' })).toBe(
      false,
    );
  });

  it('moves an APNs token off the previous device', async () => {
    const first = await register();
    const second = await register();
    const body = { platform: 'ios', token: APNS, language: 'en' };
    await call('PUT', '/devices/me/push-token', body, first.credential);
    await call('PUT', '/devices/me/push-token', body, second.credential);
    expect((await devices.get(first.deviceId))?.pushToken).toBeNull();
    expect((await devices.get(second.deviceId))?.pushToken).toBe(APNS);
  });
});

describe('cell index write-through', () => {
  it('puts an entry when a place is added, with the device target and language', async () => {
    const { deviceId, credential } = await register();
    await call(
      'PUT',
      '/devices/me/push-token',
      { platform: 'ios', token: APNS, language: 'es' },
      credential,
    );
    const place = await addPlace(credential);
    expect(await cell(place.h3Cell)).toEqual([
      {
        h3Cell: place.h3Cell,
        deviceId,
        placeId: place.id,
        target: { platform: 'ios', token: APNS },
        language: 'es',
      },
    ]);
  });

  it('indexes a place added before any push token with a null target', async () => {
    const { credential } = await register();
    const place = await addPlace(credential);
    expect((await cell(place.h3Cell))[0]?.target).toBeNull();
  });

  it('removes the entry when a place is deleted, and only that one', async () => {
    const { credential } = await register();
    const a = await addPlace(credential, SJ);
    const b = await addPlace(credential, SF);
    expect((await call('DELETE', `/places/${a.id}`, undefined, credential)).status).toBe(204);
    expect(await cell(a.h3Cell)).toEqual([]);
    expect(await cell(b.h3Cell)).toHaveLength(1);
  });

  it('does not touch the index when the delete is a 404', async () => {
    const { credential } = await register();
    const other = await register();
    const place = await addPlace(other.credential);
    expect((await call('DELETE', `/places/${place.id}`, undefined, credential)).status).toBe(404);
    expect(await cell(place.h3Cell)).toHaveLength(1);
  });

  it('updates every cell of the device when its push target changes', async () => {
    const { credential } = await register();
    const a = await addPlace(credential, SJ);
    const b = await addPlace(credential, SF);
    await call(
      'PUT',
      '/devices/me/push-token',
      { platform: 'android', token: FCM, language: 'es' },
      credential,
    );
    for (const p of [a, b]) {
      expect((await cell(p.h3Cell))[0]).toMatchObject({
        target: { platform: 'android', token: FCM },
        language: 'es',
      });
    }
    await call(
      'PUT',
      '/devices/me/push-token',
      { expoPushToken: EXPO, language: 'en' },
      credential,
    );
    expect((await cell(a.h3Cell))[0]).toMatchObject({
      target: { platform: 'expo', token: EXPO },
      language: 'en',
    });
  });

  it('nulls the target on the device that lost a moved token', async () => {
    const first = await register();
    const second = await register();
    const body = { platform: 'ios', token: APNS, language: 'en' };
    const place = await addPlace(first.credential);
    await call('PUT', '/devices/me/push-token', body, first.credential);
    expect((await cell(place.h3Cell))[0]?.target).toEqual({ platform: 'ios', token: APNS });
    await call('PUT', '/devices/me/push-token', body, second.credential);
    expect((await cell(place.h3Cell))[0]?.target).toBeNull();
  });

  it('succeeds, logs cellindex.write_failed and enqueues nothing when the index is down', async () => {
    const failing: CellIndex = {
      put: () => Promise.reject(new Error(`boom ${APNS}`)),
      remove: () => Promise.reject(new Error('boom')),
      updateDevice: () => Promise.reject(new Error('boom')),
      listForCell: () => Promise.reject(new Error('boom')),
    };
    build(failing);
    const { credential } = await register();
    expect(
      (
        await call(
          'PUT',
          '/devices/me/push-token',
          { platform: 'ios', token: APNS, language: 'en' },
          credential,
        )
      ).status,
    ).toBe(204);
    const created = await call('POST', '/places', SJ, credential);
    expect(created.status).toBe(201);
    // The push-token call had no places yet, so the next two exercise update and remove.
    expect(
      (
        await call(
          'PUT',
          '/devices/me/push-token',
          { platform: 'ios', token: 'cd'.repeat(32), language: 'en' },
          credential,
        )
      ).status,
    ).toBe(204);
    expect(
      (await call('DELETE', `/places/${created.body?.['id']}`, undefined, credential)).status,
    ).toBe(204);

    const failed = logs
      .map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((l) => l['msg'] === 'cellindex.write_failed');
    expect(failed.map((l) => l['op']).sort()).toEqual(['put', 'remove', 'update_device']);
    // No tokens, no coordinates.
    const all = logs.join('\n');
    expect(all).not.toContain(APNS);
    expect(all).not.toContain('37.33');
    // Aurora (the stores) still has the truth.
    expect((await devices.get((await register()).deviceId))?.language).toBe('en');
  });
});
