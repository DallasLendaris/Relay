import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../src/adapters/postgres/place-store.js';
import { createApi, type ApiEvent } from '../../src/api/index.js';
import { freshDb } from './helpers.js';

// The API on the real Postgres stores: the SQL casts that memory stores never exercise.
let route: ReturnType<typeof createApi>;
beforeEach(async () => {
  const db = await freshDb();
  route = createApi({
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    alerts: new PostgresAlertStore(db),
    geocoder: { geocode: async () => null },
    checkins: new MemoryCheckInStore(),
    tokenKeys: new Map(),
    clock: { now: () => new Date('2026-11-09T12:00:00Z') },
    newId: randomUUID,
  });
});

const ev = (method: string, path: string, credential?: string): ApiEvent => ({
  rawPath: path,
  headers: credential ? { authorization: `Bearer ${credential}` } : {},
  requestContext: { http: { method } },
});

const register = async () =>
  JSON.parse((await route(ev('POST', '/devices'))).body) as { credential: string };

describe('API on Postgres stores', () => {
  it('returns 401 (not 500) for a non-UUID device id in the credential', async () => {
    expect((await route(ev('GET', '/places', 'not-a-uuid.secret'))).statusCode).toBe(401);
  });

  it('returns 400 (not 500) for malformed percent-encoding in a path', async () => {
    const { credential } = await register();
    expect((await route(ev('GET', '/alerts/%E0%A4%A', credential))).statusCode).toBe(400);
  });

  it('accepts the same push token from a reinstalled device', async () => {
    const a = await register();
    const b = await register();
    const put = (credential: string) =>
      route({
        ...ev('PUT', '/devices/me/push-token', credential),
        body: JSON.stringify({ expoPushToken: 'ExponentPushToken[x]', language: 'en' }),
      });
    expect((await put(a.credential)).statusCode).toBe(204);
    expect((await put(b.credential)).statusCode).toBe(204);
  });
});
