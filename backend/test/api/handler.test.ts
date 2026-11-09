import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ApiEvent } from '../../src/api/index.js';
import { awsStores, createHandler } from '../../src/api/handler.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import type { AlertStore, Geocoder } from '../../src/ports.js';

const LAT = 37.123456;
const LNG = -121.654321;
const geocoder: Geocoder = {
  geocode: async () => ({
    latitude: LAT,
    longitude: LNG,
    shortAddress: '1 A St, San Jose',
    state: 'CA',
  }),
};
const alerts = { get: async () => null } as unknown as AlertStore;
const factory = () => ({
  devices: new MemoryDeviceStore(),
  places: new MemoryPlaceStore(),
  alerts,
  geocoder,
  checkins: new MemoryCheckInStore(),
  cellIndex: new MemoryCellIndex(),
  loadTokenKeys: async () => new Map<string, string>(),
});

const event = (
  method: string,
  rawPath: string,
  body?: unknown,
  headers: Record<string, string> = {},
): ApiEvent => ({
  rawPath,
  headers,
  body: body === undefined ? null : JSON.stringify(body),
  requestContext: { http: { method } },
});

let logs: string[];
beforeEach(() => {
  logs = [];
  const capture = (...a: unknown[]) => void logs.push(a.map(String).join(' '));
  vi.spyOn(console, 'log').mockImplementation(capture);
  vi.spyOn(console, 'error').mockImplementation(capture);
  vi.spyOn(console, 'warn').mockImplementation(capture);
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe('API Lambda handler', () => {
  it('returns the right responses through a fake store factory', async () => {
    const h = createHandler(factory);
    expect((await h(event('POST', '/devices', {}))).statusCode).toBe(201);
    expect((await h(event('GET', '/nope'))).statusCode).toBe(404);
    expect((await h(event('GET', '/places'))).statusCode).toBe(401);
  });

  it('builds stores once per warm Lambda', async () => {
    let builds = 0;
    const h = createHandler(() => {
      builds++;
      return factory();
    });
    await h(event('GET', '/nope'));
    await h(event('GET', '/nope'));
    expect(builds).toBe(1);
  });

  it('returns 500 "misconfigured" without leaking details when env vars are missing', async () => {
    vi.stubEnv('DB_CLUSTER_ARN', '');
    vi.stubEnv('DB_SECRET_ARN', 'arn:secret-value');
    vi.stubEnv('DB_NAME', 'relay');
    vi.stubEnv('PLACE_INDEX_NAME', '');
    const res = await createHandler(awsStores)(event('GET', '/places'));
    expect(res.statusCode).toBe(500);
    expect(JSON.parse(res.body)).toEqual({
      code: 'misconfigured',
      message: 'Service is not configured',
    });
    expect(res.body).not.toContain('DB_CLUSTER_ARN');
    // Details are logged for operators.
    expect(logs.join('\n')).toContain('DB_CLUSTER_ARN, PLACE_INDEX_NAME');
  });

  it('logs one structured line per request, never credentials or coordinates', async () => {
    const h = createHandler(factory);
    const secret = 'dev-id.super-secret-credential';
    await h(event('POST', '/devices', {}));
    await h(
      event(
        'POST',
        '/places',
        { label: 'Home', address: '1 A St', latitude: LAT, longitude: LNG },
        { authorization: `Bearer ${secret}` },
      ),
    );
    const requestLines = logs.filter((l) => l.startsWith('{') && l.includes('"route"'));
    expect(requestLines).toHaveLength(2);
    const parsed = JSON.parse(requestLines[1] ?? '{}') as Record<string, unknown>;
    expect(Object.keys(parsed).sort()).toEqual(['method', 'ms', 'route', 'status']);
    expect(parsed).toMatchObject({ method: 'POST', route: '/places', status: 401 });
    const all = logs.join('\n');
    for (const bad of [secret, 'super-secret', String(LAT), String(LNG), 'Bearer']) {
      expect(all).not.toContain(bad);
    }
  });

  it('never logs err.message from database errors (it can quote coordinates)', async () => {
    const dbError = Object.assign(
      new Error(
        `null value in column "x" violates not-null constraint. Failing row contains (${LAT}, ${LNG}, Home)`,
      ),
      { name: 'BadRequestException' },
    );
    const credential = '11111111-1111-4111-8111-111111111111.secret';
    // Failure inside a request (the router catches it) ...
    const inRequest = createHandler(() => ({
      ...factory(),
      devices: {
        get: async () => {
          throw dbError;
        },
      } as unknown as MemoryDeviceStore,
    }));
    const a = await inRequest(
      event('GET', '/places', undefined, { authorization: `Bearer ${credential}` }),
    );
    // ... and while building the stores (the handler catches it).
    const atStartup = createHandler(() => {
      throw dbError;
    });
    const b = await atStartup(event('GET', '/alerts/11111111-1111-4111-8111-111111111111'));
    for (const res of [a, b]) {
      expect(res.statusCode).toBe(500);
      expect(JSON.parse(res.body)).toMatchObject({ code: 'internal_error' });
    }
    const all = logs.join('\n');
    for (const bad of [String(LAT), String(LNG), 'Failing row', 'violates', 'Home']) {
      expect(all).not.toContain(bad);
    }
    const errors = logs
      .filter((l) => l.includes('"level":"error"'))
      .map((l) => JSON.parse(l) as Record<string, unknown>);
    expect(errors).toHaveLength(2);
    expect(errors[0]).toMatchObject({
      error: 'BadRequestException',
      code: 'internal_error',
      route: '/places',
    });
    expect(errors[1]).toMatchObject({
      error: 'BadRequestException',
      code: 'internal_error',
      route: '/alerts/{id}',
    });
  });
});
