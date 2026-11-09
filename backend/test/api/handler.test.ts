import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ApiEvent } from '../../src/api/index.js';
import { awsStores, createHandler } from '../../src/api/handler.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
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
});
