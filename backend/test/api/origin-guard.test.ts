import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ApiEvent } from '../../src/api/index.js';
import { awsStores, createHandler } from '../../src/api/handler.js';
import { ORIGIN_VERIFY_HEADER, createOriginGuard } from '../../src/api/origin-guard.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import type { AlertStore } from '../../src/ports.js';

const SECRET = 'origin-secret-value-0123456789';

const event = (headers: Record<string, string> = {}): ApiEvent => ({
  rawPath: '/devices',
  headers,
  body: '{}',
  requestContext: { http: { method: 'POST' } },
});

beforeEach(() => {
  vi.spyOn(console, 'log').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function handlerWith(loadSecret: () => Promise<string>, now?: () => number) {
  const originGuard = createOriginGuard({ loadSecret, ...(now ? { now } : {}) });
  return createHandler(() => ({
    devices: new MemoryDeviceStore(),
    places: new MemoryPlaceStore(),
    alerts: { get: async () => null } as unknown as AlertStore,
    geocoder: { geocode: async () => null },
    checkins: new MemoryCheckInStore(),
    cellIndex: new MemoryCellIndex(),
    loadTokenKeys: async () => new Map<string, string>(),
    originGuard,
  }));
}

describe('origin verification (REQUIRE_ORIGIN_HEADER)', () => {
  it('refuses a request without the header, with a wrong value, or with an empty one (403)', async () => {
    const h = handlerWith(async () => SECRET);
    const cases: Array<Record<string, string>> = [
      {},
      { [ORIGIN_VERIFY_HEADER]: 'wrong' },
      { [ORIGIN_VERIFY_HEADER]: '' },
      { [ORIGIN_VERIFY_HEADER]: SECRET + 'x' },
      { [ORIGIN_VERIFY_HEADER]: SECRET.slice(0, -1) },
    ];
    for (const headers of cases) {
      const res = await h(event(headers));
      expect(res.statusCode).toBe(403);
      expect(JSON.parse(res.body)).toEqual({ code: 'forbidden', message: 'Forbidden' });
    }
  });

  it('lets a request with the right header through to the router', async () => {
    const h = handlerWith(async () => SECRET);
    expect((await h(event({ [ORIGIN_VERIFY_HEADER]: SECRET }))).statusCode).toBe(201);
  });

  it('caches the secret (one load for many requests) and reloads after the TTL', async () => {
    let t = 0;
    const load = vi.fn(async () => SECRET);
    const h = handlerWith(load, () => t);
    for (let i = 0; i < 5; i++) await h(event({ [ORIGIN_VERIFY_HEADER]: SECRET }));
    expect(load).toHaveBeenCalledTimes(1);
    t = 6 * 60 * 1000;
    await h(event({ [ORIGIN_VERIFY_HEADER]: SECRET }));
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('picks up a rotated secret after the cache expires', async () => {
    let t = 0;
    let current = SECRET;
    const h = handlerWith(
      async () => current,
      () => t,
    );
    expect((await h(event({ [ORIGIN_VERIFY_HEADER]: SECRET }))).statusCode).toBe(201);
    current = 'rotated-secret-value-9876543210';
    t = 6 * 60 * 1000;
    expect((await h(event({ [ORIGIN_VERIFY_HEADER]: SECRET }))).statusCode).toBe(403);
    expect(
      (await h(event({ [ORIGIN_VERIFY_HEADER]: 'rotated-secret-value-9876543210' }))).statusCode,
    ).toBe(201);
  });

  it('fails closed: if the secret cannot be loaded the request errors, it is not let through', async () => {
    const h = handlerWith(async () => {
      throw new Error('secretsmanager down');
    });
    const res = await h(event({ [ORIGIN_VERIFY_HEADER]: SECRET }));
    expect(res.statusCode).toBe(500);
  });

  it('compares hashes of equal length in constant time (any length or content is handled)', async () => {
    const guard = createOriginGuard({ loadSecret: async () => SECRET });
    expect(await guard.allows({ [ORIGIN_VERIFY_HEADER]: SECRET })).toBe(true);
    expect(await guard.allows({ [ORIGIN_VERIFY_HEADER]: 'a' })).toBe(false);
    expect(await guard.allows({ [ORIGIN_VERIFY_HEADER]: 'a'.repeat(100_000) })).toBe(false);
    expect(await guard.allows({})).toBe(false);
  });

  it('awsStores: REQUIRE_ORIGIN_HEADER=true without ORIGIN_VERIFY_SECRET_ARN is misconfigured', async () => {
    for (const [k, v] of Object.entries({
      DB_CLUSTER_ARN: 'arn:cluster',
      DB_SECRET_ARN: 'arn:secret',
      DB_NAME: 'relay',
      PLACE_INDEX_NAME: 'idx',
      CHECKINS_TABLE: 'c',
      TOKEN_KEYS_SECRET_ARN: 'arn:t',
      DEVICE_CREDENTIAL_KEYS_SECRET_ARN: 'arn:d',
      CELL_INDEX_TABLE: 'ci',
      REQUIRE_ORIGIN_HEADER: 'true',
      ORIGIN_VERIFY_SECRET_ARN: '',
    })) {
      vi.stubEnv(k, v);
    }
    const res = await createHandler(awsStores)(event());
    expect(res.statusCode).toBe(500);
    expect(JSON.parse(res.body).code).toBe('misconfigured');
    // With the ARN set the stores build (no AWS call happens until a request needs one).
    vi.stubEnv('ORIGIN_VERIFY_SECRET_ARN', 'arn:origin');
    const stores = await awsStores();
    expect(stores.originGuard).toBeDefined();
    vi.stubEnv('REQUIRE_ORIGIN_HEADER', 'false');
    expect((await awsStores()).originGuard).toBeUndefined();
  });
});
