import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { LIMITS } from '@relay/shared';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { createApi, type ApiEvent } from '../../src/api/index.js';
import type { AlertStore } from '../../src/ports.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';

// SEC-B-6 / SEC-B-2 / DEP-1: hostile or oversized input is a 400 (or 413), never a 500, and the
// expensive collaborators (geocoder, database) are not reached.

let route: ReturnType<typeof createApi>;
const geocode = vi.fn(async () => null);

beforeEach(() => {
  geocode.mockClear();
  route = createApi({
    devices: new MemoryDeviceStore(),
    places: new MemoryPlaceStore(),
    alerts: { get: async () => null } as unknown as AlertStore,
    geocoder: { geocode },
    checkins: new MemoryCheckInStore(),
    cellIndex: new MemoryCellIndex(),
    tokenKeys: new Map(),
    clock: { now: () => new Date('2026-11-09T12:00:00Z') },
    newId: randomUUID,
  });
});

async function call(
  method: string,
  path: string,
  body?: unknown,
  credential?: string,
  raw?: Partial<ApiEvent>,
) {
  const res = await route({
    rawPath: path,
    headers: credential ? { authorization: `Bearer ${credential}` } : {},
    body: body === undefined ? null : JSON.stringify(body),
    requestContext: { http: { method } },
    ...raw,
  });
  return {
    status: res.statusCode,
    body: res.body ? (JSON.parse(res.body) as Record<string, string>) : null,
  };
}

async function register(): Promise<string> {
  const res = await call('POST', '/devices', {});
  return (res.body as unknown as { credential: string }).credential;
}

const coords = { latitude: 37.3352, longitude: -121.8811 };

describe('request body size cap', () => {
  it('rejects a body over the cap with 413 before parsing it', async () => {
    const big = JSON.stringify({ label: 'x'.repeat(LIMITS.maxRequestBodyBytes) });
    const res = await call('POST', '/places', undefined, undefined, { body: big });
    expect(res.status).toBe(413);
    expect(res.body?.['code']).toBe('payload_too_large');
  });

  it('counts base64 bodies by decoded size', async () => {
    const big = Buffer.from('a'.repeat(LIMITS.maxRequestBodyBytes + 10)).toString('base64');
    const res = await call('POST', '/places', undefined, undefined, {
      body: big,
      isBase64Encoded: true,
    });
    expect(res.status).toBe(413);
  });

  it('still accepts a small body', async () => {
    const cred = await register();
    const res = await call('POST', '/places', { label: 'Home', ...coords }, cred);
    expect(res.status).toBe(201);
  });
});

describe('place label and address', () => {
  it('accepts a label of exactly the maximum length', async () => {
    const cred = await register();
    const label = 'a'.repeat(LIMITS.labelMax);
    expect((await call('POST', '/places', { label, ...coords }, cred)).status).toBe(201);
  });

  it.each([
    ['too long', 'a'.repeat(LIMITS.labelMax + 1)],
    ['NUL', 'Ho\u0000me'],
    ['newline', 'Ho\nme'],
    ['C1 control', 'Ho\u0085me'],
  ])('rejects a label that is %s with 400', async (_name, label) => {
    const cred = await register();
    const res = await call('POST', '/places', { label, ...coords }, cred);
    expect(res.status).toBe(400);
  });

  it.each([
    ['too long', 'a'.repeat(LIMITS.addressMax + 1)],
    ['NUL', '1 Main St\u0000, San Jose'],
  ])('rejects an address that is %s with 400, before geocoding', async (_name, address) => {
    const cred = await register();
    const res = await call('POST', '/places', { label: 'Home', address }, cred);
    expect(res.status).toBe(400);
    expect(geocode).not.toHaveBeenCalled();
  });

  it('geocodes an address of exactly the maximum length', async () => {
    const cred = await register();
    const address = 'a'.repeat(LIMITS.addressMax);
    const res = await call('POST', '/places', { label: 'Home', address }, cred);
    // The fake geocoder finds nothing: that is address_not_found, not a bound error.
    expect(res.body?.['code']).toBe('address_not_found');
    expect(geocode).toHaveBeenCalledTimes(1);
  });
});

describe('push token lengths', () => {
  const put = async (body: unknown) =>
    call('PUT', '/devices/me/push-token', body, await register());

  it.each([
    ['ios too long', { platform: 'ios', token: 'ab'.repeat(LIMITS.apnsTokenMax) }],
    ['ios too short', { platform: 'ios', token: 'ab'.repeat(10) }],
    ['ios NUL', { platform: 'ios', token: 'ab'.repeat(32) + '\u0000' }],
    ['android too long', { platform: 'android', token: 'a'.repeat(LIMITS.fcmTokenMax + 1) }],
    ['android too short', { platform: 'android', token: 'a'.repeat(LIMITS.fcmTokenMin - 1) }],
    ['expo too long', { platform: 'expo', token: `ExponentPushToken[${'a'.repeat(300)}]` }],
    ['expo legacy too long', { expoPushToken: `ExponentPushToken[${'a'.repeat(300)}]` }],
  ])('%s is a 400, not a 500', async (_name, body) => {
    const res = await put({ ...body, language: 'en' });
    expect(res.status).toBe(400);
  });

  it('accepts the maximum FCM length', async () => {
    const res = await put({
      platform: 'android',
      token: 'a'.repeat(LIMITS.fcmTokenMax),
      language: 'en',
    });
    expect(res.status).toBeLessThan(300);
  });
});

describe('check-in fields', () => {
  it('rejects an oversized token or idempotency key with 400', async () => {
    const cred = await register();
    const path = `/alerts/${randomUUID()}/checkins`;
    const base = { status: 'ok', token: 't', idempotencyKey: 'k' };
    const longToken = await call(
      'POST',
      path,
      { ...base, token: 'a'.repeat(LIMITS.checkInTokenMax + 1) },
      cred,
    );
    const longKey = await call(
      'POST',
      path,
      { ...base, idempotencyKey: 'k'.repeat(LIMITS.idempotencyKeyMax + 1) },
      cred,
    );
    expect(longToken.status === 400 || longToken.status === 401).toBe(true);
    expect(longKey.status === 400 || longKey.status === 401).toBe(true);
  });
});
