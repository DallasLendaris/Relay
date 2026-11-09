import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createApi, type ApiDeps } from '../../src/api/index.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { createCheckInToken, createDeviceCredential } from '../../src/tokens/index.js';

const TOTAL = 100_000;
const TOKEN_KEY = { id: 't1', secret: 'token-secret-one' };
const CRED_KEY = { id: 'c1', secret: 'credential-secret-one' };

const forbidden = <T extends object>(name: string): T =>
  new Proxy({} as T, {
    get: () => () => {
      throw new Error(`${name} must not be called on the check-in path`);
    },
  });

describe('check-in throughput (in-memory stores)', () => {
  it(`pushes ${TOTAL.toLocaleString()} check-ins through the API in under 10 s`, async () => {
    const alertId = randomUUID();
    const cells = Array.from({ length: 200 }, (_, i) => `cell-${i}`);
    const checkins = new MemoryCheckInStore();
    const deps: ApiDeps = {
      devices: forbidden('devices'),
      places: forbidden('places'),
      alerts: forbidden('alerts'),
      geocoder: forbidden('geocoder'),
      checkins,
      tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
      deviceCredentialKeys: new Map([[CRED_KEY.id, CRED_KEY.secret]]),
      clock: { now: () => new Date('2026-11-09T12:00:00Z') },
      newId: randomUUID,
    };
    const route = createApi(deps);
    const expiresAt = new Date('2026-11-09T18:00:00Z');

    // Requests are built per chunk of 1,000 (a million phones arrive concurrently, not one by
    // one) and only the handler time is summed. Building chunk by chunk keeps the heap small,
    // so garbage collection does not swamp the measurement.
    const CHUNK = 1000;
    let elapsedMs = 0;
    let ok = 0;
    for (let i = 0; i < TOTAL; i += CHUNK) {
      const events = Array.from({ length: CHUNK }, (_, j) => {
        const deviceId = randomUUID();
        const h3Cell = cells[(i + j) % cells.length] ?? 'cell-0';
        return {
          rawPath: `/alerts/${alertId}/checkins`,
          headers: { authorization: `Bearer ${createDeviceCredential(deviceId, CRED_KEY)}` },
          body: JSON.stringify({
            status: 'ok',
            token: createCheckInToken({ alertId, deviceId, h3Cell, expiresAt }, TOKEN_KEY),
            idempotencyKey: `k-${i + j}`,
          }),
          requestContext: { http: { method: 'POST' } },
        };
      });
      const started = performance.now();
      const results = await Promise.all(events.map((e) => route(e)));
      elapsedMs += performance.now() - started;
      ok += results.filter((r) => r.statusCode === 200).length;
    }
    const seconds = elapsedMs / 1000;
    const rate = Math.round(TOTAL / seconds);
    console.log(
      JSON.stringify({
        msg: 'checkin benchmark',
        checkIns: TOTAL,
        seconds: Number(seconds.toFixed(2)),
        requestsPerSecond: rate,
      }),
    );

    expect(ok).toBe(TOTAL);
    const counted = cells.reduce((sum, c) => sum + checkins.total(alertId, c), 0);
    expect(counted).toBe(TOTAL);
    expect(seconds).toBeLessThan(10);
  }, 60_000);
});
