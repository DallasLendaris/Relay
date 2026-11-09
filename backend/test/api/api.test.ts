import { createHash, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import SwaggerParser from '@apidevtools/swagger-parser';
import { Ajv2020 } from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { AlertSchema, type Alert } from '@relay/shared';
import { createApi, type ApiEvent } from '../../src/api/index.js';
import { createHandler } from '../../src/api/handler.js';
import { MemoryCheckInStore } from '../../src/adapters/memory/checkin-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { cellForPoint } from '../../src/geo/index.js';
import { createCheckInToken } from '../../src/tokens/index.js';
import type { AlertStore, Geocoder } from '../../src/ports.js';

const SPEC_PATH = fileURLToPath(
  new URL('../../../packages/shared/openapi/relay.yaml', import.meta.url),
);

// ---------- Spec-based response validation ----------

interface SpecDoc {
  paths: Record<string, Record<string, { responses: Record<string, ResponseDef> }>>;
}
interface ResponseDef {
  content?: Record<string, { schema: object }>;
}

let spec: SpecDoc;
const ajv = new Ajv2020({ strict: false, allErrors: true });
addFormats.default(ajv);

beforeAll(async () => {
  spec = (await SwaggerParser.dereference(SPEC_PATH)) as unknown as SpecDoc;
});

/** Finds the spec path template (e.g. /places/{placeId}) that matches a concrete path. */
function templateFor(path: string): string {
  for (const tpl of Object.keys(spec.paths)) {
    const re = new RegExp('^' + tpl.replace(/\{[^}]+\}/g, '[^/]+') + '$');
    if (re.test(path)) return tpl;
  }
  throw new Error(`No spec path for ${path}`);
}

/**
 * The spec does not list 403 or 409 for check-ins yet (packages/shared is not ours to edit),
 * so those bodies are checked against the shared Error schema instead.
 */
function expectErrorBody(body: string): void {
  const schema = (spec as unknown as { components: { schemas: { Error: object } } }).components
    .schemas.Error;
  const validate = ajv.compile(schema);
  expect(validate(JSON.parse(body)), JSON.stringify(validate.errors)).toBe(true);
}

/** Asserts the response status is documented and the body matches that response's schema. */
function expectMatchesSpec(method: string, path: string, status: number, body: string): void {
  const op = spec.paths[templateFor(path)]?.[method.toLowerCase()];
  expect(op, `${method} ${path} is in the spec`).toBeDefined();
  const response = op?.responses[String(status)];
  expect(response, `${method} ${path} documents status ${status}`).toBeDefined();
  const schema = response?.content?.['application/json']?.schema;
  if (!schema) {
    expect(body, 'no content documented, so body must be empty').toBe('');
    return;
  }
  const validate = ajv.compile(schema);
  const ok = validate(JSON.parse(body));
  expect(ok, JSON.stringify(validate.errors)).toBe(true);
}

// ---------- Fakes ----------

const fakeGeocoder: Geocoder = {
  async geocode(address) {
    const known: Record<
      string,
      { latitude: number; longitude: number; shortAddress: string; state: string }
    > = {
      '1 Washington Sq, San Jose, CA': {
        latitude: 37.3352,
        longitude: -121.8811,
        shortAddress: 'Washington Sq, San Jose',
        state: 'CA',
      },
      '1 Main St, Reno, NV': {
        latitude: 39.53,
        longitude: -119.81,
        shortAddress: 'Main St, Reno',
        state: 'NV',
      },
    };
    return known[address] ?? null;
  },
};

class FakeAlertStore implements AlertStore {
  readonly alerts = new Map<string, Alert>();
  async get(id: string) {
    return this.alerts.get(id) ?? null;
  }
  async findBySourceId() {
    return null;
  }
  async save(alert: Alert) {
    this.alerts.set(alert.id, alert);
  }
  async listActiveForCells(cells: string[]) {
    return [...this.alerts.values()].filter(
      (a) =>
        (a.state === 'active' || a.state === 'updated') &&
        a.area.h3Cells.some((c) => cells.includes(c)),
    );
  }
}

const SAN_JOSE = { latitude: 37.3382, longitude: -121.8863 };
const SF = { latitude: 37.7749, longitude: -122.4194 };

function makeAlert(overrides: Partial<Alert> & { cells: string[] }): Alert {
  const { cells, ...rest } = overrides;
  return AlertSchema.parse({
    id: randomUUID(),
    source: 'nws',
    sourceId: 'urn:oid:2.49.0.1.840.0.test',
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: {
      description: 'Santa Clara County',
      geometry: null,
      ugcCodes: [],
      sameCodes: [],
      h3Cells: cells,
    },
    sent: '2026-11-09T10:00:00Z',
    effective: '2026-11-09T10:00:00Z',
    onset: null,
    expires: '2026-11-09T18:00:00Z',
    ends: null,
    official: {
      headline: 'Flash Flood Warning issued',
      description: 'Water is rising.',
      instruction: null,
      senderName: 'NWS San Francisco',
      language: 'en-US',
    },
    summaries: [],
    signature: null,
    ...rest,
  });
}

// ---------- Test harness ----------

let devices: MemoryDeviceStore;
let alerts: FakeAlertStore;
let places: MemoryPlaceStore;
let checkins: MemoryCheckInStore;
const TOKEN_KEYS = new Map([['k1', 'test-secret-one']]);
let route: ReturnType<typeof createApi>;

function event(method: string, path: string, body?: unknown, credential?: string): ApiEvent {
  return {
    rawPath: path,
    headers: credential ? { Authorization: `Bearer ${credential}` } : {},
    body: body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body),
    requestContext: { http: { method } },
  };
}

/** Calls the API, checks the response against the OpenAPI spec when the route is documented. */
async function call(method: string, path: string, body?: unknown, credential?: string) {
  const res = await route(event(method, path, body, credential));
  if (res.statusCode === 403 || res.statusCode === 409) expectErrorBody(res.body);
  else expectMatchesSpec(method, path, res.statusCode, res.body);
  return {
    status: res.statusCode,
    body: res.body ? (JSON.parse(res.body) as Record<string, any>) : null,
  };
}

async function register() {
  const res = await call('POST', '/devices', {});
  return res.body as { deviceId: string; credential: string };
}

beforeEach(() => {
  devices = new MemoryDeviceStore();
  alerts = new FakeAlertStore();
  places = new MemoryPlaceStore();
  checkins = new MemoryCheckInStore();
  route = createApi({
    devices,
    places,
    alerts,
    geocoder: fakeGeocoder,
    checkins,
    tokenKeys: TOKEN_KEYS,
    clock: { now: () => new Date('2026-11-09T12:00:00Z') },
    newId: randomUUID,
  });
});

// ---------- Tests ----------

describe('devices', () => {
  it('registers a device and stores only the credential hash', async () => {
    const res = await call('POST', '/devices', {});
    expect(res.status).toBe(201);
    const { deviceId, credential } = res.body as { deviceId: string; credential: string };
    expect(credential).toMatch(new RegExp(`^${deviceId}\\.[A-Za-z0-9_-]{43}$`));
    const stored = await devices.get(deviceId);
    expect(stored?.credentialHash).toBe(createHash('sha256').update(credential).digest('hex'));
    expect(JSON.stringify(stored)).not.toContain(credential);
  });

  it('saves the push token and language', async () => {
    const { deviceId, credential } = await register();
    const res = await call(
      'PUT',
      '/devices/me/push-token',
      { expoPushToken: 'ExponentPushToken[abc123]', language: 'es' },
      credential,
    );
    expect(res.status).toBe(204);
    const stored = await devices.get(deviceId);
    expect(stored).toMatchObject({ expoPushToken: 'ExponentPushToken[abc123]', language: 'es' });
  });

  it('accepts the ExpoPushToken form and rejects malformed tokens and languages', async () => {
    const { credential } = await register();
    const put = (b: unknown) => call('PUT', '/devices/me/push-token', b, credential);
    expect((await put({ expoPushToken: 'ExpoPushToken[xyz]', language: 'en' })).status).toBe(204);
    expect((await put({ expoPushToken: 'not-a-token', language: 'en' })).status).toBe(400);
    expect((await put({ expoPushToken: 'ExponentPushToken[]', language: 'en' })).status).toBe(400);
    expect((await put({ expoPushToken: 'ExponentPushToken[a]', language: 'fr' })).status).toBe(400);
  });
});

describe('push token reinstall', () => {
  it('moves the token to the newest device so only one holds it', async () => {
    const first = await register();
    const second = await register();
    const body = { expoPushToken: 'ExponentPushToken[same]', language: 'en' };
    await call('PUT', '/devices/me/push-token', body, first.credential);
    await call('PUT', '/devices/me/push-token', body, second.credential);
    expect((await devices.get(first.deviceId))?.expoPushToken).toBeNull();
    expect((await devices.get(second.deviceId))?.expoPushToken).toBe('ExponentPushToken[same]');
  });
});

describe('auth and errors', () => {
  it('returns 401 for a non-UUID device id in the credential', async () => {
    expect((await call('GET', '/places', undefined, 'not-a-uuid.secret')).status).toBe(401);
  });

  it('returns 400, not 500, for malformed percent-encoding in a path', async () => {
    const { credential } = await register();
    // The spec documents no 400 for this route, so skip the spec check and call the router.
    const res = await route(event('DELETE', '/places/%E0%A4%A', undefined, credential));
    expect(res.statusCode).toBe(400);
    expect(JSON.parse(res.body)).toMatchObject({ code: 'invalid_request' });
  });

  it('returns 401 without credentials, with a wrong secret, an unknown device, or garbage', async () => {
    const { deviceId, credential } = await register();
    const none = await call('GET', '/places');
    expect(none.status).toBe(401);
    expect(none.body).toMatchObject({ code: expect.any(String), message: expect.any(String) });
    expect((await call('GET', '/places', undefined, `${deviceId}.wrongsecret`)).status).toBe(401);
    expect(
      (await call('GET', '/places', undefined, `${randomUUID()}.${credential.split('.')[1]}`))
        .status,
    ).toBe(401);
    expect((await call('GET', '/places', undefined, 'garbage')).status).toBe(401);
    expect((await call('GET', '/alerts')).status).toBe(401);
    expect(
      (
        await call('PUT', '/devices/me/push-token', {
          expoPushToken: 'ExponentPushToken[a]',
          language: 'en',
        })
      ).status,
    ).toBe(401);
  });

  it('returns 404 for an unknown route and 400 for bad JSON', async () => {
    const missing = await route(event('GET', '/nope'));
    expect(missing.statusCode).toBe(404);
    expect(JSON.parse(missing.body)).toMatchObject({ code: 'not_found' });
    const bad = await route(event('POST', '/devices', '{not json'));
    expect(bad.statusCode).toBe(400);
    expect(JSON.parse(bad.body)).toMatchObject({ code: 'invalid_json' });
  });

  it('rejects a POST /devices body with extra fields (400)', async () => {
    expect((await call('POST', '/devices', { hello: 'x' })).status).toBe(400);
  });
});

describe('places', () => {
  it('runs the full flow: add by address and coordinates, list, delete, enforce limits', async () => {
    const { credential } = await register();
    const other = await register();

    const byAddress = await call(
      'POST',
      '/places',
      { label: 'Home', address: '1 Washington Sq, San Jose, CA' },
      credential,
    );
    expect(byAddress.status).toBe(201);
    expect(byAddress.body).toEqual({
      id: expect.any(String),
      label: 'Home',
      shortAddress: 'Washington Sq, San Jose',
      h3Cell: cellForPoint(37.3352, -121.8811),
      createdAt: '2026-11-09T12:00:00.000Z',
    });

    const byCoords = await call('POST', '/places', { label: 'Mom', ...SF }, credential);
    expect(byCoords.status).toBe(201);
    expect(byCoords.body?.['h3Cell']).toBe(cellForPoint(SF.latitude, SF.longitude));
    // Coordinates are never echoed back.
    expect(JSON.stringify(byCoords.body)).not.toMatch(/latitude|longitude|deviceId/);

    const list = await call('GET', '/places', undefined, credential);
    expect(list.status).toBe(200);
    expect(list.body?.['places']).toHaveLength(2);
    // Another device sees none of them.
    expect((await call('GET', '/places', undefined, other.credential)).body?.['places']).toEqual(
      [],
    );

    // Deleting someone else's place is a 404 and leaves it in place.
    const placeId = byAddress.body?.['id'] as string;
    expect((await call('DELETE', `/places/${placeId}`, undefined, other.credential)).status).toBe(
      404,
    );
    expect((await call('DELETE', `/places/${placeId}`, undefined, credential)).status).toBe(204);
    expect((await call('DELETE', `/places/${placeId}`, undefined, credential)).status).toBe(404);
    expect((await call('DELETE', '/places/not-a-uuid', undefined, credential)).status).toBe(404);
    expect((await call('GET', '/places', undefined, credential)).body?.['places']).toHaveLength(1);

    // Fill up to 5, then the 6th is rejected.
    for (let i = 0; i < 4; i++) {
      expect(
        (await call('POST', '/places', { label: `P${i}`, ...SAN_JOSE }, credential)).status,
      ).toBe(201);
    }
    const sixth = await call('POST', '/places', { label: 'Too many', ...SAN_JOSE }, credential);
    expect(sixth.status).toBe(409);
    expect(sixth.body).toMatchObject({ code: 'place_limit_reached' });
    // Deleting frees a slot.
    const first = (await call('GET', '/places', undefined, credential)).body?.['places'][0]
      .id as string;
    await call('DELETE', `/places/${first}`, undefined, credential);
    expect(
      (await call('POST', '/places', { label: 'Again', ...SAN_JOSE }, credential)).status,
    ).toBe(201);
  });

  it('returns 422 outside California, by coordinates and by address', async () => {
    const { credential } = await register();
    const reno = await call(
      'POST',
      '/places',
      { label: 'Reno', latitude: 39.53, longitude: -119.81 },
      credential,
    );
    expect(reno.status).toBe(422);
    expect(reno.body).toMatchObject({ code: 'outside_california' });
    expect(
      (
        await call(
          'POST',
          '/places',
          { label: 'Vegas', latitude: 36.17, longitude: -115.14 },
          credential,
        )
      ).status,
    ).toBe(422);
    expect(
      (await call('POST', '/places', { label: 'Reno', address: '1 Main St, Reno, NV' }, credential))
        .status,
    ).toBe(422);
    // Real California cities across the state are accepted.
    for (const [latitude, longitude] of [
      [34.05, -118.24],
      [32.72, -117.16],
      [38.58, -121.49],
      [40.8, -124.16],
    ]) {
      expect(
        (await call('POST', '/places', { label: 'CA', latitude, longitude }, credential)).status,
      ).toBe(201);
      await call('GET', '/places', undefined, credential);
      const id = (await call('GET', '/places', undefined, credential)).body?.['places'].at(-1)
        .id as string;
      await call('DELETE', `/places/${id}`, undefined, credential);
    }
  });

  it('returns 400 for invalid place requests', async () => {
    const { credential } = await register();
    const post = (b: unknown) => call('POST', '/places', b, credential);
    expect((await post({ label: 'x' })).status).toBe(400);
    expect((await post({ address: 'a' })).status).toBe(400);
    expect((await post({ label: 'x', latitude: 37 })).status).toBe(400);
    expect((await post({ label: 'x', latitude: 'a', longitude: 'b' })).status).toBe(400);
    expect((await post({ label: 'x', latitude: 95, longitude: 0 })).status).toBe(400);
    expect((await post({ label: 'x', address: 'a', ...SAN_JOSE })).status).toBe(400);
    expect((await post({ label: 'x', address: 'unknown place' })).status).toBe(400);
    expect((await post([])).status).toBe(400);
  });
});

describe('alerts', () => {
  it('lists active alerts for the device places with matchedPlaceIds', async () => {
    const { credential } = await register();
    const home = (await call('POST', '/places', { label: 'Home', ...SAN_JOSE }, credential)).body?.[
      'id'
    ] as string;
    const sf = (await call('POST', '/places', { label: 'SF', ...SF }, credential)).body?.[
      'id'
    ] as string;
    const sjCell = cellForPoint(SAN_JOSE.latitude, SAN_JOSE.longitude);
    const sfCell = cellForPoint(SF.latitude, SF.longitude);

    const both = makeAlert({ cells: [sjCell, sfCell, 'other-cell'] });
    const sjOnly = makeAlert({
      cells: [sjCell],
      state: 'updated',
      official: {
        headline: null,
        description: 'd',
        instruction: null,
        senderName: 's',
        language: 'en',
      },
    });
    const cancelled = makeAlert({ cells: [sjCell], state: 'cancelled' });
    const elsewhere = makeAlert({ cells: ['other-cell'] });
    for (const a of [both, sjOnly, cancelled, elsewhere]) await alerts.save(a);

    const res = await call('GET', '/alerts', undefined, credential);
    expect(res.status).toBe(200);
    const list = res.body?.['alerts'] as Array<{
      id: string;
      matchedPlaceIds: string[];
      headline: string | null;
      state: string;
    }>;
    expect(list.map((a) => a.id).sort()).toEqual([both.id, sjOnly.id].sort());
    const byId = Object.fromEntries(list.map((a) => [a.id, a]));
    expect(byId[both.id]?.matchedPlaceIds.sort()).toEqual([home, sf].sort());
    expect(byId[both.id]?.headline).toBe('Flash Flood Warning issued');
    expect(byId[sjOnly.id]).toMatchObject({
      matchedPlaceIds: [home],
      headline: null,
      state: 'updated',
    });
  });

  it('returns an empty list for a device with no places', async () => {
    const { credential } = await register();
    await alerts.save(makeAlert({ cells: ['x'] }));
    const res = await call('GET', '/alerts', undefined, credential);
    expect(res.body).toEqual({ alerts: [] });
  });

  it('shows an ended alert as expired but still returns it', async () => {
    const { credential } = await register();
    // Clock is 2026-11-09T12:00Z. Expired at 10:00Z with no later ends: ended.
    const ended = makeAlert({ cells: ['x'], expires: '2026-11-09T10:00:00Z' });
    // Expired at 10:00Z but ends at 14:00Z: still running.
    const running = makeAlert({
      cells: ['x'],
      expires: '2026-11-09T10:00:00Z',
      ends: '2026-11-09T14:00:00Z',
    });
    await alerts.save(ended);
    await alerts.save(running);
    const a = await call('GET', `/alerts/${ended.id}`, undefined, credential);
    expect(a.status).toBe(200);
    expect(a.body).toEqual({ ...ended, state: 'expired' });
    const b = await call('GET', `/alerts/${running.id}`, undefined, credential);
    expect(b.body?.['state']).toBe('active');
  });

  it('returns the full alert to any registered device, or 404', async () => {
    const { credential } = await register(); // no places, still allowed: alerts are public
    const alert = makeAlert({ cells: ['somewhere'] });
    await alerts.save(alert);
    const res = await call('GET', `/alerts/${alert.id}`, undefined, credential);
    expect(res.status).toBe(200);
    expect(res.body).toEqual(alert);
    expect((await call('GET', `/alerts/${randomUUID()}`, undefined, credential)).status).toBe(404);
    expect((await call('GET', '/alerts/nope', undefined, credential)).status).toBe(404);
    expect((await call('GET', `/alerts/${alert.id}`)).status).toBe(401);
  });
});

describe('check-ins', () => {
  const sjCell = cellForPoint(SAN_JOSE.latitude, SAN_JOSE.longitude);
  const sfCell = cellForPoint(SF.latitude, SF.longitude);
  const KEY = { id: 'k1', secret: 'test-secret-one' };
  const EXPIRES = new Date('2026-11-09T18:00:00Z');

  /** A device with a San Jose place, an alert covering it, and a valid token for the pair. */
  async function setup(cells = [sjCell]) {
    const device = await register();
    await call('POST', '/places', { label: 'Home', ...SAN_JOSE }, device.credential);
    const alert = makeAlert({ cells });
    await alerts.save(alert);
    const tokenFor = (alertId: string, deviceId: string, expiresAt = EXPIRES) =>
      createCheckInToken({ alertId, deviceId, expiresAt }, KEY);
    return { ...device, alert, token: tokenFor(alert.id, device.deviceId), tokenFor };
  }
  const post = (alertId: string, credential: string | undefined, body: unknown) =>
    call('POST', `/alerts/${alertId}/checkins`, body, credential);
  const body = (token: string, over: Record<string, unknown> = {}) => ({
    status: 'ok',
    token,
    idempotencyKey: 'key-1',
    ...over,
  });

  it('records a valid check-in against the matching cell', async () => {
    const s = await setup([sfCell, sjCell]);
    const res = await post(s.alert.id, s.credential, body(s.token));
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ checkedInAt: '2026-11-09T12:00:00.000Z' });
    expect(await checkins.get(s.alert.id, 'key-1')).toMatchObject({
      deviceId: s.deviceId,
      status: 'ok',
      h3Cell: sjCell,
    });
    expect(checkins.total(s.alert.id, sjCell)).toBe(1);
  });

  it('uses the first of the device places that the alert covers', async () => {
    const s = await setup([sfCell, sjCell]);
    await call('POST', '/places', { label: 'SF', ...SF }, s.credential);
    // Home (San Jose) was saved first, so it wins even though the alert lists SF first.
    await post(s.alert.id, s.credential, body(s.token));
    expect((await checkins.get(s.alert.id, 'key-1'))?.h3Cell).toBe(sjCell);
  });

  it('rejects a token made for another device with 403 invalid_token', async () => {
    const s = await setup();
    const other = await register();
    const res = await post(s.alert.id, s.credential, body(s.tokenFor(s.alert.id, other.deviceId)));
    expect(res.status).toBe(403);
    expect(res.body?.['code']).toBe('invalid_token');
  });

  it('rejects a token made for another alert with 403 invalid_token', async () => {
    const s = await setup();
    const res = await post(s.alert.id, s.credential, body(s.tokenFor(randomUUID(), s.deviceId)));
    expect(res.status).toBe(403);
    expect(res.body?.['code']).toBe('invalid_token');
  });

  it('rejects an expired token with 403 invalid_token', async () => {
    const s = await setup();
    const old = s.tokenFor(s.alert.id, s.deviceId, new Date('2026-11-09T11:00:00Z'));
    const res = await post(s.alert.id, s.credential, body(old));
    expect(res.status).toBe(403);
    expect(res.body?.['code']).toBe('invalid_token');
  });

  it('rejects tampered, unknown-key and malformed tokens the same way', async () => {
    const s = await setup();
    const [payload = '', sig = ''] = s.token.split('.');
    const forged = createCheckInToken(
      { alertId: s.alert.id, deviceId: s.deviceId, expiresAt: EXPIRES },
      { id: 'k1', secret: 'someone-elses-secret' },
    );
    const unknownKey = createCheckInToken(
      { alertId: s.alert.id, deviceId: s.deviceId, expiresAt: EXPIRES },
      { id: 'k9', secret: 'test-secret-one' },
    );
    const bad = [
      `${payload}.${sig.slice(0, -2)}AA`,
      forged,
      unknownKey,
      'garbage',
      `${payload}x.${sig}`,
    ];
    const messages = new Set<string>();
    for (const token of bad) {
      const res = await post(s.alert.id, s.credential, body(token));
      expect(res.status).toBe(403);
      expect(res.body?.['code']).toBe('invalid_token');
      messages.add(JSON.stringify(res.body));
    }
    // One identical answer for every failure: nothing says which check failed.
    expect(messages.size).toBe(1);
    expect(checkins.total(s.alert.id, sjCell)).toBe(0);
  });

  it('returns the original time for a repeated idempotencyKey and counts once', async () => {
    const s = await setup();
    const first = await post(s.alert.id, s.credential, body(s.token));
    const later = createApi({
      devices,
      places,
      alerts,
      geocoder: fakeGeocoder,
      checkins,
      tokenKeys: TOKEN_KEYS,
      clock: { now: () => new Date('2026-11-09T12:05:00Z') },
      newId: randomUUID,
    });
    const res = await later(
      event('POST', `/alerts/${s.alert.id}/checkins`, body(s.token), s.credential),
    );
    expectMatchesSpec('POST', `/alerts/${s.alert.id}/checkins`, res.statusCode, res.body);
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body)).toEqual(first.body);
    expect(checkins.total(s.alert.id, sjCell)).toBe(1);
  });

  it('does not let another device reuse an idempotencyKey (409)', async () => {
    const s = await setup();
    await post(s.alert.id, s.credential, body(s.token));
    const other = await register();
    await call('POST', '/places', { label: 'Home', ...SAN_JOSE }, other.credential);
    const res = await post(
      s.alert.id,
      other.credential,
      body(s.tokenFor(s.alert.id, other.deviceId)),
    );
    expect(res.status).toBe(409);
    expect(res.body?.['code']).toBe('idempotency_conflict');
    expect(res.body?.['checkedInAt']).toBeUndefined();
  });

  it('rejects need_help with 400 (reserved for the Pilot)', async () => {
    const s = await setup();
    const res = await post(s.alert.id, s.credential, body(s.token, { status: 'need_help' }));
    expect(res.status).toBe(400);
    expect(checkins.total(s.alert.id, sjCell)).toBe(0);
  });

  it('rejects malformed bodies with 400', async () => {
    const s = await setup();
    for (const b of [
      {},
      body(s.token, { status: 'maybe' }),
      body(s.token, { idempotencyKey: '' }),
      body(s.token, { idempotencyKey: 'x'.repeat(201) }),
      { status: 'ok', idempotencyKey: 'k' },
      { status: 'ok', token: 5, idempotencyKey: 'k' },
    ]) {
      expect((await post(s.alert.id, s.credential, b)).status).toBe(400);
    }
  });

  it('returns 403 not_in_area when none of the device places is in the alert area', async () => {
    const s = await setup([sfCell]);
    const res = await post(s.alert.id, s.credential, body(s.token));
    expect(res.status).toBe(403);
    expect(res.body?.['code']).toBe('not_in_area');
    expect(checkins.total(s.alert.id, sfCell)).toBe(0);
  });

  it('returns 404 for an unknown or malformed alert id', async () => {
    const s = await setup();
    expect((await post(randomUUID(), s.credential, body(s.token))).status).toBe(404);
    expect((await post('nope', s.credential, body(s.token))).status).toBe(404);
  });

  it('returns 401 without valid device credentials', async () => {
    const s = await setup();
    expect((await post(s.alert.id, undefined, body(s.token))).status).toBe(401);
    expect((await post(s.alert.id, `${s.deviceId}.wrong`, body(s.token))).status).toBe(401);
    expect(checkins.total(s.alert.id, sjCell)).toBe(0);
  });
});

describe('Lambda handler', () => {
  it('builds the API from an injected store factory on first use', async () => {
    let builds = 0;
    const handler = createHandler(() => {
      builds++;
      return {
        devices: new MemoryDeviceStore(),
        places: new MemoryPlaceStore(),
        alerts: new FakeAlertStore(),
        geocoder: fakeGeocoder,
        checkins: new MemoryCheckInStore(),
        loadTokenKeys: async () => TOKEN_KEYS,
      };
    });
    const first = await handler(event('POST', '/devices', {}));
    const second = await handler(event('POST', '/devices', {}));
    expect([first.statusCode, second.statusCode]).toEqual([201, 201]);
    expect(builds).toBe(1);
  });
});

// Keep the spec file referenced so a missing contract fails loudly.
it('finds the OpenAPI contract', () => {
  expect(readFileSync(SPEC_PATH, 'utf8')).toContain('openapi: 3.1.0');
});
