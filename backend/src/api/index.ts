import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import type { Alert, CheckIn, components } from '@relay/shared';
import { cellForPoint } from '../geo/index.js';
import { verifyCheckInToken } from '../tokens/index.js';
import { effectiveState } from '../ingest/lifecycle.js';
import type {
  AlertStore,
  CheckInStore,
  Clock,
  Device,
  DeviceStore,
  Geocoder,
  Place,
  PlaceStore,
} from '../ports.js';
import { isInCalifornia } from './california.js';
import { HttpError, Router, json, noContent, type ApiEvent, type ApiResult } from './router.js';
import type { Request } from './router.js';

export type { ApiEvent, ApiResult } from './router.js';

export interface CheckInLookup {
  /** The stored check-in for this alert and idempotency key, or null. */
  get(alertId: string, idempotencyKey: string): Promise<CheckIn | null>;
}

export interface ApiDeps {
  devices: DeviceStore;
  places: PlaceStore;
  alerts: AlertStore;
  geocoder: Geocoder;
  /** The port plus a lookup by idempotency key (adapter classes have it; the port does not). */
  checkins: CheckInStore & CheckInLookup;
  /** Check-in token keys by key id (kid -> secret). */
  tokenKeys: Map<string, string>;
  clock: Clock;
  /** Returns a new UUID. Injected so tests are deterministic. */
  newId: () => string;
}

export const MAX_PLACES_PER_DEVICE = 5;
const MAX_IDEMPOTENCY_KEY_LENGTH = 200;

type PlaceBody = components['schemas']['Place'];
type AlertSummary = components['schemas']['AlertSummary'];

const PUSH_TOKEN = /^Expo(nent)?PushToken\[[^\]\s]+\]$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const sha256Hex = (s: string): string => createHash('sha256').update(s).digest('hex');

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function requireObject(body: unknown): Record<string, unknown> {
  if (!isObject(body)) throw new HttpError(400, 'invalid_request', 'Body must be a JSON object');
  return body;
}

function nonEmptyString(v: unknown, name: string): string {
  if (typeof v !== 'string' || v.trim() === '') {
    throw new HttpError(400, 'invalid_request', `${name} must be a non-empty string`);
  }
  return v.trim();
}

/** The Place shape from the spec. Coordinates and deviceId are deliberately left out. */
function toPlaceBody(p: Place): PlaceBody {
  return {
    id: p.id,
    label: p.label,
    shortAddress: p.shortAddress,
    h3Cell: p.h3Cell,
    createdAt: p.createdAt,
  };
}

/** Builds the route function: `route(event) -> {statusCode, headers, body}`. */
export function createApi(deps: ApiDeps): (event: ApiEvent) => Promise<ApiResult> {
  const router = new Router();

  /** Reads `Authorization: Bearer <deviceId>.<secret>` and returns the device, or throws 401. */
  async function authenticate(req: Request): Promise<Device> {
    const fail = () => new HttpError(401, 'unauthorized', 'Missing or invalid credentials');
    const header = req.headers['authorization'];
    const match = header ? /^Bearer (\S+)$/.exec(header) : null;
    const credential = match?.[1];
    if (!credential) throw fail();
    const dot = credential.indexOf('.');
    if (dot <= 0) throw fail();
    // Check the id's shape first: a non-UUID would make the database cast fail (a 500).
    const deviceId = credential.slice(0, dot);
    if (!UUID.test(deviceId)) throw fail();
    const device = await deps.devices.get(deviceId);
    if (!device) throw fail();
    const given = Buffer.from(sha256Hex(credential), 'hex');
    const stored = Buffer.from(device.credentialHash, 'hex');
    if (given.length !== stored.length || !timingSafeEqual(given, stored)) throw fail();
    return device;
  }

  // ---------- Devices ----------

  router.add('POST', '/devices', async (req) => {
    if (req.body !== undefined && Object.keys(requireObject(req.body)).length > 0) {
      throw new HttpError(400, 'invalid_request', 'Body must be an empty object');
    }
    const id = deps.newId();
    const credential = `${id}.${randomBytes(32).toString('base64url')}`;
    await deps.devices.create({
      id,
      credentialHash: sha256Hex(credential),
      expoPushToken: null,
      language: 'en',
      createdAt: deps.clock.now().toISOString(),
    });
    return json(201, { deviceId: id, credential });
  });

  router.add('PUT', '/devices/me/push-token', async (req) => {
    const device = await authenticate(req);
    const body = requireObject(req.body);
    const token = body['expoPushToken'];
    const language = body['language'];
    if (typeof token !== 'string' || !PUSH_TOKEN.test(token)) {
      throw new HttpError(
        400,
        'invalid_push_token',
        'expoPushToken must look like ExponentPushToken[...]',
      );
    }
    if (language !== 'en' && language !== 'es') {
      throw new HttpError(400, 'invalid_request', 'language must be "en" or "es"');
    }
    // Moves the token here if another (reinstalled) device still holds it.
    await deps.devices.assignPushToken(device.id, token, language);
    return noContent();
  });

  // ---------- Places ----------

  router.add('POST', '/places', async (req) => {
    const device = await authenticate(req);
    const body = requireObject(req.body);
    const label = nonEmptyString(body['label'], 'label');
    const hasAddress = body['address'] !== undefined;
    const hasCoords = body['latitude'] !== undefined || body['longitude'] !== undefined;
    if (hasAddress === hasCoords) {
      throw new HttpError(
        400,
        'invalid_request',
        'Provide either address or latitude and longitude',
      );
    }

    if ((await deps.places.listForDevice(device.id)).length >= MAX_PLACES_PER_DEVICE) {
      throw new HttpError(
        409,
        'place_limit_reached',
        `At most ${MAX_PLACES_PER_DEVICE} places per device`,
      );
    }

    let latitude: number;
    let longitude: number;
    let shortAddress: string;
    if (hasAddress) {
      const found = await deps.geocoder.geocode(nonEmptyString(body['address'], 'address'));
      if (!found) throw new HttpError(400, 'address_not_found', 'Could not find that address');
      if (found.state !== 'CA') {
        throw new HttpError(422, 'outside_california', 'Relay supports California addresses only');
      }
      ({ latitude, longitude, shortAddress } = found);
    } else {
      const lat = body['latitude'];
      const lng = body['longitude'];
      if (
        typeof lat !== 'number' ||
        typeof lng !== 'number' ||
        !Number.isFinite(lat) ||
        !Number.isFinite(lng) ||
        Math.abs(lat) > 90 ||
        Math.abs(lng) > 180
      ) {
        throw new HttpError(400, 'invalid_request', 'latitude and longitude must be valid numbers');
      }
      if (!isInCalifornia(lat, lng)) {
        throw new HttpError(422, 'outside_california', 'Relay supports California locations only');
      }
      latitude = lat;
      longitude = lng;
      shortAddress = 'Dropped pin'; // no street address when saved by coordinates
    }

    const place: Place = {
      id: deps.newId(),
      deviceId: device.id,
      label,
      shortAddress,
      latitude,
      longitude,
      h3Cell: cellForPoint(latitude, longitude),
      createdAt: deps.clock.now().toISOString(),
    };
    await deps.places.add(place);
    return json(201, toPlaceBody(place));
  });

  router.add('GET', '/places', async (req) => {
    const device = await authenticate(req);
    const places = await deps.places.listForDevice(device.id);
    return json(200, { places: places.map(toPlaceBody) });
  });

  router.add('DELETE', '/places/{placeId}', async (req) => {
    const device = await authenticate(req);
    const placeId = req.params['placeId'] ?? '';
    // A malformed id can never match, so it is the same 404 as an unknown one.
    if (!UUID.test(placeId) || !(await deps.places.remove(device.id, placeId))) {
      throw new HttpError(404, 'not_found', 'Place not found');
    }
    return noContent();
  });

  // ---------- Alerts ----------

  router.add('GET', '/alerts', async (req) => {
    const device = await authenticate(req);
    const places = await deps.places.listForDevice(device.id);
    const cells = [...new Set(places.map((p) => p.h3Cell))];
    if (cells.length === 0) return json(200, { alerts: [] });
    const alerts = await deps.alerts.listActiveForCells(cells, deps.clock.now());
    const summaries = alerts.map((a): AlertSummary => {
      const areaCells = new Set(a.area.h3Cells);
      return {
        id: a.id,
        event: a.event,
        relaySeverity: a.relaySeverity,
        state: a.state,
        expires: a.expires,
        headline: a.official.headline,
        matchedPlaceIds: places.filter((p) => areaCells.has(p.h3Cell)).map((p) => p.id),
      };
    });
    return json(200, { alerts: summaries });
  });

  router.add('GET', '/alerts/{alertId}', async (req) => {
    await authenticate(req);
    const alertId = req.params['alertId'] ?? '';
    // Alerts are public information, so no per-device filtering here.
    const alert: Alert | null = UUID.test(alertId) ? await deps.alerts.get(alertId) : null;
    if (!alert) throw new HttpError(404, 'not_found', 'Alert not found');
    // Stored state only reflects the last NWS message; show "expired" once time has passed.
    return json(200, { ...alert, state: effectiveState(alert, deps.clock.now()) });
  });

  // ---------- Check-ins ----------

  router.add('POST', '/alerts/{alertId}/checkins', async (req) => {
    const device = await authenticate(req);
    const body = requireObject(req.body);
    const status = body['status'];
    if (status !== 'ok' && status !== 'need_help') {
      throw new HttpError(400, 'invalid_request', 'status must be "ok"');
    }
    // Reserved for the Pilot. The format allows it, the demo does not.
    if (status === 'need_help') {
      throw new HttpError(400, 'unsupported_status', 'status "need_help" is not supported yet');
    }
    const token = nonEmptyString(body['token'], 'token');
    const idempotencyKey = nonEmptyString(body['idempotencyKey'], 'idempotencyKey');
    if (idempotencyKey.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
      throw new HttpError(400, 'invalid_request', 'idempotencyKey is too long');
    }

    const alertId = req.params['alertId'] ?? '';
    const alert: Alert | null = UUID.test(alertId) ? await deps.alerts.get(alertId) : null;
    if (!alert) throw new HttpError(404, 'not_found', 'Alert not found');

    // One answer for every token problem (malformed, forged, wrong alert/device, expired),
    // so a caller learns nothing about which check failed.
    const verified = verifyCheckInToken(
      token,
      { alertId: alert.id, deviceId: device.id, now: deps.clock.now() },
      deps.tokenKeys,
    );
    if (!verified.ok) throw new HttpError(403, 'invalid_token', 'Invalid check-in token');

    // The check-in counts toward the cell of the first saved place that this alert covers.
    const areaCells = new Set(alert.area.h3Cells);
    const place = (await deps.places.listForDevice(device.id)).find((p) => areaCells.has(p.h3Cell));
    if (!place) throw new HttpError(403, 'not_in_area', 'This alert does not cover your places');

    const checkIn: CheckIn = {
      alertId: alert.id,
      deviceId: device.id,
      status,
      checkedInAt: deps.clock.now().toISOString(),
      token,
      idempotencyKey,
      h3Cell: place.h3Cell,
    };
    if (await deps.checkins.save(checkIn)) return json(200, { checkedInAt: checkIn.checkedInAt });

    // Repeat key: answer with the original time, so retries after a lost response are harmless.
    const original = await deps.checkins.get(alert.id, idempotencyKey);
    if (!original) throw new Error('Repeated check-in could not be read back');
    if (original.deviceId !== device.id) {
      throw new HttpError(409, 'idempotency_conflict', 'idempotencyKey is already in use');
    }
    return json(200, { checkedInAt: original.checkedInAt });
  });

  return (event) => router.route(event);
}
