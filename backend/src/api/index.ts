import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import type { Alert, CheckIn, components } from '@relay/shared';
import { cellForPoint } from '../geo/index.js';
import {
  QUEUE_FOR_SEVERITY,
  type CellIndex,
  type PushTarget,
  type SendBatch,
  type SendQueue,
  type SendQueueName,
} from '../scale/contracts.js';
import type { IndexRepairMessage, IndexRepairQueue } from '../scale/cell-index/repair-queue.js';
import {
  deviceCellsOf,
  syncDeviceCells,
  type DeviceTruth,
} from '../scale/cell-index/sync-device-cells.js';
import { primaryCellOf } from '../scale/fanout/primary-cell.js';
import { targetOf } from '../scale/cell-index/util.js';
import { alertEndTime } from '../delivery/eligibility.js';
import {
  createCheckInToken,
  createDeviceCredential,
  verifyCheckInToken,
  verifyDeviceCredential,
  type TokenKey,
} from '../tokens/index.js';
import type { StoredCheckIn } from '../scale/checkins/types.js';
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
  /** The device's stored check-in for this alert, or null. */
  get(alertId: string, deviceId: string): Promise<StoredCheckIn | null>;
}

export interface ApiDeps {
  devices: DeviceStore;
  places: PlaceStore;
  alerts: AlertStore;
  geocoder: Geocoder;
  /** The port plus a lookup by device (adapter classes have it; the port does not). */
  checkins: CheckInStore & CheckInLookup;
  /** Check-in token keys by key id (kid -> secret). */
  tokenKeys: Map<string, string>;
  /** DynamoDB cell index, written through from here. Aurora stays the record. */
  cellIndex: CellIndex;
  /**
   * Where a failed cell-index write is queued for the repair Lambda (INDEX_REPAIR_QUEUE_URL).
   * Without it a failure is only logged, and the backfill reconciles.
   */
  repairQueue?: IndexRepairQueue;
  /**
   * Late joiners: a device that registers a place or a push target while an alert is already
   * active missed the fan-out (re-planning skips batches already sent), so the API sends it the
   * active alerts itself, as single-recipient batches. Needs the send queues and a token key.
   */
  lateJoin?: { sendQueue: SendQueue; tokenKey: TokenKey };
  /**
   * Device credential keys by key id, for stateless credentials (see tokens/deviceCredential.ts).
   * Without it, only old two-part credentials verify.
   */
  deviceCredentialKeys?: Map<string, string>;
  /** When set, POST /devices issues stateless credentials signed with this key. */
  deviceCredentialKey?: TokenKey;
  clock: Clock;
  /** Returns a new UUID. Injected so tests are deterministic. */
  newId: () => string;
}

export const MAX_PLACES_PER_DEVICE = 5;
const MAX_IDEMPOTENCY_KEY_LENGTH = 200;

type PlaceBody = components['schemas']['Place'];
type AlertSummary = components['schemas']['AlertSummary'];

const PUSH_TOKEN = /^Expo(nent)?PushToken\[[^\]\s]+\]$/;
const APNS_TOKEN = /^[0-9a-fA-F]{64,200}$/;
const FCM_TOKEN = /^[A-Za-z0-9_:-]{100,4096}$/;
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

/** Reads the old {expoPushToken} or the new {platform, token} body. Throws 400 when invalid. */
function parsePushTarget(body: Record<string, unknown>): PushTarget {
  const legacy = body['expoPushToken'] !== undefined;
  const native = body['platform'] !== undefined || body['token'] !== undefined;
  if (legacy === native) {
    throw new HttpError(
      400,
      'invalid_request',
      'Provide either expoPushToken, or platform and token',
    );
  }
  const bad = (message: string) => new HttpError(400, 'invalid_push_token', message);
  if (legacy) {
    const token = body['expoPushToken'];
    if (typeof token !== 'string' || !PUSH_TOKEN.test(token)) {
      throw bad('expoPushToken must look like ExponentPushToken[...]');
    }
    return { platform: 'expo', token };
  }
  const platform = body['platform'];
  const token = body['token'];
  if (platform !== 'ios' && platform !== 'android' && platform !== 'expo') {
    throw new HttpError(400, 'invalid_request', 'platform must be "ios", "android" or "expo"');
  }
  if (typeof token !== 'string') throw bad('token must be a string');
  if (platform === 'ios') {
    if (!APNS_TOKEN.test(token)) throw bad('ios token must be 64 to 200 hex characters');
    return { platform, token: token.toLowerCase() }; // hex is case-insensitive; one spelling
  }
  if (platform === 'android') {
    if (!FCM_TOKEN.test(token)) throw bad('android token must be 100 to 4096 token characters');
    return { platform, token };
  }
  if (!PUSH_TOKEN.test(token)) throw bad('expo token must look like ExponentPushToken[...]');
  return { platform, token };
}

/** Builds the route function: `route(event) -> {statusCode, headers, body}`. */
export function createApi(deps: ApiDeps): (event: ApiEvent) => Promise<ApiResult> {
  const router = new Router();

  /** Queues repairs for the repair Lambda. Never throws: the original write already failed. */
  async function enqueueRepair(messages: IndexRepairMessage[]): Promise<void> {
    if (!deps.repairQueue) return;
    try {
      await deps.repairQueue.enqueue(messages);
    } catch (err) {
      console.error(
        JSON.stringify({
          level: 'error',
          msg: 'cellindex.repair_enqueue_failed',
          error: err instanceof Error ? err.name : 'UnknownError',
        }),
      );
    }
  }

  /**
   * Runs a cell index write. A failure never fails the request (Aurora is the record); it is
   * logged and the repair messages are queued so the repair Lambda redoes it from Aurora's truth.
   */
  async function writeThrough(
    op: string,
    fn: () => Promise<void>,
    repairs: IndexRepairMessage[],
  ): Promise<void> {
    try {
      await fn();
    } catch (err) {
      // Never log tokens or coordinates: only the operation and the error's class.
      console.warn(
        JSON.stringify({
          level: 'warn',
          msg: 'cellindex.write_failed',
          op,
          error: err instanceof Error ? err.name : 'UnknownError',
          repairQueued: deps.repairQueue !== undefined,
        }),
      );
      await enqueueRepair(repairs);
    }
  }

  /**
   * Sends the alerts already active over `cells` to one device that just registered a place or a
   * push target. The batch key is per device and alert, so repeating this (a second place, a token
   * refresh) is deduplicated by the sender's claim. Best effort: a failure never fails the API
   * call. A device registered a moment before the planner reads its cell can get both this and
   * the planned batch; both carry the alert id as collapse key, so the phone shows one.
   */
  async function sendLateJoinAlerts(
    deviceId: string,
    target: PushTarget | null,
    language: 'en' | 'es',
    cells: string[],
    /** ALL the device's cells (sorted). Defaults to `cells` (fail open: sends). */
    deviceCells: string[] = cells,
  ): Promise<void> {
    const late = deps.lateJoin;
    if (!late || !target || cells.length === 0) return;
    try {
      const now = deps.clock.now();
      const active = await deps.alerts.listActiveForCells(cells, now);
      const batches: Record<SendQueueName, SendBatch[]> = { urgent: [], standard: [] };
      for (const alert of active) {
        const queue = QUEUE_FOR_SEVERITY[alert.relaySeverity];
        if (!queue) continue; // statements are in-app only
        if (alert.state !== 'active' && alert.state !== 'updated') continue;
        const expiresAt = alertEndTime(alert);
        if (expiresAt.getTime() <= now.getTime()) continue;
        const areaCells = new Set(alert.area.h3Cells);
        if (!cells.some((c) => areaCells.has(c))) continue;
        // Same primary-cell rule as the shard worker: the device is sent from the smallest of its
        // cells in the alert area. A new place in any other cell adds no push: the planned
        // fan-out already reaches the device through its primary cell.
        const h3Cell = primaryCellOf(deviceCells, areaCells);
        if (h3Cell === undefined || !cells.includes(h3Cell)) continue;
        batches[queue].push({
          alertId: alert.id,
          sourceId: alert.sourceId,
          relaySeverity: alert.relaySeverity,
          batchKey: `${alert.id}:${alert.relaySeverity}:late:${deviceId}`,
          recipients: [
            {
              deviceId,
              target,
              language,
              checkInToken: createCheckInToken(
                { alertId: alert.id, deviceId, h3Cell, expiresAt },
                late.tokenKey,
              ),
            },
          ],
          attempt: 0,
        });
      }
      for (const queue of ['urgent', 'standard'] as const) {
        if (batches[queue].length > 0) await late.sendQueue.enqueue(queue, batches[queue]);
      }
    } catch (err) {
      console.warn(
        JSON.stringify({
          level: 'warn',
          msg: 'latejoin.failed',
          error: err instanceof Error ? err.name : 'UnknownError',
        }),
      );
    }
  }

  /** Points every cell index entry of this device at a new target. */
  async function syncDevice(
    deviceId: string,
    target: PushTarget | null,
    language: 'en' | 'es',
  ): Promise<void> {
    const places = await deps.places.listForDevice(deviceId);
    if (places.length === 0) return;
    await deps.cellIndex.updateDevice(
      deviceId,
      places.map((p) => ({ h3Cell: p.h3Cell, placeId: p.id })),
      { target, language },
    );
  }

  const allPlaces = (deviceId: string): IndexRepairMessage => ({
    op: 'update',
    h3Cell: '',
    deviceId,
    placeId: '',
  });

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

  /**
   * Device id for the check-in route, with NO database read for a stateless credential
   * (`<deviceId>.<kid>.<mac>`): the check-in route is the only one a million phones hit at once.
   * An old two-part credential still goes through the hash lookup, so existing installs work.
   */
  async function authenticateCheckIn(req: Request): Promise<string> {
    const header = req.headers['authorization'];
    const credential = header ? /^Bearer (\S+)$/.exec(header)?.[1] : undefined;
    if (credential?.split('.').length === 3) {
      const deviceId = verifyDeviceCredential(credential, deps.deviceCredentialKeys ?? new Map());
      if (deviceId) return deviceId;
      throw new HttpError(401, 'unauthorized', 'Missing or invalid credentials');
    }
    return (await authenticate(req)).id;
  }

  // ---------- Devices ----------

  router.add('POST', '/devices', async (req) => {
    if (req.body !== undefined && Object.keys(requireObject(req.body)).length > 0) {
      throw new HttpError(400, 'invalid_request', 'Body must be an empty object');
    }
    const id = deps.newId();
    const credential = deps.deviceCredentialKey
      ? createDeviceCredential(id, deps.deviceCredentialKey)
      : `${id}.${randomBytes(32).toString('base64url')}`;
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
    const language = body['language'];
    if (language !== 'en' && language !== 'es') {
      throw new HttpError(400, 'invalid_request', 'language must be "en" or "es"');
    }
    const target = parsePushTarget(body);
    // Moves the target here if another (reinstalled) device still holds it.
    const { displaced } = await deps.devices.assignPushTarget(device.id, target, language);
    // Keep the cell index in step. A failure here never fails the request (Aurora is the
    // record); the failed update is queued for the repair Lambda.
    await writeThrough('update_device', () => syncDevice(device.id, target, language), [
      allPlaces(device.id),
    ]);
    for (const otherId of displaced) {
      await writeThrough(
        'update_device',
        async () => {
          const other = await deps.devices.get(otherId);
          if (other) await syncDevice(otherId, null, other.language);
        },
        [allPlaces(otherId)],
      );
    }
    // A device that just got a target missed any alert that is already active: send it now.
    if (deps.lateJoin) {
      try {
        const cells = [
          ...new Set((await deps.places.listForDevice(device.id)).map((p) => p.h3Cell)),
        ].sort();
        await sendLateJoinAlerts(device.id, target, language, cells, cells);
      } catch {
        /* best effort */
      }
    }
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
    // Write the new entry FIRST, then rewrite deviceCells on the device's other entries
    // (syncDeviceCells). It reads the device's CURRENT target and places from Aurora after the
    // place is saved (the row loaded for authentication is from the start of the request) and
    // re-reads afterwards, so a push token registered in between is caught and rewritten.
    const seen: { truth?: DeviceTruth } = {};
    const repairPut: IndexRepairMessage = {
      op: 'put',
      h3Cell: place.h3Cell,
      deviceId: device.id,
      placeId: place.id,
    };
    await writeThrough(
      'put',
      () =>
        syncDeviceCells(deps.cellIndex, {
          deviceId: device.id,
          added: { placeId: place.id },
          load: async () => {
            const current = (await deps.devices.get(device.id)) ?? device;
            const places = await deps.places.listForDevice(device.id);
            seen.truth = {
              target: targetOf(current),
              language: current.language,
              places: places.map((p) => ({ id: p.id, h3Cell: p.h3Cell })),
            };
            return seen.truth;
          },
        }),
      [repairPut],
    );
    const truth = seen.truth;
    await sendLateJoinAlerts(
      device.id,
      truth ? truth.target : targetOf(device),
      truth ? truth.language : device.language,
      [place.h3Cell],
      truth ? deviceCellsOf(truth.places) : [place.h3Cell],
    );
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
    const place = UUID.test(placeId)
      ? (await deps.places.listForDevice(device.id)).find((p) => p.id === placeId)
      : undefined;
    if (!place || !(await deps.places.remove(device.id, placeId))) {
      throw new HttpError(404, 'not_found', 'Place not found');
    }
    // Rewrite the remaining entries without this cell FIRST, then delete the removed entry.
    await writeThrough(
      'remove',
      () =>
        syncDeviceCells(deps.cellIndex, {
          deviceId: device.id,
          removed: { h3Cell: place.h3Cell, placeId: place.id },
          load: async () => {
            const current = (await deps.devices.get(device.id)) ?? device;
            const places = await deps.places.listForDevice(device.id);
            return {
              target: targetOf(current),
              language: current.language,
              places: places.map((p) => ({ id: p.id, h3Cell: p.h3Cell })),
            };
          },
        }),
      [{ op: 'remove', h3Cell: place.h3Cell, deviceId: device.id, placeId: place.id }],
    );
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
    const deviceId = await authenticateCheckIn(req);
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
    // A malformed id can never match an alert (no lookup needed to know that).
    if (!UUID.test(alertId)) throw new HttpError(404, 'not_found', 'Alert not found');

    // One answer for every token problem (malformed, forged, wrong alert/device, expired),
    // so a caller learns nothing about which check failed.
    const verified = verifyCheckInToken(
      token,
      { alertId, deviceId, now: deps.clock.now() },
      deps.tokenKeys,
    );
    if (!verified.ok) throw new HttpError(403, 'invalid_token', 'Invalid check-in token');

    // A v2 token is signed for this alert, device and cell, so it proves the device was sent this
    // alert for this cell: no alert or place lookup, and no database read at all.
    let h3Cell = verified.h3Cell;
    if (h3Cell === null) {
      // Legacy v1 token (no cell): fall back to the old lookups. Only tokens issued before the
      // v2 rollout reach this, and they stop working when their alerts expire.
      const alert: Alert | null = await deps.alerts.get(alertId);
      if (!alert) throw new HttpError(404, 'not_found', 'Alert not found');
      // The check-in counts toward the cell of the first saved place that this alert covers.
      const areaCells = new Set(alert.area.h3Cells);
      const place = (await deps.places.listForDevice(deviceId)).find((p) =>
        areaCells.has(p.h3Cell),
      );
      if (!place) throw new HttpError(403, 'not_in_area', 'This alert does not cover your places');
      h3Cell = place.h3Cell;
    }

    const checkIn: CheckIn = {
      alertId,
      deviceId,
      status,
      checkedInAt: deps.clock.now().toISOString(),
      token,
      idempotencyKey,
      h3Cell,
    };
    if (await deps.checkins.save(checkIn)) return json(200, { checkedInAt: checkIn.checkedInAt });

    // This device already checked in for this alert (a retry, or a new idempotencyKey).
    // Only the first check-in counts; answer with its original time.
    const original = await deps.checkins.get(alertId, deviceId);
    if (!original) throw new Error('Repeated check-in could not be read back');
    return json(200, { checkedInAt: original.checkedInAt });
  });

  return (event) => router.route(event);
}
