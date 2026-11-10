import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import {
  ApnsTokenSchema,
  ExpoTokenSchema,
  FcmTokenSchema,
  LIMITS,
  PlaceAddressSchema,
  PlaceLabelSchema,
  hasControlChars,
  type Alert,
  type CheckIn,
  type components,
} from '@relay/shared';
import { cellForPoint } from '../geo/index.js';
import { coarsenShortAddress } from '../geo/coarse-address.js';
import type { CellIndex, PushTarget } from '../scale/contracts.js';
import type { CellIndexOutbox } from '../scale/cell-index/outbox.js';
import type { LateJoinIntent, LateJoinQueue } from '../scale/latejoin/intent.js';
import { PushTokenInUseError } from '../scale/latejoin/push-target-rules.js';
import type { IndexRepairMessage, IndexRepairQueue } from '../scale/cell-index/repair-queue.js';
import { syncDeviceCells, type DeviceTruth } from '../scale/cell-index/sync-device-cells.js';
import { targetOf } from '../scale/cell-index/util.js';
import { assignFcmProject } from '../adapters/push/fcm-projects.js';
import {
  createDeviceCredential,
  verifyCheckInToken,
  verifyDeviceCredential,
  type TokenKey,
} from '../tokens/index.js';
import {
  checkInTtlSeconds,
  DEFAULT_CHECKIN_RETENTION_DAYS,
  type StoredCheckIn,
} from '../scale/checkins/types.js';
import { effectiveState } from '../ingest/lifecycle.js';
import type {
  AlertStore,
  CheckInStore,
  Clock,
  Device,
  DeviceErasure,
  DeviceStore,
  Geocoder,
  Place,
  PlaceErasure,
  PlaceStore,
} from '../ports.js';
import { MAX_LABEL_LENGTH } from '../ports.js';
import { isInCalifornia } from './california.js';
import { HttpError, Router, json, noContent, type ApiEvent, type ApiResult } from './router.js';
import type { Request } from './router.js';

export type { ApiEvent, ApiResult } from './router.js';

export interface CheckInLookup {
  /** The device's stored check-in for this alert, or null. */
  get(alertId: string, deviceId: string): Promise<StoredCheckIn | null>;
}

export interface CheckInEraser {
  /** Permanently deletes every check-in row of one device. Idempotent. Returns the count. */
  deleteForDevice(deviceId: string): Promise<number>;
}

export interface ApiDeps {
  /**
   * The Firebase projects Android devices are spread over (FCM_PROJECT_IDS, in order; only ever
   * append). POST /devices returns each device's assigned project, and PUT /devices/me/push-token
   * accepts only these as `fcmProject`. Empty or absent: a single project, no assignment.
   */
  fcmProjectIds?: string[];
  devices: DeviceStore & DeviceErasure;
  places: PlaceStore & PlaceErasure;
  alerts: AlertStore;
  geocoder: Geocoder;
  /** The port plus a lookup by device (adapter classes have it; the port does not). */
  checkins: CheckInStore & CheckInLookup & CheckInEraser;
  /**
   * How long after an alert ends its check-in rows are kept (their TTL). Default 30 days;
   * COUNSEL decides the real period (CHECKIN_RETENTION_DAYS in handler.ts).
   */
  checkInRetentionDays?: number;
  /**
   * How long the set of erased devices is cached between reads (default 60 s). See
   * `isDeleted` in createApi for the trade-off.
   */
  tombstoneCacheMs?: number;
  /**
   * Check-in token keys by key id (kid -> secret), for VERIFYING check-ins only.
   *
   * Known limitation: the API reads the token keyring, which also holds the current signing key,
   * because the check-in route has to verify tokens statelessly (no database read). A compromised
   * API could therefore still mint check-in tokens. What it can no longer do is deliver anything:
   * it has no permission on the send queues, so a minted token never reaches a phone. Closing the
   * rest needs asymmetric token signing (verify key only on the API); tracked as a follow-up.
   */
  tokenKeys: Map<string, string>;
  /** DynamoDB cell index, written through from here. Aurora stays the record. */
  cellIndex: CellIndex;
  /**
   * Where a failed cell-index write is queued for the repair Lambda (INDEX_REPAIR_QUEUE_URL).
   * Without it a failure is only logged, and the backfill reconciles.
   */
  repairQueue?: IndexRepairQueue;
  /**
   * Transactional outbox for cell-index writes. When set, every place insert/delete and push-target
   * change writes an outbox row in the same Aurora transaction; the row is deleted here after the
   * write-through succeeds, and the scheduled drainer redoes any that are left.
   */
  outbox?: Pick<CellIndexOutbox, 'remove'>;
  /**
   * Late joiners: a device that registers a place or a push target while an alert is already
   * active missed the fan-out (re-planning skips batches already sent). The API only asks: it
   * enqueues a small LateJoinIntent on a queue that ONLY the late-join Lambda reads. That Lambda
   * re-derives the device, places and alerts from Aurora and does the sending, so the API never
   * builds a SendBatch, never mints a check-in token and has no permission on the send queues.
   */
  lateJoin?: { queue: LateJoinQueue };
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
const MAX_IDEMPOTENCY_KEY_LENGTH = LIMITS.idempotencyKeyMax;

type PlaceBody = components['schemas']['Place'];
type AlertSummary = components['schemas']['AlertSummary'];

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
  // NUL and other control characters never belong in client text; Postgres rejects NUL (a 500).
  if (hasControlChars(v.trim())) {
    throw new HttpError(400, 'invalid_request', `${name} must not contain control characters`);
  }
  return v.trim();
}

/** The Place shape from the spec. Coordinates and deviceId are deliberately left out. */
function toPlaceBody(p: Place): PlaceBody {
  return {
    id: p.id,
    label: p.label,
    ...(p.shortAddress !== undefined ? { shortAddress: p.shortAddress } : {}),
    h3Cell: p.h3Cell,
    createdAt: p.createdAt,
  };
}

/** Reads the old {expoPushToken} or the new {platform, token} body. Throws 400 when invalid. */
function parsePushTarget(
  body: Record<string, unknown>,
  fcmProjectIds: readonly string[],
): PushTarget {
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
    if (typeof token !== 'string' || !ExpoTokenSchema.safeParse(token).success) {
      throw bad('expoPushToken must look like ExponentPushToken[...]');
    }
    return { platform: 'expo', token };
  }
  const platform = body['platform'];
  const token = body['token'];
  // The Firebase project the app registered this FCM token with (see adapters/push/fcm-projects.ts).
  const project = body['fcmProject'];
  if (project !== undefined) {
    if (platform !== 'android') {
      throw new HttpError(400, 'invalid_request', 'fcmProject is only for platform "android"');
    }
    if (typeof project !== 'string' || !fcmProjectIds.includes(project)) {
      throw new HttpError(400, 'invalid_fcm_project', 'fcmProject is not a configured project');
    }
  }
  if (platform !== 'ios' && platform !== 'android' && platform !== 'expo') {
    throw new HttpError(400, 'invalid_request', 'platform must be "ios", "android" or "expo"');
  }
  if (typeof token !== 'string') throw bad('token must be a string');
  if (platform === 'ios') {
    if (!ApnsTokenSchema.safeParse(token).success)
      throw bad('ios token must be 64 to 200 hex characters');
    return { platform, token: token.toLowerCase() }; // hex is case-insensitive; one spelling
  }
  if (platform === 'android') {
    if (!FcmTokenSchema.safeParse(token).success)
      throw bad('android token must be 100 to 4096 token characters');
    return { platform, token, ...(project !== undefined ? { project } : {}) };
  }
  if (!ExpoTokenSchema.safeParse(token).success)
    throw bad('expo token must look like ExponentPushToken[...]');
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
    /** Outbox rows written with the Aurora change: deleted on success, kept for the drainer on failure. */
    outboxIds: string[] = [],
  ): Promise<void> {
    try {
      await fn();
      if (deps.outbox && outboxIds.length > 0) {
        try {
          await deps.outbox.remove(outboxIds);
        } catch {
          // The write-through is done; the drainer redoes it harmlessly and deletes the row.
        }
      }
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
   * Asks the late-join Lambda to check this device for active alerts. Best effort: a failure
   * never fails the API call (the device just waits for the next alert or a retry). Only called
   * for a device that has a push target; the Lambda re-checks everything anyway.
   */
  async function requestLateJoin(intent: LateJoinIntent): Promise<void> {
    if (!deps.lateJoin) return;
    try {
      await deps.lateJoin.queue.enqueue(intent);
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
      if (deviceId && !(await isDeleted(deviceId))) return deviceId;
      throw new HttpError(401, 'unauthorized', 'Missing or invalid credentials');
    }
    return (await authenticate(req)).id;
  }

  /**
   * Has this device been erased? Only the check-in route asks, and only for a stateless
   * credential (which would otherwise verify forever with no database read).
   *
   * Trade-off: the whole tombstone set (a few UUIDs per erasure request) is read at most once per
   * `tombstoneCacheMs` per container, never once per check-in, so the million-phone burst adds a
   * handful of reads instead of a million. In exchange an erased device's credential can still
   * check in for up to that long in a container that has not refreshed yet (default 60 s); that
   * costs one check-in row, which its TTL removes. If a refresh fails, the last known set is
   * used (fail open: an outage of the tombstone table must not stop people from answering
   * "I'm okay"); with no set yet, nothing is denied and the failure is logged.
   */
  const tombstoneTtlMs = deps.tombstoneCacheMs ?? 60_000;
  let tombstones: Set<string> | undefined;
  let tombstonesLoadedAt = 0;
  let tombstoneRefresh: Promise<void> | undefined;
  async function isDeleted(deviceId: string): Promise<boolean> {
    const nowMs = deps.clock.now().getTime();
    if (!tombstones || nowMs - tombstonesLoadedAt >= tombstoneTtlMs) {
      tombstoneRefresh ??= (async () => {
        try {
          tombstones = new Set(await deps.devices.listDeletedDevices());
          tombstonesLoadedAt = nowMs;
        } catch (err) {
          // Do not retry on every request while it is down: back off for one cache period.
          tombstonesLoadedAt = nowMs;
          console.warn(
            JSON.stringify({
              level: 'warn',
              msg: 'tombstones.refresh_failed',
              error: err instanceof Error ? err.name : 'UnknownError',
              code: 'tombstone_refresh_failed',
            }),
          );
        } finally {
          tombstoneRefresh = undefined;
        }
      })();
      await tombstoneRefresh;
    }
    return tombstones?.has(deviceId) ?? false;
  }

  /**
   * Authentication for DELETE /devices/me. Normally the device exists and this is `authenticate`.
   * If an earlier erasure deleted the device row but could not finish (check-ins), the retry
   * arrives with a stateless credential whose device is already tombstoned: that is accepted so
   * the erasure can be finished. Anything else is the usual 401.
   */
  async function authenticateForErase(req: Request): Promise<{ deviceId: string; known: boolean }> {
    try {
      return { deviceId: (await authenticate(req)).id, known: true };
    } catch (err) {
      const header = req.headers['authorization'];
      const credential = header ? /^Bearer (\S+)$/.exec(header)?.[1] : undefined;
      if (err instanceof HttpError && credential?.split('.').length === 3) {
        const id = verifyDeviceCredential(credential, deps.deviceCredentialKeys ?? new Map());
        // Read the tombstones directly (not the cache): this is a rare, deliberate request.
        if (id && (await deps.devices.listDeletedDevices()).includes(id)) {
          return { deviceId: id, known: false };
        }
      }
      throw err;
    }
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
    // The Firebase project this device's app must register its FCM token with (Android). Omitted
    // when the server runs a single project.
    const fcmProject = assignFcmProject(id, deps.fcmProjectIds ?? []);
    return json(201, { deviceId: id, credential, ...(fcmProject ? { fcmProject } : {}) });
  });

  router.add('PUT', '/devices/me/push-token', async (req) => {
    const device = await authenticate(req);
    const body = requireObject(req.body);
    const language = body['language'];
    if (language !== 'en' && language !== 'es') {
      throw new HttpError(400, 'invalid_request', 'language must be "en" or "es"');
    }
    const target = parsePushTarget(body, deps.fcmProjectIds ?? []);
    // Moves the target here if another (reinstalled) device still holds it, within the move rules
    // (scale/latejoin/push-target-rules.ts): otherwise one phone could mint unlimited identities.
    let displaced: string[];
    let outboxIds: Record<string, string> = {};
    try {
      ({ displaced, outboxIds = {} } = await deps.devices.assignPushTarget(
        device.id,
        target,
        language,
        deps.outbox ? { outbox: true } : undefined,
      ));
    } catch (err) {
      if (err instanceof PushTokenInUseError) {
        throw new HttpError(
          409,
          'push_token_in_use',
          'This push token is attached to another active device. Try again later.',
        );
      }
      throw err;
    }
    const outboxFor = (id: string): string[] => (outboxIds[id] ? [outboxIds[id]] : []);
    // Keep the cell index in step. A failure here never fails the request (Aurora is the
    // record); the failed update is queued for the repair Lambda.
    await writeThrough(
      'update_device',
      () => syncDevice(device.id, target, language),
      [allPlaces(device.id)],
      outboxFor(device.id),
    );
    for (const otherId of displaced) {
      await writeThrough(
        'update_device',
        async () => {
          const other = await deps.devices.get(otherId);
          if (other) await syncDevice(otherId, null, other.language);
        },
        [allPlaces(otherId)],
        outboxFor(otherId),
      );
    }
    // A device that just got a target may have missed an active alert: ask the late-join Lambda.
    await requestLateJoin({ deviceId: device.id, reason: 'push_target' });
    return noContent();
  });

  /**
   * Permanent erasure of this device (Gov. Code 11015.5(c): discard personal information on
   * request). Order matters: the cell-index entries go FIRST, so no entry ever points at a
   * device that is gone (an index failure fails the request with the device untouched, and the
   * client retries). Then the places and the device row (with the tombstone, atomically), then
   * the check-in rows. A check-in failure after the device row is gone is retried inline; if it
   * persists the request fails with 500 and the retry (same credential) finishes the job, see
   * authenticateForErase. The rows' TTL is the last backstop.
   */
  router.add('DELETE', '/devices/me', async (req) => {
    const { deviceId, known } = await authenticateForErase(req);
    if (known) {
      const places = await deps.places.listForDevice(deviceId);
      for (const place of places) {
        await deps.cellIndex.remove(place.h3Cell, deviceId, place.id);
      }
      await deps.places.removeAllForDevice(deviceId);
      await deps.devices.deleteDevice(deviceId, deps.clock.now());
      tombstones?.add(deviceId); // this container refuses it at once
    }
    const attempts = 3;
    for (let attempt = 1; ; attempt++) {
      try {
        await deps.checkins.deleteForDevice(deviceId);
        break;
      } catch (err) {
        if (attempt >= attempts) {
          console.error(
            JSON.stringify({
              level: 'error',
              msg: 'device_delete.checkins_incomplete',
              error: err instanceof Error ? err.name : 'UnknownError',
              code: 'checkins_incomplete',
            }),
          );
          throw new Error('check-in erasure incomplete');
        }
        await new Promise((resolve) => setTimeout(resolve, 100 * attempt));
      }
    }
    return noContent();
  });

  // ---------- Places ----------

  router.add('POST', '/places', async (req) => {
    const device = await authenticate(req);
    const body = requireObject(req.body);
    const label = nonEmptyString(body['label'], 'label');
    if (!PlaceLabelSchema.safeParse(label).success) {
      throw new HttpError(
        400,
        'invalid_request',
        `label must be at most ${MAX_LABEL_LENGTH} characters`,
      );
    }
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

    // The exact point only lives in this function: it is used to find the cell (and to check
    // California BEFORE it is thrown away) and is never stored, logged or returned.
    let h3Cell: string;
    let shortAddress: string | undefined;
    if (hasAddress) {
      const address = nonEmptyString(body['address'], 'address');
      // Bounded before the geocoder: Amazon Location bills per request and per long query.
      if (!PlaceAddressSchema.safeParse(address).success) {
        throw new HttpError(
          400,
          'invalid_request',
          `address must be at most ${LIMITS.addressMax} characters`,
        );
      }
      const found = await deps.geocoder.geocode(address);
      if (!found) throw new HttpError(400, 'address_not_found', 'Could not find that address');
      if (found.state !== 'CA') {
        throw new HttpError(422, 'outside_california', 'Relay supports California addresses only');
      }
      h3Cell = cellForPoint(found.latitude, found.longitude);
      // The adapter already returns the coarse form; strip again so no geocoder can leak a number.
      shortAddress = coarsenShortAddress(found.shortAddress);
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
      h3Cell = cellForPoint(lat, lng);
      // No street text for a pin: nothing but the cell is kept.
    }

    const place: Place = {
      id: deps.newId(),
      deviceId: device.id,
      label,
      ...(shortAddress ? { shortAddress } : {}),
      h3Cell,
      createdAt: deps.clock.now().toISOString(),
    };
    const addOutboxId = deps.outbox ? deps.newId() : undefined;
    await deps.places.add(
      place,
      addOutboxId
        ? { outbox: { id: addOutboxId, op: 'put', h3Cell: place.h3Cell, placeId: place.id } }
        : undefined,
    );
    // Write the new entry FIRST, then rewrite deviceCells on the device's other entries
    // (syncDeviceCells). It reads the device's CURRENT target and places from Aurora after the
    // place is saved (the row loaded for authentication is from the start of the request) and
    // re-reads afterwards, so a push token registered in between is caught and rewritten.
    const seen: { truth?: DeviceTruth; erased?: boolean } = {};
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
            const current = await deps.devices.get(device.id);
            // Erased while we were writing (DELETE /devices/me raced this request): the sync then
            // removes what it wrote, so no entry outlives the device.
            if (!current) {
              seen.erased = true;
              return null;
            }
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
      addOutboxId ? [addOutboxId] : [],
    );
    if (seen.erased) {
      // The device no longer exists: nothing to send, and the credential is dead.
      throw new HttpError(401, 'unauthorized', 'Missing or invalid credentials');
    }
    const truth = seen.truth;
    if (truth ? truth.target : targetOf(device)) {
      await requestLateJoin({ deviceId: device.id, placeId: place.id, reason: 'place_added' });
    }
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
    const removeOutboxId = deps.outbox ? deps.newId() : undefined;
    if (
      !place ||
      !(await deps.places.remove(
        device.id,
        placeId,
        removeOutboxId
          ? { outbox: { id: removeOutboxId, op: 'remove', h3Cell: place.h3Cell, placeId } }
          : undefined,
      ))
    ) {
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
      removeOutboxId ? [removeOutboxId] : [],
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
    if (token.length > LIMITS.checkInTokenMax) {
      throw new HttpError(400, 'invalid_request', 'token is too long');
    }
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
    // The row is deleted (DynamoDB TTL) after the alert's end plus the retention period.
    const retainUntil = new Date(
      checkInTtlSeconds(
        verified.expiresAt,
        deps.checkInRetentionDays ?? DEFAULT_CHECKIN_RETENTION_DAYS,
      ) * 1000,
    );
    if (await deps.checkins.save(checkIn, retainUntil))
      return json(200, { checkedInAt: checkIn.checkedInAt });

    // This device already checked in for this alert (a retry, or a new idempotencyKey).
    // Only the first check-in counts; answer with its original time.
    const original = await deps.checkins.get(alertId, deviceId);
    if (!original) throw new Error('Repeated check-in could not be read back');
    return json(200, { checkedInAt: original.checkedInAt });
  });

  return (event) => router.route(event);
}
