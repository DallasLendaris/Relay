/**
 * Ports: the interfaces between the alert engine's logic and the outside world
 * (databases, queues, AWS services, the clock). Engine code depends only on these.
 * Each port has an in-memory implementation in src/adapters/memory/ for tests and
 * local runs, and an AWS implementation in src/adapters/aws/ added as services exist.
 *
 * Changing a port affects several modules. Keep changes small and update every
 * implementation in the same pull request.
 */
import type { Alert, CheckIn } from '@relay/shared';
import type { PushPlatform, PushTarget } from './scale/contracts.js';
import type { OutboxEntry } from './scale/cell-index/outbox.js';

/** Injectable clock so tests control time. */
export interface Clock {
  now(): Date;
}

// ---------- Ingest ----------

/** Raw NWS responses, stored for replay and reports (S3 in AWS). Ticket AE-02. */
export interface RawArchive {
  /** Store one raw response body. Key is chosen by the implementation (by date). */
  put(body: string, fetchedAt: Date): Promise<string>;
}

/** What ingest has already seen, for dedupe (DynamoDB `alerts-state` in AWS). */
export interface SeenStore {
  /** Returns true if this source message was recorded before. */
  has(sourceId: string): Promise<boolean>;
  /** Record a source message as processed. */
  mark(sourceId: string, sent: string): Promise<void>;
}

/** The current record of every alert (Aurora in AWS). */
export interface AlertStore {
  get(id: string): Promise<Alert | null>;
  /** Find the Relay alert whose chain contains this NWS source message id. */
  findBySourceId(sourceId: string): Promise<Alert | null>;
  /** Insert or replace by Alert.id. */
  save(alert: Alert): Promise<void>;
  /**
   * Alerts with state active or updated whose h3Cells include any of these cells,
   * and that have not ended yet (now is before the later of expires and ends).
   */
  listActiveForCells(cells: string[], now: Date): Promise<Alert[]>;
}

// ---------- Devices and places ----------

export interface Device {
  id: string;
  /** SHA-256 hex of the device credential. The credential itself is never stored. */
  credentialHash: string;
  /** Legacy Expo path. Equals pushToken whenever pushPlatform is 'expo', otherwise null. */
  expoPushToken: string | null;
  /**
   * Native push target (APNs, FCM, or Expo). Both are null, or both set. Optional so code
   * that predates native push keeps compiling; absent means "derive from expoPushToken".
   */
  pushPlatform?: PushPlatform | null;
  pushToken?: string | null;
  language: 'en' | 'es';
  createdAt: string;
}

export interface Place {
  id: string;
  deviceId: string;
  label: string;
  shortAddress: string;
  latitude: number;
  longitude: number;
  h3Cell: string;
  createdAt: string;
}

export interface DeviceStore {
  create(device: Device): Promise<void>;
  get(id: string): Promise<Device | null>;
  update(device: Device): Promise<void>;
  /**
   * Give this push token to this device and remove it from any other device that had it,
   * so a reinstalled app never makes one phone receive two pushes.
   */
  assignPushToken(deviceId: string, token: string, language: 'en' | 'es'): Promise<void>;
  /**
   * Give this push target to this device and remove it from any other device that held it.
   * For platform 'expo' this also sets expoPushToken, so the Expo path keeps working.
   * Returns the ids of the devices that lost the target (their cell index entries are stale).
   */
  assignPushTarget(
    deviceId: string,
    target: PushTarget,
    language: 'en' | 'es',
    /**
     * `outbox: true`: in the same transaction, write an "update" cell-index outbox row for this
     * device and for each displaced device that has places. Their ids come back in `outboxIds`
     * (by device id) so the caller can delete them after its write-through succeeds.
     */
    opts?: { outbox?: boolean },
  ): Promise<{ displaced: string[]; outboxIds?: Record<string, string> }>;
  /** Forget a push token reported as dead (Expo DeviceNotRegistered, or APNs/FCM unregistered). */
  clearPushToken(token: string): Promise<void>;
}

export interface PlaceStore {
  /**
   * `opts.outbox`: write this cell-index outbox row in the SAME transaction as the insert
   * (stores that cannot do transactions, such as the in-memory one, ignore it).
   */
  add(place: Place, opts?: { outbox?: OutboxEntry }): Promise<void>;
  listForDevice(deviceId: string): Promise<Place[]>;
  /** Permanent delete. Returns false if no such place for that device. */
  remove(deviceId: string, placeId: string, opts?: { outbox?: OutboxEntry }): Promise<boolean>;
  /** Every place whose h3Cell is in the list. Used to match an alert to devices. */
  listForCells(cells: string[]): Promise<Place[]>;
  /**
   * Lightweight version of listForCells for delivery fan-out: only the ids and the matched
   * cell (bound into the v2 check-in token), so large matches stay well under the Data API's
   * response size limit.
   */
  listMatchesForCells(
    cells: string[],
  ): Promise<Array<{ placeId: string; deviceId: string; h3Cell: string }>>;
}

/** Address to coordinates (Amazon Location Service in AWS). */
export interface Geocoder {
  geocode(address: string): Promise<{
    latitude: number;
    longitude: number;
    shortAddress: string;
    /** Two-letter US state code, used to enforce California only. */
    state: string;
  } | null>;
}

// ---------- Delivery ----------

/** One push to send, as queued on SQS. */
export interface DeliveryJob {
  alertId: string;
  deviceId: string;
  expoPushToken: string;
  language: 'en' | 'es';
  checkInToken: string;
  expiresAt: string;
  /** Lets the phone group or replace notifications for the same alert chain. */
  groupingKey: string;
}

export interface DeliveryQueue {
  enqueue(jobs: DeliveryJob[]): Promise<void>;
}

/**
 * Ensures nobody is notified twice for the same alert (DynamoDB conditional put in AWS).
 * A claim is a lease: if the worker dies before sending, the lease runs out and a retry can
 * claim again. Only markSent makes the key permanent (until its expiry).
 */
export interface IdempotencyStore {
  /**
   * Atomically claim a key until `leaseUntil`. Succeeds if the key is absent, or its lease
   * has expired and it was never marked sent. 'sent' = already delivered; 'leased' = another
   * attempt holds a live lease.
   */
  claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'>;
  /** Record that the push went out. The key is remembered until `expiresAt`. */
  markSent(key: string, expiresAt: Date): Promise<void>;
  /** Give a claim back after a retryable failure, so the next attempt can send. */
  release(key: string): Promise<void>;
  /**
   * Push the lease end of our own live claim out to `leaseUntil`. Resolves true if extended;
   * false if the claim is no longer ours (state is not 'leased', or a different attempt owns it),
   * in which case the caller must stop work on the key.
   */
  extend?(key: string, leaseUntil: Date): Promise<boolean>;
}

export interface PushMessage {
  to: string;
  title: string;
  body: string;
  data: Record<string, string>;
  /** Expo channel / category so the "I'm okay" action button shows. */
  categoryId?: string;
}

export interface PushSender {
  send(messages: PushMessage[]): Promise<Array<{ ok: true } | { ok: false; error: string }>>;
}

// ---------- Check-ins ----------

export interface CheckInStore {
  /** Saves the check-in unless the idempotency key was seen. Returns false for a repeat. */
  save(checkIn: CheckIn): Promise<boolean>;
}
