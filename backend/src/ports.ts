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
  /**
   * Feed reconciliation bookkeeping (optional, kept per Relay alert id, outside the shared Alert
   * schema). `markEnded` records that reconciliation ended the alert because NWS stopped listing
   * it; `getEnded` returns that record until `clearEnded` (the alert was restored, or a real
   * Cancel arrived and the record no longer applies).
   */
  markEnded?(alertId: string, marker: EndedMarker): Promise<void>;
  getEnded?(alertId: string): Promise<EndedMarker | null>;
  clearEnded?(alertId: string): Promise<void>;
}

/** Why and when reconciliation ended an alert, and the state to restore it to. */
export interface EndedMarker {
  endedBy: 'feed_absence';
  priorState: 'active' | 'updated';
  endedAt: string;
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
  /**
   * Android only: the Firebase project (adapters/push/fcm-projects.ts) the FCM token was
   * registered with. Null/absent: the default project, or not an Android target.
   */
  pushProject?: string | null;
  language: 'en' | 'es';
  createdAt: string;
}

/**
 * A saved place. Data minimization: the exact coordinates of a place are NEVER stored. The API
 * computes the H3 cell (resolution 7, about 5 km2) and discards the point; the cell is all that
 * alert matching uses. Stores may keep the cell's CENTER (never the original point).
 */
export interface Place {
  id: string;
  deviceId: string;
  /**
   * What the person calls it. At most MAX_LABEL_LENGTH characters. Labels should be generic
   * (the app suggests Home, Work or School): they are stored, so never an address or a name.
   */
  label: string;
  /**
   * Only when the place was added by address: the street NAME and locality, never a house
   * number (PRIV-5; see geo/coarse-address.ts). Never for a pin.
   */
  shortAddress?: string;
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

/** Erasure of a device (DELETE /devices/me). Implemented by the real and in-memory stores. */
export interface DeviceErasure {
  /**
   * Erase the device row (its places go with it where the store cascades) and record a
   * tombstone, in one atomic step, so its credential stops working even when it is stateless.
   * Idempotent. Returns true when a device row was deleted.
   */
  deleteDevice(deviceId: string, deletedAt: Date): Promise<boolean>;
  /** Ids of every erased device (the tombstones). Small: one row per erasure request. */
  listDeletedDevices(): Promise<string[]>;
}

/** Erasure of a device's places (DELETE /devices/me). */
export interface PlaceErasure {
  /** Permanent delete of every place of a device. Returns how many were removed. */
  removeAllForDevice(deviceId: string): Promise<number>;
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

/** Longest label a place may have (see Place.label). */
export const MAX_LABEL_LENGTH = 60;

/** Address to coordinates (Amazon Location Service in AWS). */
export interface Geocoder {
  geocode(address: string): Promise<{
    latitude: number;
    longitude: number;
    /** Coarse: street name and locality, never a house number (see geo/coarse-address.ts). */
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
  /**
   * Record that the push went out. The key is remembered until `expiresAt`. `detail.members`
   * (scale sender, G5): the member set (scale/fanout/member-set.ts) of the recipients this batch
   * sent, stored on the item for late join; like the state, it is never overwritten by another
   * attempt once the key is sent.
   */
  markSent(key: string, expiresAt: Date, detail?: { members?: Uint8Array }): Promise<void>;
  /**
   * The shard worker's per-cell (or per dense part) plan record (G5): marks `key` sent and stores
   * the planned recipients' member set, replacing any earlier record of the key (a later plan's
   * read is newer). `members` null: the set was over MAX_SET_BYTES and is not stored.
   * `detail.targets` (G5b): the set of (device, push target) pairs the plan's batches carry. It is
   * stored only by the FIRST plan of the key; any later plan's write removes it, so a record with
   * targets means exactly one plan wrote it (and the shard worker writes it before enqueueing any
   * of those batches, so a sent batch of the key is that plan's).
   */
  markPlanned?(
    key: string,
    members: Uint8Array | null,
    expiresAt: Date,
    detail?: { targets?: Uint8Array },
  ): Promise<void>;
  /**
   * peek() plus the stored member set, in the same single read: `members` is undefined when the
   * item has none (absent, leased, written by an older version, or over the size cap).
   */
  inspect?(key: string): Promise<KeyInspection>;
  /** Give a claim back after a retryable failure, so the next attempt can send. */
  release(key: string): Promise<void>;
  /**
   * Push the lease end of our own live claim out to `leaseUntil`. Resolves true if extended;
   * false if the claim is no longer ours (state is not 'leased', or a different attempt owns it),
   * in which case the caller must stop work on the key.
   */
  extend?(key: string, leaseUntil: Date): Promise<boolean>;
  /**
   * Read a key without touching it: 'sent' (marked sent, or written before leases existed),
   * 'leased' (another attempt holds a live lease) or 'absent' (never claimed, released, or its
   * lease ran out unsent). A strongly consistent read. Unlike claim-then-release it never makes
   * the real sender see 'leased', so a probe cannot delay an urgent batch.
   */
  peek?(key: string): Promise<'absent' | 'leased' | 'sent'>;
  /**
   * Progress of a batch sent in sub-chunks (optional; the sender works without it). Records,
   * under our own live claim only, that sub-chunk `progress.chunk` finished: every recipient in it
   * went out except the offsets in `progress.redo`. Resolves false if the claim is no longer ours
   * (the caller must stop). Progress outlives an expired lease and a release, so the next claim
   * of the key can skip what already went out; markSent ends it.
   */
  recordChunk?(key: string, progress: ChunkProgress): Promise<boolean>;
  /**
   * After claim() returned 'claimed': the sub-chunk progress earlier attempts on this key
   * recorded (empty for a fresh key). Several entries for one chunk mean the union of what went
   * out: a recipient is done if any entry leaves it out of `redo`.
   */
  claimedProgress?(key: string): Promise<ChunkProgress[]>;
}

/** What IdempotencyStore.inspect returns. */
export interface KeyInspection {
  state: 'absent' | 'leased' | 'sent';
  members?: Uint8Array;
  /** A plan record's (device, target) set, when exactly one plan wrote it (markPlanned). */
  targets?: Uint8Array;
}

/** One finished sub-chunk: offsets (within the chunk) that still need sending. See recordChunk. */
export interface ChunkProgress {
  /**
   * Fingerprint of the batch's ordered recipient list (and chunk size) the offsets refer to. A
   * batch key can come back with a different list (re-plan after devices were added or removed),
   * so progress whose fingerprint does not match the batch being sent must be ignored.
   */
  set: string;
  chunk: number;
  redo: number[];
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
  /**
   * Saves the check-in unless the idempotency key was seen. Returns false for a repeat.
   * `retainUntil` is when the row may be deleted (the alert's end plus the retention period);
   * the store writes it as the table's TTL so check-ins are not kept forever.
   */
  save(checkIn: CheckIn, retainUntil?: Date): Promise<boolean>;
}
