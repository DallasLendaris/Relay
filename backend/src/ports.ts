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
  /** Alerts with state active or updated whose h3Cells include any of these cells. */
  listActiveForCells(cells: string[]): Promise<Alert[]>;
}

// ---------- Devices and places ----------

export interface Device {
  id: string;
  /** SHA-256 hex of the device credential. The credential itself is never stored. */
  credentialHash: string;
  expoPushToken: string | null;
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
}

export interface PlaceStore {
  add(place: Place): Promise<void>;
  listForDevice(deviceId: string): Promise<Place[]>;
  /** Permanent delete. Returns false if no such place for that device. */
  remove(deviceId: string, placeId: string): Promise<boolean>;
  /** Every place whose h3Cell is in the list. Used to match an alert to devices. */
  listForCells(cells: string[]): Promise<Place[]>;
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

/** Ensures nobody is notified twice for the same alert (DynamoDB conditional put in AWS). */
export interface IdempotencyStore {
  /** Atomically claim a key. Returns false if it was already claimed. */
  claim(key: string, expiresAt: Date): Promise<boolean>;
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
