/** Shared by the t3 coordinator and its worker processes: keys, device classes, arrival curve. */
import { generateDevices, uuidForDevice } from '../../../seed.js';

export const ALERT_ID = '5f0c2a7e-1b3d-4c8e-9a6f-0d2e4b6a8c10';
export const TOKEN_KEY = { id: 't3-token', secret: 't3-token-secret' };
export const DEVICE_CRED_KEY = { id: 't3-cred', secret: 't3-cred-secret' };

/** Share of devices that were erased (tombstoned) before they tap: the API must answer 401. */
export const TOMBSTONED_SHARE = 0.001;
/** Share of devices that send a tampered token: the API must answer 403. */
export const TAMPERED_SHARE = 0.01;
/** Share of the valid devices that tap a second time (a retry with a new idempotency key). */
export const DUPLICATE_SHARE = 0.05;

export type DeviceClass = 'valid' | 'tampered' | 'tombstoned';

/** Deterministic 32-bit hash of (device number, salt). */
export function mix32(i: number, salt: number): number {
  let x = (i ^ Math.imul(salt, 0x9e3779b1)) >>> 0;
  x = Math.imul(x ^ (x >>> 16), 0x85ebca6b);
  x = Math.imul(x ^ (x >>> 13), 0xc2b2ae35);
  return (x ^ (x >>> 16)) >>> 0;
}

const U32 = 4294967296;

export function classOf(i: number): DeviceClass {
  const u = mix32(i, 1) / U32;
  if (u < TOMBSTONED_SHARE) return 'tombstoned';
  if (u < TOMBSTONED_SHARE + TAMPERED_SHARE) return 'tampered';
  return 'valid';
}

export function isDuplicate(i: number): boolean {
  return classOf(i) === 'valid' && mix32(i, 2) / U32 < DUPLICATE_SHARE;
}

export const deviceIdOf = uuidForDevice;

let pool: string[] | undefined;
/** Cells drawn from the seed generator's distribution (urban concentration); see cellOf. */
export function cellPool(): string[] {
  if (!pool) {
    pool = [];
    for (const d of generateDevices({ devices: 20_000, seed: 7 })) {
      const p = d.places[0];
      if (p) pool.push(p.h3Cell);
    }
  }
  return pool;
}

/** The cell device i is matched in (its token's signed cell). */
export function cellOf(i: number): string {
  const p = cellPool();
  return p[mix32(i, 3) % p.length] as string;
}

export interface Segment {
  startSec: number;
  endSec: number;
  /** Share of all devices whose first tap falls in this segment. */
  share: number;
}

/** Scheduled time (s since start) of device i's first tap: devices arrive in index order. */
export function arrivalSec(i: number, n: number, segs: Segment[]): number {
  const f = (i + 0.5) / n;
  let cum = 0;
  for (const s of segs) {
    if (f <= cum + s.share + 1e-12) {
      return s.startSec + ((f - cum) / s.share) * (s.endSec - s.startSec);
    }
    cum += s.share;
  }
  const last = segs[segs.length - 1] as Segment;
  return last.endSec;
}

// ----- latency histogram: 1% relative buckets from 0.01 ms -----
const LOG = Math.log(1.01);
export const HIST_BUCKETS = 2000;
export function bucketOf(ms: number): number {
  const b = Math.floor(Math.log(Math.max(ms, 0.01) * 100) / LOG);
  return b < 0 ? 0 : b >= HIST_BUCKETS ? HIST_BUCKETS - 1 : b;
}
export function percentileFromHist(h: number[], p: number): number {
  let total = 0;
  for (const c of h) total += c;
  if (total === 0) return 0;
  const want = Math.ceil(p * total);
  let acc = 0;
  for (let b = 0; b < h.length; b++) {
    acc += h[b] as number;
    if (acc >= want) return Math.exp((b + 0.5) * LOG) / 100;
  }
  return 0;
}

// ----- messages -----
export interface GenRun {
  type: 'run';
  g: number;
  L: number;
  n: number;
  ports: number[];
  t0Ms: number;
  segments: Segment[];
  dupDelaySec: number;
  maxSockets: number;
}
export interface GenResult {
  type: 'result';
  hist: number[];
  maxMs: number;
  sent: number;
  responses: number;
  ok200: number;
  s401: number;
  s403: number;
  s5xx: number;
  otherStatus: number;
  connErrors: number;
  unexpected: number;
  firstSchedMs: number;
  lastDoneMs: number;
  lastIssuedMs: number;
  maxRssKb: number;
}
export interface ApiDone {
  type: 'done';
  accepted: number;
  duplicateSaves: number;
  requests: number;
  perCell: Array<[string, number]>;
  lastAcceptedMs: number;
  loopDelayP99Ms: number;
  maxRssKb: number;
}
export interface AggDone {
  type: 'done';
  totals: Array<[string, number]>;
  batches: number;
  redeliveredBatches: number;
  failedItems: number;
  records: number;
  markers: number;
  lastBatchDoneMs: number;
  busyMs: number;
  maxQueue: number;
  maxRssKb: number;
}

export const nowMs = (): number => performance.timeOrigin + performance.now();
