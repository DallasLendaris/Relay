/**
 * Seed data: N devices (default 1,000,000) with saved places spread over Santa Clara County,
 * streamed as CellIndexEntry objects. Nothing here holds the whole data set in memory.
 *
 * CLI: `npm run seed -w @relay/loadtest -- --devices 1000000 --seed 1 --out /tmp/cells.ndjson`
 */
import { createReadStream, createWriteStream } from 'node:fs';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import type { Writable } from 'node:stream';
import { latLngToCell } from 'h3-js';
import {
  createRng,
  isMain,
  logJson,
  normal,
  numberArg,
  parseArgs,
  type Rng,
} from './mock-common.js';

// Copy of the stable shapes in backend/src/scale/contracts.ts (kept local so this workspace does
// not depend on the backend's build). If the contract changes, update these.
export type PushPlatform = 'ios' | 'android' | 'expo';
export interface PushTarget {
  platform: PushPlatform;
  token: string;
}
export interface CellIndexEntry {
  h3Cell: string;
  deviceId: string;
  placeId: string;
  target: PushTarget | null;
  language: 'en' | 'es';
}

/** H3 resolution of the cell index. */
export const CELL_RESOLUTION = 7;

/** Santa Clara County bounding box. */
export const COUNTY_BOUNDS = { south: 36.89, north: 37.48, west: -122.2, east: -121.21 };

export interface UrbanCenter {
  name: string;
  lat: number;
  lng: number;
  /** Standard deviation in degrees (0.01 degree is about 1.1 km of latitude). */
  sigma: number;
  /** Share of the urban devices. */
  weight: number;
}

export const URBAN_CENTERS: readonly UrbanCenter[] = [
  { name: 'San Jose', lat: 37.3382, lng: -121.8863, sigma: 0.045, weight: 0.45 },
  { name: 'Santa Clara', lat: 37.3541, lng: -121.9552, sigma: 0.025, weight: 0.15 },
  { name: 'Sunnyvale', lat: 37.3688, lng: -122.0363, sigma: 0.03, weight: 0.2 },
  { name: 'Mountain View', lat: 37.3861, lng: -122.0839, sigma: 0.03, weight: 0.2 },
];

export const URBAN_SHARE = 0.7;
export const IOS_SHARE = 0.55;
export const EN_SHARE = 0.8;
/** Share of devices with more than one place; half of them have 2 places, half 3. */
export const MULTI_PLACE_SHARE = 0.1;

export const DEFAULT_DEVICES = 1_000_000;
export const DEFAULT_SEED = 1;

export interface SeedOptions {
  devices?: number;
  seed?: number;
  /** `dev` (default): `dev-00000042`. `uuid`: a deterministic v4-shaped UUID, as the API issues. */
  deviceIds?: 'dev' | 'uuid';
}

/** Deterministic UUID-shaped id for device number i (matches the API's id check). */
export function uuidForDevice(i: number): string {
  return `00000000-0000-4000-8000-${i.toString(16).padStart(12, '0')}`;
}

export interface SeedPlace {
  placeId: string;
  h3Cell: string;
  /** True when the place was drawn from an urban cluster rather than uniformly. */
  urban: boolean;
}

export interface SeedDevice {
  deviceId: string;
  target: PushTarget;
  language: 'en' | 'es';
  places: SeedPlace[];
}

function inBounds(lat: number, lng: number): boolean {
  return (
    lat >= COUNTY_BOUNDS.south &&
    lat <= COUNTY_BOUNDS.north &&
    lng >= COUNTY_BOUNDS.west &&
    lng <= COUNTY_BOUNDS.east
  );
}

function pickCenter(rng: Rng): UrbanCenter {
  let r = rng();
  for (const c of URBAN_CENTERS) {
    r -= c.weight;
    if (r < 0) return c;
  }
  return URBAN_CENTERS[0] as UrbanCenter;
}

function drawPlace(rng: Rng, placeNo: number): SeedPlace {
  const urban = rng() < URBAN_SHARE;
  let lat: number;
  let lng: number;
  if (urban) {
    const c = pickCenter(rng);
    do {
      lat = c.lat + normal(rng) * c.sigma;
      lng = c.lng + normal(rng) * c.sigma;
    } while (!inBounds(lat, lng));
  } else {
    lat = COUNTY_BOUNDS.south + rng() * (COUNTY_BOUNDS.north - COUNTY_BOUNDS.south);
    lng = COUNTY_BOUNDS.west + rng() * (COUNTY_BOUNDS.east - COUNTY_BOUNDS.west);
  }
  return { placeId: `p${placeNo}`, h3Cell: latLngToCell(lat, lng, CELL_RESOLUTION), urban };
}

const B64URL = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

function word(rng: Rng): number {
  return (rng() * 4294967296) >>> 0;
}

/** 64 hex characters, like an APNs device token (8 random words, 8 hex digits each). */
function apnsToken(rng: Rng): string {
  let s = '';
  for (let i = 0; i < 8; i++) s += word(rng).toString(16).padStart(8, '0');
  return s;
}

/** About 160 characters in the FCM registration token shape (5 base64url characters per word). */
function b64(rng: Rng, chars: number): string {
  let s = '';
  while (s.length < chars) {
    let w = word(rng);
    for (let k = 0; k < 5; k++) {
      s += B64URL[w & 63];
      w >>>= 6;
    }
  }
  return s.slice(0, chars);
}

function fcmToken(rng: Rng): string {
  return `${b64(rng, 22)}:APA91b${b64(rng, 132)}`;
}

/** Streams devices one at a time. Same seed gives the same devices. */
export function* generateDevices(options: SeedOptions = {}): Generator<SeedDevice> {
  const count = options.devices ?? DEFAULT_DEVICES;
  const rng = createRng(options.seed ?? DEFAULT_SEED);
  const uuid = options.deviceIds === 'uuid';
  for (let i = 0; i < count; i++) {
    const ios = rng() < IOS_SHARE;
    const language = rng() < EN_SHARE ? 'en' : 'es';
    const r = rng();
    const placeCount = r < 1 - MULTI_PLACE_SHARE ? 1 : r < 1 - MULTI_PLACE_SHARE / 2 ? 2 : 3;
    const places: SeedPlace[] = [];
    for (let p = 1; p <= placeCount; p++) places.push(drawPlace(rng, p));
    yield {
      deviceId: uuid ? uuidForDevice(i) : `dev-${String(i).padStart(8, '0')}`,
      target: ios
        ? { platform: 'ios', token: apnsToken(rng) }
        : { platform: 'android', token: fcmToken(rng) },
      language,
      places,
    };
  }
}

/** Streams one CellIndexEntry per (device, place). Roughly 1.15 entries per device. */
export function* generateEntries(options: SeedOptions = {}): Generator<CellIndexEntry> {
  for (const d of generateDevices(options)) {
    for (const p of d.places) {
      yield {
        h3Cell: p.h3Cell,
        deviceId: d.deviceId,
        placeId: p.placeId,
        target: d.target,
        language: d.language,
      };
    }
  }
}

// ---------- NDJSON ----------

export interface WriteOptions {
  /** Flush when the pending text reaches this many characters. Default 1 MiB. */
  chunkChars?: number;
}

/** Writes entries as NDJSON in chunks, honouring backpressure. Returns the entry count. */
export async function writeNdjson(
  entries: Iterable<CellIndexEntry>,
  dest: string | Writable,
  options: WriteOptions = {},
): Promise<number> {
  const out = typeof dest === 'string' ? createWriteStream(dest) : dest;
  const limit = options.chunkChars ?? 1 << 20;
  let pending: string[] = [];
  let size = 0;
  let count = 0;
  const flush = async () => {
    if (pending.length === 0) return;
    const text = pending.join('');
    pending = [];
    size = 0;
    if (!out.write(text)) await once(out, 'drain');
  };
  for (const e of entries) {
    const line = JSON.stringify(e) + '\n';
    pending.push(line);
    size += line.length;
    count++;
    if (size >= limit) await flush();
  }
  await flush();
  if (typeof dest === 'string') {
    out.end();
    await once(out, 'finish');
  }
  return count;
}

/** Reads an NDJSON file back as a stream of entries. */
export async function* readNdjson(path: string): AsyncGenerator<CellIndexEntry> {
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.length > 0) yield JSON.parse(line) as CellIndexEntry;
  }
}

// ---------- Stats ----------

export interface SeedSummary {
  devices: number;
  entries: number;
  cellsUsed: number;
  /** Distinct devices per cell. */
  devicesPerCell: { p50: number; p95: number; max: number };
  densestCell: { h3Cell: string; devices: number } | null;
  platform: { ios: number; android: number };
  language: { en: number; es: number };
  /** Devices with 2 or 3 places. */
  multiPlaceDevices: number;
}

/** Accumulates seed statistics in O(cells) memory. Entries of one device must be adjacent. */
export class SeedStats {
  private readonly perCell = new Map<string, number>();
  private currentDevice: string | undefined;
  private currentCells = new Set<string>();
  private currentPlaces = 0;
  private devices = 0;
  private entries = 0;
  private ios = 0;
  private android = 0;
  private en = 0;
  private es = 0;
  private multi = 0;

  add(e: CellIndexEntry): void {
    if (e.deviceId !== this.currentDevice) {
      this.closeDevice();
      this.currentDevice = e.deviceId;
      this.devices++;
      if (e.target?.platform === 'ios') this.ios++;
      else if (e.target?.platform === 'android') this.android++;
      if (e.language === 'en') this.en++;
      else this.es++;
    }
    this.entries++;
    this.currentPlaces++;
    if (!this.currentCells.has(e.h3Cell)) {
      this.currentCells.add(e.h3Cell);
      this.perCell.set(e.h3Cell, (this.perCell.get(e.h3Cell) ?? 0) + 1);
    }
  }

  private closeDevice(): void {
    if (this.currentPlaces > 1) this.multi++;
    this.currentCells = new Set();
    this.currentPlaces = 0;
  }

  summary(): SeedSummary {
    this.closeDevice();
    const counts = [...this.perCell.values()].sort((a, b) => a - b);
    const at = (q: number) =>
      counts.length === 0
        ? 0
        : (counts[Math.min(counts.length - 1, Math.floor(q * counts.length))] ?? 0);
    let densest: SeedSummary['densestCell'] = null;
    for (const [h3Cell, devices] of this.perCell) {
      if (!densest || devices > densest.devices) densest = { h3Cell, devices };
    }
    return {
      devices: this.devices,
      entries: this.entries,
      cellsUsed: this.perCell.size,
      devicesPerCell: { p50: at(0.5), p95: at(0.95), max: counts[counts.length - 1] ?? 0 },
      densestCell: densest,
      platform: { ios: this.ios, android: this.android },
      language: { en: this.en, es: this.es },
      multiPlaceDevices: this.multi,
    };
  }
}

/** Convenience: runs a generator through a stats accumulator without storing anything. */
export function seedStats(options: SeedOptions = {}): SeedSummary {
  const stats = new SeedStats();
  for (const e of generateEntries(options)) stats.add(e);
  return stats.summary();
}

/** Passes entries through while feeding a SeedStats. */
export function* tapStats(
  entries: Iterable<CellIndexEntry>,
  stats: SeedStats,
): Generator<CellIndexEntry> {
  for (const e of entries) {
    stats.add(e);
    yield e;
  }
}

if (isMain(import.meta.url)) {
  const args = parseArgs(process.argv.slice(2));
  const devices = numberArg(args, 'devices') ?? DEFAULT_DEVICES;
  const seed = numberArg(args, 'seed') ?? DEFAULT_SEED;
  const out = args['out'];
  const stats = new SeedStats();
  const started = Date.now();
  const entries = tapStats(generateEntries({ devices, seed }), stats);
  if (out) {
    await writeNdjson(entries, out);
  } else {
    for (const _e of entries) {
      // stats only
    }
  }
  logJson('seed.done', {
    seed,
    out: out ?? null,
    ms: Date.now() - started,
    ...stats.summary(),
  });
}
