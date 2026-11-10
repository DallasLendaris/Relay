/**
 * Test 2's on-disk cell index: the seed devices as one TSV file per H3 res-4 shard
 * (`<res4>.tsv`, lines sorted by cell, then `deviceId#placeId`, like the DynamoDB sort key) plus
 * `<res4>.idx.json` mapping each res-7 cell to its byte range. Written once per
 * (region, devices, seed) under ctx.dataDir and reused; `manifest.json` is written last and marks
 * the set complete.
 *
 * Line: h3Cell \t deviceId \t placeId \t i|a \t token \t e|s \t deviceCells (comma list, empty when
 * the device has one cell).
 *
 * Regions:
 *  - county: Santa Clara County, the existing loadtest seed generator (seed.ts), one stream.
 *  - state: California, a rough model (documented in the report): devices spread over 10 metro
 *    areas by approximate population share (85%), 13% uniform over the state, and 2% decoys just
 *    outside the alert polygon (Reno, Las Vegas, Tijuana) that must never be pushed. Generated in
 *    fixed chunks of 1,250,000 devices, each with its own RNG stream, so the result does not depend
 *    on how many processes ran it.
 */
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { open, type FileHandle } from 'node:fs/promises';
import path from 'node:path';
import { cellToParent, latLngToCell } from 'h3-js';
import type {
  CellIndex,
  CellIndexEntry,
  CellIndexPage,
} from '../../../../../backend/src/scale/contracts.js';
import { assignFcmProject } from '../../../../../backend/src/adapters/push/fcm-projects.js';
import { createRng, normal, type Rng } from '../../../mock-common.js';
import { COUNTY_BOUNDS, CELL_RESOLUTION, uuidForDevice } from '../../../seed.js';

export type Region = 'county' | 'state' | 'dense';
// v2: statewide tokens derive from the device index (v1 drew them from per-chunk 32-bit PRNG
// streams, which overlap at 10M devices and gave about 2.1M devices a duplicate token).
export const SEED_FORMAT = 'v2';
export const STATE_CHUNK = 1_250_000;
export const SHARD_RES = 4;

export interface SeedManifest {
  format: string;
  region: Region;
  devices: number;
  seed: number;
  entries: number;
  ios: number;
  android: number;
  multiCell: number;
  shards: number;
  generatedMs: number;
}

export function seedDirFor(
  dataDir: string,
  region: Region,
  devices: number,
  seed: number,
  denseDevices = 0,
): string {
  const dense = region === 'dense' ? `-d${denseDevices}` : '';
  return path.join(dataDir, 't2-seed', `${region}-${devices}${dense}-s${seed}-${SEED_FORMAT}`);
}

/**
 * The one dense res-7 cell of scenario f: downtown San Jose, inside the county alert area. Real
 * metro cells (LA, SF, San Diego) hold 75,000-150,000 devices (contracts.ts DENSE_CELL_RECIPIENTS).
 */
export const DENSE_CELL = latLngToCell(37.3382, -121.8863, CELL_RESOLUTION);

/** `count` devices that all live in DENSE_CELL (indices start..start+count-1, 55% iOS). */
export function* generateDenseCell(
  seed: number,
  start: number,
  count: number,
): Generator<GenDevice> {
  const rng = createRng((Math.imul(seed, 0x9e3779b1) ^ 0x51ed270b) >>> 0);
  for (let i = start; i < start + count; i++) {
    const ios = rng() < 0.55;
    yield {
      deviceId: uuidForDevice(i),
      platform: ios ? 'ios' : 'android',
      token: stateToken(seed, i, ios),
      language: rng() < 0.8 ? 'en' : 'es',
      cells: [DENSE_CELL],
    };
  }
}

export function readManifest(dir: string): SeedManifest | null {
  const f = path.join(dir, 'manifest.json');
  if (!existsSync(f)) return null;
  try {
    return JSON.parse(readFileSync(f, 'utf8')) as SeedManifest;
  } catch {
    return null;
  }
}

// ---------- polygons ----------

/** GeoJSON [lng, lat] ring of the county bounding box (the existing sim's alert area). */
export function countyRing(): Array<[number, number]> {
  const { south, north, west, east } = COUNTY_BOUNDS;
  return [
    [west, south],
    [east, south],
    [east, north],
    [west, north],
    [west, south],
  ];
}

/** A simplified California outline (about 410,000 km2, about 80,000 res-7 cells). */
export const CA_RING: Array<[number, number]> = [
  [-124.21, 41.99],
  [-120.0, 41.99],
  [-120.0, 39.0],
  [-114.63, 35.0],
  [-114.43, 34.0],
  [-114.72, 32.72],
  [-117.12, 32.53],
  [-117.25, 32.9],
  [-117.6, 33.4],
  [-118.4, 33.75],
  [-118.5, 34.0],
  [-119.2, 34.1],
  [-120.6, 34.5],
  [-120.65, 35.3],
  [-121.9, 36.3],
  [-121.95, 36.95],
  [-122.5, 37.5],
  [-122.55, 37.85],
  [-123.0, 38.0],
  [-123.7, 38.9],
  [-123.8, 39.8],
  [-124.4, 40.4],
  [-124.1, 41.0],
  [-124.21, 41.99],
];

/** Ray casting; ring is [lng, lat]. */
export function inRing(lat: number, lng: number, ring: Array<[number, number]>): boolean {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i] as [number, number];
    const [xj, yj] = ring[j] as [number, number];
    if (yi > lat !== yj > lat && lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}

// ---------- California generator ----------

interface Center {
  name: string;
  lat: number;
  lng: number;
  sigma: number;
  weight: number;
}

/** Approximate population shares of the main metro areas (sum 0.85 of devices). */
export const CA_CENTERS: readonly Center[] = [
  { name: 'Los Angeles / Orange', lat: 34.0, lng: -118.2, sigma: 0.3, weight: 0.33 },
  { name: 'Inland Empire', lat: 34.0, lng: -117.3, sigma: 0.25, weight: 0.09 },
  { name: 'San Diego', lat: 32.8, lng: -117.1, sigma: 0.15, weight: 0.085 },
  { name: 'SF / Oakland', lat: 37.77, lng: -122.25, sigma: 0.15, weight: 0.1 },
  { name: 'San Jose', lat: 37.34, lng: -121.89, sigma: 0.1, weight: 0.05 },
  { name: 'Sacramento', lat: 38.58, lng: -121.45, sigma: 0.15, weight: 0.06 },
  { name: 'Fresno', lat: 36.75, lng: -119.77, sigma: 0.12, weight: 0.035 },
  { name: 'Bakersfield', lat: 35.37, lng: -119.02, sigma: 0.1, weight: 0.025 },
  { name: 'Stockton / Modesto', lat: 37.8, lng: -121.1, sigma: 0.15, weight: 0.04 },
  { name: 'Ventura / Santa Barbara', lat: 34.3, lng: -119.2, sigma: 0.12, weight: 0.03 },
];
const URBAN = 0.85;
const DECOY = 0.02;
/** Places just outside the alert polygon: devices only here must never be pushed. */
const DECOYS: readonly Center[] = [
  { name: 'Reno', lat: 39.53, lng: -119.81, sigma: 0.05, weight: 0.4 },
  { name: 'Las Vegas', lat: 36.17, lng: -115.14, sigma: 0.08, weight: 0.4 },
  { name: 'Tijuana', lat: 32.45, lng: -117.0, sigma: 0.03, weight: 0.2 },
];
const CA_BOX = { south: 32.5, north: 42.0, west: -124.45, east: -114.1 };

function pick(rng: Rng, list: readonly Center[]): Center {
  let r = rng() * list.reduce((s, c) => s + c.weight, 0);
  for (const c of list) {
    r -= c.weight;
    if (r < 0) return c;
  }
  return list[0] as Center;
}

function drawStatePoint(rng: Rng): { lat: number; lng: number } {
  const r = rng();
  if (r < DECOY) {
    const c = pick(rng, DECOYS);
    for (;;) {
      const lat = c.lat + normal(rng) * c.sigma;
      const lng = c.lng + normal(rng) * c.sigma;
      if (!inRing(lat, lng, CA_RING)) return { lat, lng };
    }
  }
  if (r < DECOY + URBAN) {
    const c = pick(rng, CA_CENTERS);
    for (;;) {
      const lat = c.lat + normal(rng) * c.sigma;
      const lng = c.lng + normal(rng) * c.sigma;
      if (inRing(lat, lng, CA_RING)) return { lat, lng };
    }
  }
  for (;;) {
    const lat = CA_BOX.south + rng() * (CA_BOX.north - CA_BOX.south);
    const lng = CA_BOX.west + rng() * (CA_BOX.east - CA_BOX.west);
    if (inRing(lat, lng, CA_RING)) return { lat, lng };
  }
}

/**
 * A push token unique to device `i`, independent of any PRNG stream: hashing the index makes
 * collisions astronomically unlikely (the per-chunk 32-bit streams of v1 overlapped at 10M).
 */
function stateToken(seed: number, i: number, ios: boolean): string {
  const h = (n: number) => createHash('sha512').update(`relay-state:${seed}:${i}:${n}`).digest();
  if (ios) return h(0).subarray(0, 32).toString('hex');
  const raw = Buffer.concat([h(0), h(1)]).toString('base64url');
  return `${raw.slice(0, 22)}:APA91b${raw.slice(22, 154)}`;
}

export interface GenDevice {
  deviceId: string;
  platform: 'ios' | 'android';
  token: string;
  language: 'en' | 'es';
  cells: string[];
}

/** One chunk of the California seed (same shares as seed.ts for platform, language, places). */
export function* generateStateChunk(
  seed: number,
  chunk: number,
  devices: number,
): Generator<GenDevice> {
  const start = chunk * STATE_CHUNK;
  const end = Math.min(devices, start + STATE_CHUNK);
  const rng = createRng((Math.imul(seed, 0x9e3779b1) ^ Math.imul(chunk + 1, 0x85ebca6b)) >>> 0);
  for (let i = start; i < end; i++) {
    const ios = rng() < 0.55;
    const language = rng() < 0.8 ? 'en' : 'es';
    const r = rng();
    const places = r < 0.9 ? 1 : r < 0.95 ? 2 : 3;
    const cells: string[] = [];
    for (let p = 0; p < places; p++) {
      const { lat, lng } = drawStatePoint(rng);
      cells.push(latLngToCell(lat, lng, CELL_RESOLUTION));
    }
    yield {
      deviceId: uuidForDevice(i),
      platform: ios ? 'ios' : 'android',
      token: stateToken(seed, i, ios),
      language,
      cells,
    };
  }
}

/** TSV lines of one device (one per place), and its res-4 shard per line. */
export function deviceLines(d: GenDevice): Array<{ shard: string; line: string }> {
  const uniq = [...new Set(d.cells)].sort();
  const dc = uniq.length > 1 ? uniq.join(',') : '';
  const plat = d.platform === 'ios' ? 'i' : 'a';
  const lang = d.language === 'en' ? 'e' : 's';
  return d.cells.map((cell, i) => ({
    shard: cellToParent(cell, SHARD_RES),
    line: `${cell}\t${d.deviceId}\tp${i + 1}\t${plat}\t${d.token}\t${lang}\t${dc}\n`,
  }));
}

// ---------- reading ----------

export interface ParsedLine {
  h3Cell: string;
  deviceId: string;
  placeId: string;
  platform: 'ios' | 'android';
  token: string;
  language: 'en' | 'es';
  deviceCells: string[];
}

export function parseLine(line: string): ParsedLine {
  const f = line.split('\t');
  const h3Cell = f[0] ?? '';
  const dc = f[6] ?? '';
  return {
    h3Cell,
    deviceId: f[1] ?? '',
    placeId: f[2] ?? '',
    platform: f[3] === 'i' ? 'ios' : 'android',
    token: f[4] ?? '',
    language: f[5] === 's' ? 'es' : 'en',
    deviceCells: dc === '' ? [h3Cell] : dc.split(','),
  };
}

type ShardIndex = Record<string, [number, number, number]>;

/** Reads cells from the on-disk seed; shared by the CellIndex adapter and the verifier. */
export class SeedReader {
  private readonly indexes = new Map<string, ShardIndex | null>();
  private readonly handles = new Map<string, Promise<FileHandle>>();

  constructor(readonly dir: string) {}

  private index(shard: string): ShardIndex | null {
    let ix = this.indexes.get(shard);
    if (ix === undefined) {
      const f = path.join(this.dir, `${shard}.idx.json`);
      ix = existsSync(f) ? (JSON.parse(readFileSync(f, 'utf8')) as ShardIndex) : null;
      this.indexes.set(shard, ix);
    }
    return ix;
  }

  private handle(shard: string): Promise<FileHandle> {
    let h = this.handles.get(shard);
    if (!h) {
      h = open(path.join(this.dir, `${shard}.tsv`), 'r');
      this.handles.set(shard, h);
      if (this.handles.size > 64) {
        const [oldest, p] = this.handles.entries().next().value as [string, Promise<FileHandle>];
        if (oldest !== shard) {
          this.handles.delete(oldest);
          void p.then((fh) => fh.close()).catch(() => {});
        }
      }
    }
    return h;
  }

  /** The cell's lines, in sort-key order. */
  async cellLines(cell: string): Promise<string[]> {
    const shard = cellToParent(cell, SHARD_RES);
    const range = this.index(shard)?.[cell];
    if (!range) return [];
    const [offset, length] = range;
    const fh = await this.handle(shard);
    const buf = Buffer.allocUnsafe(length);
    let got = 0;
    while (got < length) {
      const { bytesRead } = await fh.read(buf, got, length - got, offset + got);
      if (bytesRead === 0) throw new Error(`seed file ${shard}.tsv is truncated`);
      got += bytesRead;
    }
    const text = buf.toString('utf8');
    const lines = text.split('\n');
    if (lines[lines.length - 1] === '') lines.pop();
    return lines;
  }

  async close(): Promise<void> {
    const hs = [...this.handles.values()];
    this.handles.clear();
    await Promise.all(hs.map((h) => h.then((fh) => fh.close()).catch(() => {})));
  }
}

/**
 * CellIndex adapter over the on-disk seed (only listForCell is on the fan-out path). The FCM
 * project of an Android target is assigned here with the real assignFcmProject, as the device
 * store would have stored it; with one project the field stays absent (default project).
 */
export class DiskCellIndex implements Pick<CellIndex, 'listForCell'> {
  private readonly open = new Map<string, CellIndexEntry[]>();

  constructor(
    private readonly reader: SeedReader,
    private readonly fcmProjects: readonly string[],
  ) {}

  async listForCell(h3Cell: string, cursor?: string, limit = 1000): Promise<CellIndexPage> {
    let all = this.open.get(h3Cell);
    if (!all || cursor === undefined) {
      const multi = this.fcmProjects.length > 1;
      all = (await this.reader.cellLines(h3Cell)).map((line): CellIndexEntry => {
        const p = parseLine(line);
        const project =
          multi && p.platform === 'android'
            ? assignFcmProject(p.deviceId, this.fcmProjects)
            : undefined;
        return {
          h3Cell: p.h3Cell,
          deviceId: p.deviceId,
          placeId: p.placeId,
          target: project
            ? { platform: p.platform, token: p.token, project }
            : { platform: p.platform, token: p.token },
          language: p.language,
          deviceCells: p.deviceCells,
        };
      });
      this.open.set(h3Cell, all);
    }
    const start = cursor === undefined ? 0 : Number(cursor);
    const entries = all.slice(start, start + limit);
    const next = start + limit;
    if (next < all.length) return { entries, cursor: String(next) };
    this.open.delete(h3Cell);
    return { entries };
  }
}

export function ensureDir(dir: string): void {
  mkdirSync(dir, { recursive: true });
}
