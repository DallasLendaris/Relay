/**
 * Fake data for local development, front-end work and tests.
 *
 * Every value comes from a seeded random number generator, so the same seed and
 * counts always give the same output. Nothing here calls AWS or the NWS, and every
 * name, address and token is made up. Alerts and check-ins are checked against the
 * shared schemas before they are returned.
 */
import {
  AlertSchema,
  CheckInSchema,
  relaySeverityFor,
  type Alert,
  type CheckIn,
} from '@relay/shared';
import { cellForPoint, cellsForAlertArea, type ZoneGeometryProvider } from '../geo/index.js';
import type { Place } from '../ports.js';

/** Santa Clara County, in degrees. Every fake place and alert polygon sits inside this box. */
export const SANTA_CLARA_BOUNDS = {
  minLat: 37.1,
  maxLat: 37.45,
  minLng: -122.15,
  maxLng: -121.6,
} as const;

/** Default "now" for fake alerts. Every alert is active from here until 6 hours later. */
export const DEFAULT_BASE_TIME = '2026-12-10T18:00:00Z';

const ALERT_HOURS = 6;
const DAY_MS = 24 * 60 * 60 * 1000;

// ---------- Seeded random numbers ----------

/** A random number generator: each call returns a float in [0, 1). */
export type Rng = () => number;

/** mulberry32: a small, fast 32-bit generator. The same seed always gives the same sequence. */
export function mulberry32(seed: number): Rng {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Turns any string into a 32-bit seed (FNV-1a), so each alert gets its own sequence. */
function hashString(text: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/** A float in [min, max). */
function between(rng: Rng, min: number, max: number): number {
  return min + (max - min) * rng();
}

/** A whole number in [min, max], both included. */
function wholeBetween(rng: Rng, min: number, max: number): number {
  return Math.floor(between(rng, min, max + 1));
}

/** A random element. Throws on an empty list, which would be a bug in the caller. */
function pick<T>(rng: Rng, items: readonly T[]): T {
  return cycle(items, wholeBetween(rng, 0, items.length - 1));
}

/** The element at index i, wrapping around. */
function cycle<T>(items: readonly T[], i: number): T {
  if (items.length === 0) throw new Error('fake-data: empty list');
  const item = items[i % items.length];
  if (item === undefined) throw new Error('fake-data: index out of range');
  return item;
}

function hex(rng: Rng, length: number): string {
  return Array.from({ length }, () => Math.floor(rng() * 16).toString(16)).join('');
}

/** A random UUID in version 4 format. Zod's uuid() check accepts it. */
function uuid(rng: Rng): string {
  const h = hex(rng, 32);
  const variant = pick(rng, ['8', '9', 'a', 'b']);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-${variant}${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

// ---------- Alerts ----------

/** The three kinds of alert the fake feed cycles through. */
const ALERT_KINDS = [
  {
    event: 'Flash Flood Warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Observed',
    description:
      'Flash flooding is occurring or is imminent in the warned area. Heavy rain is causing ' +
      'creeks and low-lying roads to rise quickly. Excessive runoff may cause flooding of ' +
      'streets, underpasses and small streams.',
    instruction: 'Move to higher ground now. Do not walk or drive through flood water.',
  },
  {
    event: 'Red Flag Warning',
    capSeverity: 'Severe',
    urgency: 'Expected',
    certainty: 'Likely',
    description:
      'Critical fire weather conditions are expected. Very low humidity combined with strong ' +
      'winds will make any fire start and spread rapidly.',
    instruction: 'Avoid activities that could start a fire. Report smoke right away.',
  },
  {
    event: 'Heat Advisory',
    capSeverity: 'Moderate',
    urgency: 'Expected',
    certainty: 'Likely',
    description:
      'Hot temperatures are expected with limited overnight relief. Heat-related illness is ' +
      'possible for people without air conditioning, and for outdoor workers.',
    instruction: 'Limit time outdoors and drink water. Check on neighbors and pets.',
  },
] as const;

/** A random closed polygon, roughly 3 to 10 km across, wholly inside the county box. */
function randomPolygon(rng: Rng): { type: 'Polygon'; coordinates: number[][][] } {
  const { minLat, maxLat, minLng, maxLng } = SANTA_CLARA_BOUNDS;
  // Radius in km; the shape is about 2 x radius across.
  const radiusKm = between(rng, 1.5, 5);
  // Degrees per km. Latitude degrees are nearly constant; longitude shrinks with latitude,
  // so use the largest latitude (smallest cosine) to keep the shape inside the box.
  const dLat = radiusKm / 110.574;
  const dLng = radiusKm / (111.32 * Math.cos((maxLat * Math.PI) / 180));
  const centreLat = between(rng, minLat + dLat, maxLat - dLat);
  const centreLng = between(rng, minLng + dLng, maxLng - dLng);

  // Random angles, sorted, give a star-shaped ring, which never crosses itself.
  const sides = wholeBetween(rng, 7, 10);
  const angles = Array.from({ length: sides }, () => between(rng, 0, 2 * Math.PI)).sort(
    (a, b) => a - b,
  );
  const ring = angles.map((angle) => {
    const reach = between(rng, 0.6, 1); // vary the distance from the centre a little
    return [
      round6(centreLng + reach * dLng * Math.cos(angle)),
      round6(centreLat + reach * dLat * Math.sin(angle)),
    ];
  });
  const first = ring[0];
  if (!first) throw new Error('fake-data: polygon has no vertices');
  ring.push([...first]); // GeoJSON rings are closed
  return { type: 'Polygon', coordinates: [ring] };
}

function round6(value: number): number {
  return Math.round(value * 1e6) / 1e6;
}

/** Fake zones: no lookups, because every fake alert already has its own geometry. */
const NO_ZONES: ZoneGeometryProvider = { getZoneGeometry: async () => null };

export interface FakeAlertOptions {
  seed: number;
  count: number;
  /** Start of the fake alerts, as an ISO time. Defaults to DEFAULT_BASE_TIME. */
  base?: string;
}

/**
 * Fake NWS-style alerts, cycling through flash flood, red flag and heat advisory.
 * Async because H3 cell lookup goes through cellsForAlertArea.
 */
export async function fakeAlerts(options: FakeAlertOptions): Promise<Alert[]> {
  const baseMs = parseTime(options.base ?? DEFAULT_BASE_TIME);
  const rng = mulberry32(options.seed);
  const alerts: Alert[] = [];

  for (let i = 0; i < options.count; i++) {
    const kind = cycle(ALERT_KINDS, i);
    const geometry = randomPolygon(rng);
    const h3Cells = await cellsForAlertArea({ geometry, ugcCodes: [] }, NO_ZONES);

    // Sent a few minutes before it takes effect; expires 6 hours after the base time.
    const sentMs = baseMs - wholeBetween(rng, 0, 30) * 60_000;
    const expiresMs = baseMs + ALERT_HOURS * 60 * 60 * 1000;

    const draft = {
      id: uuid(rng),
      source: 'nws',
      sourceId: `urn:oid:fake.${i + 1}`,
      event: kind.event,
      eventCode: null,
      relaySeverity: relaySeverityFor(kind.event, kind.capSeverity),
      capSeverity: kind.capSeverity,
      urgency: kind.urgency,
      certainty: kind.certainty,
      status: 'Actual',
      messageType: 'Alert',
      state: 'active',
      references: [],
      area: {
        description: 'Santa Clara County, CA',
        geometry,
        ugcCodes: ['CAC085'],
        sameCodes: ['006085'],
        h3Cells,
      },
      sent: new Date(sentMs).toISOString(),
      effective: new Date(baseMs).toISOString(),
      onset: null,
      expires: new Date(expiresMs).toISOString(),
      ends: null,
      official: {
        headline: `${kind.event} issued for Santa Clara County`,
        description: kind.description,
        instruction: kind.instruction,
        senderName: 'NWS San Francisco Bay Area',
        language: 'en-US',
      },
      summaries: [],
      signature: null,
    };
    alerts.push(AlertSchema.parse(draft));
  }
  return alerts;
}

// ---------- Check-ins ----------

export interface FakeCheckInOptions {
  seed: number;
  alert: Alert;
  count: number;
}

/**
 * Fake "I'm okay" check-ins for one alert. Each one is at a random point inside the
 * alert polygon, dated between the alert's effective and expires times.
 */
export function fakeCheckIns(options: FakeCheckInOptions): CheckIn[] {
  const { alert } = options;
  // Mixing in the alert id gives each alert its own sequence, still fixed by the seed.
  const rng = mulberry32(hashString(`${options.seed}:${alert.id}`));
  const ring = outerRing(alert);
  const cells = new Set(alert.area.h3Cells);
  const startMs = Date.parse(alert.effective);
  const endMs = Date.parse(alert.expires);
  const checkIns: CheckIn[] = [];

  for (let i = 0; i < options.count; i++) {
    // The check-in stores only the H3 cell of its point, never the exact location.
    const h3Cell = pointInAlert(rng, ring, cells, alert.area.h3Cells);
    const checkedAtMs = between(rng, startMs, endMs);
    const draft = {
      alertId: alert.id,
      deviceId: uuid(rng),
      status: 'ok',
      checkedInAt: new Date(Math.floor(checkedAtMs)).toISOString(),
      token: `fake-token-${hex(rng, 40)}`,
      idempotencyKey: uuid(rng),
      h3Cell,
    };
    checkIns.push(CheckInSchema.parse(draft));
  }
  return checkIns;
}

/** The outer ring of an alert's polygon. Fake alerts always have one. */
function outerRing(alert: Alert): number[][] {
  const geometry = alert.area.geometry;
  if (geometry?.type !== 'Polygon') throw new Error(`fake-data: alert ${alert.id} has no polygon`);
  const ring = geometry.coordinates[0];
  if (!ring) throw new Error(`fake-data: alert ${alert.id} has an empty polygon`);
  return ring;
}

/**
 * The H3 cell of a random point inside the ring, chosen so the cell is one of the alert's cells.
 * The fallback picks a cell from the alert's own list. That point may fall just outside
 * the polygon, but the cell is always in the list, so the check-in still matches the alert.
 * The fallback is very rare: a point inside the polygon almost always lands in a listed cell.
 */
function pointInAlert(rng: Rng, ring: number[][], cells: Set<string>, cellList: string[]): string {
  for (let attempt = 0; attempt < 200; attempt++) {
    const point = randomPointInRing(rng, ring);
    const h3Cell = cellForPoint(point.lat, point.lng);
    if (cells.has(h3Cell)) return h3Cell;
  }
  return pick(rng, cellList);
}

/** Rejection sampling inside the bounding box of the ring. */
function randomPointInRing(rng: Rng, ring: number[][]): { lat: number; lng: number } {
  const xs = ring.map((p) => p[0] ?? 0);
  const ys = ring.map((p) => p[1] ?? 0);
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  for (;;) {
    const lng = between(rng, minX, maxX);
    const lat = between(rng, minY, maxY);
    if (insideRing(lng, lat, ring)) return { lat, lng };
  }
}

/** Ray casting: does the point (lng, lat) lie inside the ring? */
function insideRing(lng: number, lat: number, ring: number[][]): boolean {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const a = ring[i];
    const b = ring[j];
    if (!a || !b) continue;
    const [xi = 0, yi = 0] = a;
    const [xj = 0, yj = 0] = b;
    const crosses = yi > lat !== yj > lat && lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi;
    if (crosses) inside = !inside;
  }
  return inside;
}

// ---------- Places ----------

const PLACE_LABELS = ['Home', 'Work', 'School', 'Parents', 'Gym', 'Grocery store', 'Daycare'];
const STREETS = [
  'Stevens Creek Blvd',
  'El Camino Real',
  'Lawrence Expy',
  'Homestead Rd',
  'Saratoga Ave',
  'Bascom Ave',
  'Foothill Expy',
  'Monroe St',
  'Park Ave',
  'Hamilton Ave',
];
const CITIES = ['San Jose', 'Santa Clara', 'Sunnyvale', 'Cupertino', 'Campbell', 'Los Altos'];

export interface FakePlaceOptions {
  seed: number;
  count: number;
  /** Creation times are spread over the year before this ISO time. Defaults to DEFAULT_BASE_TIME. */
  base?: string;
}

/**
 * Fake saved places spread evenly across the county. Places share a small pool of devices,
 * about one device per four places, the way a real user saves several places.
 */
export function fakePlaces(options: FakePlaceOptions): Place[] {
  const baseMs = parseTime(options.base ?? DEFAULT_BASE_TIME);
  const rng = mulberry32(options.seed ^ 0x9e3779b9);
  const deviceCount = Math.max(1, Math.ceil(options.count / 4));
  const deviceIds = Array.from({ length: deviceCount }, () => uuid(rng));
  const { minLat, maxLat, minLng, maxLng } = SANTA_CLARA_BOUNDS;
  const places: Place[] = [];

  for (let i = 0; i < options.count; i++) {
    const latitude = round6(between(rng, minLat, maxLat));
    const longitude = round6(between(rng, minLng, maxLng));
    places.push({
      id: uuid(rng),
      deviceId: pick(rng, deviceIds),
      label: pick(rng, PLACE_LABELS),
      shortAddress: `${wholeBetween(rng, 100, 9999)} ${pick(rng, STREETS)}, ${pick(rng, CITIES)}`,
      latitude,
      longitude,
      h3Cell: cellForPoint(latitude, longitude),
      createdAt: new Date(baseMs - wholeBetween(rng, 0, 365) * DAY_MS).toISOString(),
    });
  }
  return places;
}

// ---------- Whole dataset ----------

export interface DatasetOptions {
  seed: number;
  alerts: number;
  /** Total check-ins, shared out across the alerts as evenly as possible. */
  checkins: number;
  places: number;
  base?: string;
}

export interface Dataset {
  alerts: Alert[];
  checkins: CheckIn[];
  places: Place[];
}

/** Everything the CLI writes: alerts, their check-ins and the saved places. */
export async function generateDataset(options: DatasetOptions): Promise<Dataset> {
  if (options.alerts === 0 && options.checkins > 0) {
    throw new Error('fake-data: check-ins need at least one alert');
  }
  const alerts = await fakeAlerts({
    seed: options.seed,
    count: options.alerts,
    base: options.base,
  });
  const checkins = alerts.flatMap((alert, i) =>
    fakeCheckIns({
      seed: options.seed,
      alert,
      count: share(options.checkins, options.alerts, i),
    }),
  );
  const places = fakePlaces({ seed: options.seed, count: options.places, base: options.base });
  return { alerts, checkins, places };
}

/** How many of `total` items go to slot `index` out of `slots`, with the remainder spread at the start. */
function share(total: number, slots: number, index: number): number {
  const base = Math.floor(total / slots);
  return index < total % slots ? base + 1 : base;
}

function parseTime(iso: string): number {
  const ms = Date.parse(iso);
  if (Number.isNaN(ms)) throw new Error(`fake-data: not a valid time: ${iso}`);
  return ms;
}
