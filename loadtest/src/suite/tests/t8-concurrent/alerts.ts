/**
 * The t8 alert script: which alerts arrive, how severe they are, which areas they cover and when
 * (seconds after the first). Areas overlap on purpose: a county-wide warning, city-sized
 * emergencies inside it, a small zone inside one of the cities, advisories on top, and a
 * statewide-like watch. A device in the middle of San Jose is in five or more alerts at once.
 */
import type { Alert } from '@relay/shared';
import { cellsForAlertArea } from '../../../../../backend/src/geo/h3-cells.js';
import { COUNTY_BOUNDS } from '../../../seed.js';
import { NORCAL_RING } from '../t2-fanout/config.js';
import { CA_RING, countyRing } from '../t2-fanout/seed-data.js';
import type { AlertKeys } from '../t2-fanout/alert.js';

export type Severity = Alert['relaySeverity'];
export type Ring = Array<[number, number]>;

export interface AlertSpec {
  /** Short label for reports. */
  name: string;
  severity: Severity;
  /** Key into `areaRings`. */
  area: string;
  /** Arrival, seconds after the first alert. */
  atS: number;
}

/** A closed [lng, lat] ring for a box around a centre. */
function box(lat: number, lng: number, dLat: number, dLng: number): Ring {
  return [
    [lng - dLng, lat - dLat],
    [lng + dLng, lat - dLat],
    [lng + dLng, lat + dLat],
    [lng - dLng, lat + dLat],
    [lng - dLng, lat - dLat],
  ];
}

const { south: S, north: N, west: W, east: E } = COUNTY_BOUNDS;
const MID_LAT = (S + N) / 2;
const MID_LNG = (W + E) / 2;

export function areaRings(scale: 'full' | 'smoke'): Record<string, { label: string; ring: Ring }> {
  const rect = (s: number, n: number, w: number, e: number): Ring => [
    [w, s],
    [e, s],
    [e, n],
    [w, n],
    [w, s],
  ];
  return {
    county: { label: 'Santa Clara County', ring: countyRing() },
    north: { label: 'north half of the county', ring: rect(MID_LAT, N, W, E) },
    south: { label: 'south half of the county', ring: rect(S, MID_LAT, W, E) },
    east: { label: 'east half of the county', ring: rect(S, N, MID_LNG, E) },
    sanjose: { label: 'San Jose', ring: box(37.3382, -121.8863, 0.09, 0.11) },
    sjcore: { label: 'central San Jose', ring: box(37.3382, -121.8863, 0.03, 0.04) },
    santaclara: { label: 'Santa Clara', ring: box(37.3541, -121.9552, 0.05, 0.06) },
    sunnyvale: { label: 'Sunnyvale', ring: box(37.3688, -122.0363, 0.06, 0.07) },
    mountainview: { label: 'Mountain View', ring: box(37.3861, -122.0839, 0.06, 0.07) },
    state:
      scale === 'full'
        ? { label: 'California (simplified outline)', ring: CA_RING }
        : { label: 'Northern California box (smoke)', ring: NORCAL_RING },
  };
}

/** The 20 alerts of the full run, arriving over about 10 s. */
export const FULL_SCRIPT: AlertSpec[] = [
  { name: 'county warning', severity: 'warning', area: 'county', atS: 0.0 },
  { name: 'San Jose emergency', severity: 'emergency', area: 'sanjose', atS: 0.4 },
  { name: 'Sunnyvale emergency', severity: 'emergency', area: 'sunnyvale', atS: 0.8 },
  { name: 'Mountain View emergency', severity: 'emergency', area: 'mountainview', atS: 1.2 },
  { name: 'north county warning', severity: 'warning', area: 'north', atS: 1.6 },
  { name: 'central San Jose emergency', severity: 'emergency', area: 'sjcore', atS: 2.0 },
  { name: 'statewide watch', severity: 'watch', area: 'state', atS: 2.5 },
  { name: 'county advisory', severity: 'advisory', area: 'county', atS: 3.0 },
  { name: 'San Jose advisory', severity: 'advisory', area: 'sanjose', atS: 3.5 },
  { name: 'south county advisory', severity: 'advisory', area: 'south', atS: 4.0 },
  { name: 'Santa Clara watch', severity: 'watch', area: 'santaclara', atS: 4.5 },
  { name: 'east county watch', severity: 'watch', area: 'east', atS: 5.0 },
  { name: 'county statement', severity: 'statement', area: 'county', atS: 5.5 },
  { name: 'Santa Clara emergency (late)', severity: 'emergency', area: 'santaclara', atS: 6.0 },
  { name: 'Sunnyvale advisory', severity: 'advisory', area: 'sunnyvale', atS: 6.5 },
  { name: 'Mountain View advisory', severity: 'advisory', area: 'mountainview', atS: 7.0 },
  { name: 'south county warning (late)', severity: 'warning', area: 'south', atS: 7.5 },
  { name: 'San Jose statement', severity: 'statement', area: 'sanjose', atS: 8.0 },
  { name: 'central San Jose emergency 2 (late)', severity: 'emergency', area: 'sjcore', atS: 8.5 },
  { name: 'Santa Clara advisory', severity: 'advisory', area: 'santaclara', atS: 9.5 },
];

/** The six alerts of the smoke run, arriving over about 2 s. */
export const SMOKE_SCRIPT: AlertSpec[] = [
  { name: 'county warning', severity: 'warning', area: 'county', atS: 0.0 },
  { name: 'San Jose emergency', severity: 'emergency', area: 'sanjose', atS: 0.3 },
  { name: 'central San Jose emergency', severity: 'emergency', area: 'sjcore', atS: 0.6 },
  { name: 'statewide watch', severity: 'watch', area: 'state', atS: 0.9 },
  { name: 'county advisory', severity: 'advisory', area: 'county', atS: 1.2 },
  { name: 'county statement', severity: 'statement', area: 'county', atS: 1.5 },
];

export const alertIdFor8 = (n: number): string =>
  `7a8e0c11-5b3d-4c8e-9a6f-${(0x2000 + n).toString(16).padStart(12, '0')}`;

const EVENT: Record<Severity, { event: string; code: string; cap: Alert['capSeverity'] }> = {
  emergency: { event: 'Flash Flood Emergency', code: 'FFE', cap: 'Extreme' },
  warning: { event: 'Flash Flood Warning', code: 'FFW', cap: 'Severe' },
  watch: { event: 'Flood Watch', code: 'FFA', cap: 'Moderate' },
  advisory: { event: 'Flood Advisory', code: 'FLS', cap: 'Minor' },
  statement: { event: 'Special Weather Statement', code: 'SPS', cap: 'Minor' },
};

/** Builds and signs one alert of the script. `n` is its index; ids and source ids are unique per alert. */
export async function buildScriptAlert(
  keys: AlertKeys,
  n: number,
  spec: AlertSpec,
  area: { label: string; ring: Ring },
  idPrefix = 't8',
): Promise<Alert> {
  const geometry = { type: 'Polygon' as const, coordinates: [area.ring] };
  const h3Cells = await cellsForAlertArea(
    { geometry, ugcCodes: [] },
    {
      getZoneGeometry: () => {
        throw new Error('zone lookup not expected');
      },
    },
  );
  const ev = EVENT[spec.severity];
  const now = Date.now();
  return keys.sign({
    id: alertIdFor8(n),
    source: 'nws',
    sourceId: `urn:oid:${idPrefix}-${n}`,
    event: ev.event,
    eventCode: ev.code,
    relaySeverity: spec.severity,
    capSeverity: ev.cap,
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description: area.label, geometry, ugcCodes: [], sameCodes: [], h3Cells },
    sent: new Date(now - 60_000).toISOString(),
    effective: new Date(now - 60_000).toISOString(),
    onset: null,
    expires: new Date(now + 4 * 3600_000).toISOString(),
    ends: null,
    official: {
      headline: `${ev.event} for ${area.label} (scalability test)`,
      description: 'Simulated alert for the local scalability suite.',
      instruction: null,
      senderName: 'NWS',
      language: 'en-US',
    },
    summaries: [],
    signature: 'unsigned',
  });
}

export async function buildScript(
  keys: AlertKeys,
  script: AlertSpec[],
  scale: 'full' | 'smoke',
): Promise<Alert[]> {
  const rings = areaRings(scale);
  const out: Alert[] = [];
  for (const [n, spec] of script.entries()) {
    const area = rings[spec.area];
    if (!area) throw new Error(`unknown area ${spec.area}`);
    out.push(await buildScriptAlert(keys, n, spec, area));
  }
  return out;
}

export const isUrgent = (s: Severity): boolean => s === 'emergency' || s === 'warning';
export const isStandard = (s: Severity): boolean => s === 'watch' || s === 'advisory';
