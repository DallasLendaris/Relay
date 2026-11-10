import type { MultiPolygon, Polygon, Position } from 'geojson';

/** More positions than this in one geometry is treated as broken (NWS polygons have tens to hundreds). */
export const MAX_GEOMETRY_POSITIONS = 100_000;

export type GeometryCheck =
  { ok: true; geometry: Polygon | MultiPolygon } | { ok: false; reason: string };

/** The geometry can never produce cells. Permanent: retrying does not help. */
export class InvalidGeometryError extends Error {
  constructor(readonly reason: string) {
    super(`invalid alert geometry: ${reason}`);
    this.name = 'InvalidGeometryError';
  }
}

/** Twice the signed area of a ring (shoelace, planar lng/lat: enough to tell "flat" from "not flat"). */
function ringArea2(ring: Position[]): number {
  // Shift to the first point first so tiny rings far from the origin keep their precision.
  const [x0 = 0, y0 = 0] = ring[0] ?? [];
  let sum = 0;
  for (let i = 0; i < ring.length - 1; i++) {
    const a = ring[i];
    const b = ring[i + 1];
    if (!a || !b) continue;
    sum += (a[0]! - x0) * (b[1]! - y0) - (b[0]! - x0) * (a[1]! - y0);
  }
  return sum;
}

function checkRing(ring: unknown, label: string): string | null {
  if (!Array.isArray(ring)) return `${label} is not an array`;
  if (ring.length < 4) return `${label} has ${ring.length} positions (need at least 4)`;
  for (const p of ring) {
    if (!Array.isArray(p) || p.length < 2) return `${label} has a malformed position`;
    for (let i = 0; i < p.length; i++) {
      if (typeof p[i] !== 'number' || !Number.isFinite(p[i])) {
        return `${label} has a non-finite coordinate`;
      }
    }
    const lng = p[0] as number;
    const lat = p[1] as number;
    if (lng < -180 || lng > 180 || lat < -90 || lat > 90) {
      return `${label} has a coordinate out of range`;
    }
  }
  const first = ring[0] as Position;
  const last = ring[ring.length - 1] as Position;
  if (first[0] !== last[0] || first[1] !== last[1]) return `${label} is not closed`;
  if (ringArea2(ring as Position[]) === 0) return `${label} has zero area (collinear or repeated)`;
  return null;
}

function checkPolygon(rings: unknown, label: string): string | null {
  if (!Array.isArray(rings) || rings.length === 0) return `${label} has no rings`;
  for (const [i, ring] of rings.entries()) {
    const bad = checkRing(ring, i === 0 ? `${label} outer ring` : `${label} hole ${i}`);
    if (bad) return bad;
  }
  return null;
}

/**
 * Validates a GeoJSON Polygon / MultiPolygon before it is handed to H3: every ring needs at
 * least 4 finite, in-range positions, closed, with non-zero area. Never throws.
 */
export function validateGeometry(geometry: unknown): GeometryCheck {
  if (typeof geometry !== 'object' || geometry === null)
    return { ok: false, reason: 'not an object' };
  const g = geometry as { type?: unknown; coordinates?: unknown };
  let positions = 0;
  const count = (rings: unknown): void => {
    if (Array.isArray(rings)) for (const r of rings) positions += Array.isArray(r) ? r.length : 0;
  };
  if (g.type === 'Polygon') {
    const bad = checkPolygon(g.coordinates, 'polygon');
    if (bad) return { ok: false, reason: bad };
    count(g.coordinates);
  } else if (g.type === 'MultiPolygon') {
    if (!Array.isArray(g.coordinates) || g.coordinates.length === 0) {
      return { ok: false, reason: 'multipolygon has no polygons' };
    }
    for (const [i, poly] of g.coordinates.entries()) {
      const bad = checkPolygon(poly, `polygon ${i}`);
      if (bad) return { ok: false, reason: bad };
      count(poly);
    }
  } else {
    return { ok: false, reason: `unsupported type ${String(g.type)}` };
  }
  if (positions > MAX_GEOMETRY_POSITIONS) {
    return { ok: false, reason: `${positions} positions (limit ${MAX_GEOMETRY_POSITIONS})` };
  }
  return { ok: true, geometry: geometry as Polygon | MultiPolygon };
}
