import { getHexagonAreaAvg, latLngToCell, polygonToCells } from 'h3-js';
import type { MultiPolygon, Polygon, Position } from 'geojson';
import type { ZoneGeometryProvider } from './zones.js';

/** H3 resolution used for matching (will move to @relay/shared). Cells are ~5 km2. */
export const H3_RESOLUTION = 7;

/** Safety limit: a broken geometry must not flood delivery with cells. */
export const MAX_CELLS_PER_AREA = 200_000;

/** Boundary sampling distance in metres. Much smaller than a res-7 cell (~1.2 km edge). */
const BOUNDARY_STEP_M = 100;

export interface AlertArea {
  geometry: Polygon | MultiPolygon | null;
  ugcCodes: string[];
}

/** A zone lookup failed (network, timeout, HTTP 5xx). Transient: retry on the next poll. */
export class ZoneLookupError extends Error {
  constructor(
    readonly ugc: string,
    cause: unknown,
  ) {
    super(
      `zone lookup failed for ${ugc}: ${cause instanceof Error ? cause.message : String(cause)}`,
    );
    this.name = 'ZoneLookupError';
  }
}

/** The area resolved to no cells at all, so the alert would reach nobody. Never save it silently. */
export class NoCellsError extends Error {
  constructor(readonly unknownUgcs: string[]) {
    super(
      `alert area resolved to 0 H3 cells (unknown zones: ${unknownUgcs.join(', ') || 'none'}); ` +
        'refusing to save an alert that reaches nobody',
    );
    this.name = 'NoCellsError';
  }
}

/** The geometry is broken (huge). Permanent: retrying can never help. */
export class TooManyCellsError extends Error {
  constructor() {
    super(
      `Alert area covers more than ${MAX_CELLS_PER_AREA} H3 cells at resolution ${H3_RESOLUTION}; ` +
        'the geometry is probably broken, refusing to fan it out',
    );
    this.name = 'TooManyCellsError';
  }
}

/** H3 cell for a saved place. */
export function cellForPoint(lat: number, lng: number): string {
  return latLngToCell(lat, lng, H3_RESOLUTION);
}

/**
 * All H3 cells covering an alert area, sorted and deduplicated.
 * Uses the alert's own geometry when present; otherwise unions the geometry of each UGC zone.
 *
 * Failure rules (an alert must never silently reach nobody):
 * - a zone lookup that fails (network error) throws ZoneLookupError, so the feature is retried;
 * - a zone the provider reports as unknown (404 -> null) is skipped with a warning, because
 *   retrying can never fix it, but if that leaves 0 cells in total we throw NoCellsError.
 */
export async function cellsForAlertArea(
  area: AlertArea,
  zones: ZoneGeometryProvider,
  opts: { warn?(msg: string, fields: Record<string, unknown>): void } = {},
): Promise<string[]> {
  const polygons: Position[][][] = [];
  const unknownUgcs: string[] = [];
  if (area.geometry) {
    polygons.push(...toPolygons(area.geometry));
  } else {
    for (const ugc of area.ugcCodes) {
      let geometry: Polygon | MultiPolygon | null;
      try {
        geometry = await zones.getZoneGeometry(ugc);
      } catch (err) {
        throw new ZoneLookupError(ugc, err);
      }
      if (geometry) {
        polygons.push(...toPolygons(geometry));
      } else {
        unknownUgcs.push(ugc);
        opts.warn?.('unknown UGC zone, skipped', { ugc });
      }
    }
  }

  const cells = new Set<string>();
  for (const rings of polygons) {
    const outer = rings[0];
    if (!outer) continue;
    // Cheap size estimate first, so a huge polygon never reaches polygonToCells.
    if (estimateCellCount(outer) > MAX_CELLS_PER_AREA) throw tooManyCells();
    for (const cell of cellsForPolygon(rings)) cells.add(cell);
    if (cells.size > MAX_CELLS_PER_AREA) throw tooManyCells();
  }
  if (cells.size === 0) throw new NoCellsError(unknownUgcs);
  return [...cells].sort();
}

type LngLat = [number, number];

/** A GeoJSON position as a [lng, lat] pair (any elevation is dropped). */
function xy(p: Position): LngLat {
  return [p[0] ?? 0, p[1] ?? 0];
}

function toPolygons(g: Polygon | MultiPolygon): Position[][][] {
  return g.type === 'Polygon' ? [g.coordinates] : g.coordinates;
}

/** rings[0] is the outer ring, the rest are holes (GeoJSON order: [lng, lat]). */
function cellsForPolygon(rings: Position[][]): string[] {
  // polygonToCells only returns cells whose CENTER lies inside the polygon (holes respected).
  // A cell that is only partly inside, such as one holding a home just inside the edge,
  // would be missed. So we also add the cell of every point sampled along each ring
  // (outer and holes): any cell the boundary passes through is included.
  const cells = new Set<string>(polygonToCells(rings, H3_RESOLUTION, true));
  for (const ring of rings) {
    for (const [lng, lat] of densify(ring)) cells.add(latLngToCell(lat, lng, H3_RESOLUTION));
  }
  return [...cells];
}

/** Points along a ring at most BOUNDARY_STEP_M apart (linear in lng/lat, fine at this scale). */
function densify(ring: Position[]): LngLat[] {
  const out: LngLat[] = [];
  for (let i = 0; i < ring.length - 1; i++) {
    const a = ring[i];
    const b = ring[i + 1];
    if (!a || !b) continue;
    const [x1, y1] = xy(a);
    const [x2, y2] = xy(b);
    const dyM = (y2 - y1) * 111_320;
    const dxM = (x2 - x1) * 111_320 * Math.cos((((y1 + y2) / 2) * Math.PI) / 180);
    const steps = Math.max(1, Math.ceil(Math.hypot(dxM, dyM) / BOUNDARY_STEP_M));
    for (let s = 0; s < steps; s++) {
      out.push([x1 + ((x2 - x1) * s) / steps, y1 + ((y2 - y1) * s) / steps]);
    }
  }
  const last = ring[ring.length - 1];
  if (last) out.push(xy(last));
  return out;
}

/** Rough cell count from the outer ring's bounding box (an upper bound for the polygon). */
function estimateCellCount(outer: Position[]): number {
  let minX = Infinity,
    maxX = -Infinity,
    minY = Infinity,
    maxY = -Infinity;
  for (const p of outer) {
    const [x, y] = xy(p);
    minX = Math.min(minX, x);
    maxX = Math.max(maxX, x);
    minY = Math.min(minY, y);
    maxY = Math.max(maxY, y);
  }
  const midLat = ((minY + maxY) / 2) * (Math.PI / 180);
  const km2 = (maxX - minX) * 111.32 * Math.cos(midLat) * (maxY - minY) * 111.32;
  return km2 / getHexagonAreaAvg(H3_RESOLUTION, 'km2');
}

function tooManyCells(): Error {
  return new TooManyCellsError();
}
