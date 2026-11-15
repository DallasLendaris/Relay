import { booleanPointInPolygon } from '@turf/boolean-point-in-polygon';
import { cellToLatLng } from 'h3-js';
import type { MultiPolygon, Polygon } from 'geojson';
import type { ZoneGeometryProvider } from '../geo/zones.js';
import { countyUgc } from './jurisdiction.js';

/**
 * Assigns H3 cells to counties. Check-in totals are kept per H3 cell (resolution 7, about 5 km²)
 * and jurisdictions are counties, so every cell must be placed in exactly one county before it
 * can be shown.
 *
 * Rule: a cell belongs to the county that contains its CENTER point. A cell straddling a county
 * line is therefore shown to one county only (never both), and a cell whose center is outside
 * every requested county is not shown at all. DRAFT rule, see docs/agency-auth.md "Decisions".
 */
export interface CellAreaResolver {
  /** Maps each cell that falls inside one of `counties` to that county. Others are left out. */
  assign(cells: readonly string[], counties: readonly string[]): Promise<Map<string, string>>;
}

/** The county boundary is missing, so cells cannot be placed safely: the request fails closed. */
export class AreaDataUnavailableError extends Error {}

type Bbox = [minLng: number, minLat: number, maxLng: number, maxLat: number];

function bboxOf(geometry: Polygon | MultiPolygon): Bbox {
  const box: Bbox = [Infinity, Infinity, -Infinity, -Infinity];
  const rings = geometry.type === 'Polygon' ? geometry.coordinates : geometry.coordinates.flat();
  for (const ring of rings) {
    for (const [lng, lat] of ring) {
      if (lng === undefined || lat === undefined) continue;
      box[0] = Math.min(box[0], lng);
      box[1] = Math.min(box[1], lat);
      box[2] = Math.max(box[2], lng);
      box[3] = Math.max(box[3], lat);
    }
  }
  return box;
}

/**
 * County boundaries from the NWS county zones (UGC CACnnn), read through the same zone provider
 * ingest uses (the S3 zone pack, NWS as fallback). A future owner can swap in Census TIGER
 * boundaries or a precomputed cell-to-county table behind the same interface.
 */
export class ZoneCountyResolver implements CellAreaResolver {
  constructor(private readonly zones: ZoneGeometryProvider) {}

  async assign(
    cells: readonly string[],
    counties: readonly string[],
  ): Promise<Map<string, string>> {
    const shapes = await Promise.all(
      counties.map(async (fips) => {
        const geometry = await this.zones.getZoneGeometry(countyUgc(fips));
        if (!geometry) {
          throw new AreaDataUnavailableError(`no boundary for county ${fips}`);
        }
        return { fips, geometry, bbox: bboxOf(geometry) };
      }),
    );
    const out = new Map<string, string>();
    for (const cell of cells) {
      let lat: number;
      let lng: number;
      try {
        [lat, lng] = cellToLatLng(cell);
      } catch {
        continue; // not a valid H3 index: never shown
      }
      for (const s of shapes) {
        if (lng < s.bbox[0] || lng > s.bbox[2] || lat < s.bbox[1] || lat > s.bbox[3]) continue;
        if (booleanPointInPolygon([lng, lat], s.geometry)) {
          out.set(cell, s.fips);
          break;
        }
      }
    }
    return out;
  }
}
