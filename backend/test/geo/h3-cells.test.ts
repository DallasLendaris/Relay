import { describe, expect, it } from 'vitest';
import { booleanIntersects, destination, point, polygon as turfPolygon } from '@turf/turf';
import { cellToBoundary, gridDisk } from 'h3-js';
import type { MultiPolygon, Polygon } from 'geojson';
import {
  H3_RESOLUTION,
  MAX_CELLS_PER_AREA,
  cellForPoint,
  cellsForAlertArea,
  type ZoneGeometryProvider,
} from '../../src/geo/index.js';

// Sample shapes around San Jose, [lng, lat], closed rings.
const square: Polygon = {
  type: 'Polygon',
  coordinates: [
    [
      [-122.0, 37.2],
      [-121.8, 37.2],
      [-121.8, 37.4],
      [-122.0, 37.4],
      [-122.0, 37.2],
    ],
  ],
};
const squareWithHole: Polygon = {
  type: 'Polygon',
  coordinates: [
    [
      [-122.1, 37.1],
      [-121.7, 37.1],
      [-121.7, 37.5],
      [-122.1, 37.5],
      [-122.1, 37.1],
    ],
    [
      [-121.98, 37.28],
      [-121.82, 37.28],
      [-121.82, 37.42],
      [-121.98, 37.42],
      [-121.98, 37.28],
    ],
  ],
};
const multi: MultiPolygon = {
  type: 'MultiPolygon',
  coordinates: [
    [
      [
        [-122.0, 37.2],
        [-121.9, 37.2],
        [-121.9, 37.3],
        [-122.0, 37.3],
        [-122.0, 37.2],
      ],
    ],
    [
      [
        [-121.6, 37.0],
        [-121.45, 37.0],
        [-121.5, 37.15],
        [-121.6, 37.0],
      ],
    ],
  ],
};
// Long thin diagonal sliver: many cells are only clipped at a corner.
const sliver: Polygon = {
  type: 'Polygon',
  coordinates: [
    [
      [-122.2, 37.0],
      [-121.7, 37.45],
      [-121.69, 37.44],
      [-122.19, 36.99],
      [-122.2, 37.0],
    ],
  ],
};
// Concave L shape.
const lShape: Polygon = {
  type: 'Polygon',
  coordinates: [
    [
      [-122.0, 37.0],
      [-121.7, 37.0],
      [-121.7, 37.1],
      [-121.9, 37.1],
      [-121.9, 37.4],
      [-122.0, 37.4],
      [-122.0, 37.0],
    ],
  ],
};

/** H3 cell of the point `km` kilometres from [lng, lat] along `bearing` (degrees). */
function cellMovedFrom(lng: number, lat: number, bearing: number, km: number): string {
  const c = destination(point([lng, lat]), km, bearing, { units: 'kilometers' }).geometry
    .coordinates;
  // NaN makes h3 throw, so a missing coordinate fails the test loudly.
  return cellForPoint(c[1] ?? NaN, c[0] ?? NaN);
}

const fakeZones = (table: Record<string, Polygon | MultiPolygon | null>): ZoneGeometryProvider => ({
  getZoneGeometry: async (ugc) => table[ugc] ?? null,
});
const none = fakeZones({});

const cellsFor = (g: Polygon | MultiPolygon) =>
  cellsForAlertArea({ geometry: g, ugcCodes: [] }, none);

describe('cellsForAlertArea', () => {
  it('returns a sorted, deduplicated list for a polygon alert', async () => {
    const cells = await cellsFor(square);
    expect(cells.length).toBeGreaterThan(20);
    expect(cells).toEqual([...new Set(cells)].sort());
  });

  it('builds cells from UGC zones when there is no geometry, and unions them', async () => {
    const zones = fakeZones({ CAZ001: square, CAZ002: multi, CAZ003: square });
    const cells = await cellsForAlertArea(
      { geometry: null, ugcCodes: ['CAZ001', 'CAZ002', 'CAZ003', 'CAZ999'] },
      zones,
    );
    const expected = new Set([...(await cellsFor(square)), ...(await cellsFor(multi))]);
    expect(cells).toEqual([...expected].sort());
  });

  it("prefers the alert's own geometry over zones", async () => {
    const zones = fakeZones({ CAZ001: multi });
    const cells = await cellsForAlertArea({ geometry: square, ugcCodes: ['CAZ001'] }, zones);
    expect(cells).toEqual(await cellsFor(square));
  });

  it('returns an empty list when nothing can be resolved', async () => {
    expect(await cellsForAlertArea({ geometry: null, ugcCodes: ['CAZ404'] }, none)).toEqual([]);
  });

  it('includes a point inside the area and excludes a point well outside', async () => {
    const cells = await cellsFor(square);
    expect(cells).toContain(cellForPoint(37.3, -121.9));
    expect(cells).not.toContain(cellForPoint(37.8, -122.4)); // San Francisco
  });

  it('covers a home 100 m inside every edge', async () => {
    const cells = new Set(await cellsFor(square));
    const insideEdges: { lng: number; lat: number; bearing: number; alongLng: boolean }[] = [
      { lng: -121.9, lat: 37.2, bearing: 0, alongLng: true }, // south edge, move north
      { lng: -121.9, lat: 37.4, bearing: 180, alongLng: true }, // north edge, move south
      { lng: -122.0, lat: 37.3, bearing: 90, alongLng: false }, // west edge, move east
      { lng: -121.8, lat: 37.3, bearing: -90, alongLng: false }, // east edge, move west
    ];
    for (const { lng, lat, bearing, alongLng } of insideEdges) {
      for (let along = -0.08; along <= 0.08; along += 0.01) {
        const cell = alongLng
          ? cellMovedFrom(lng + along, lat, bearing, 0.1)
          : cellMovedFrom(lng, lat + along, bearing, 0.1);
        expect(cells.has(cell)).toBe(true);
      }
    }
  });

  it('handles a polygon with a hole: hole interior excluded, hole edge covered', async () => {
    const cells = new Set(await cellsFor(squareWithHole));
    expect(cells.has(cellForPoint(37.35, -121.9))).toBe(false); // deep inside the hole
    expect(cells.has(cellForPoint(37.46, -121.9))).toBe(true); // in the solid band north of the hole
    // 100 m outside the hole (inside the polygon), near the hole's north edge.
    expect(cells.has(cellMovedFrom(-121.9, 37.42, 0, 0.1))).toBe(true);
  });

  it('handles a MultiPolygon', async () => {
    const cells = new Set(await cellsFor(multi));
    expect(cells.has(cellForPoint(37.25, -121.95))).toBe(true);
    expect(cells.has(cellForPoint(37.05, -121.53))).toBe(true);
    expect(cells.has(cellForPoint(37.2, -121.7))).toBe(false); // gap between the two parts
  });

  it('throws a clear error when the area is absurdly large', async () => {
    const huge: Polygon = {
      type: 'Polygon',
      coordinates: [
        [
          [-125, 30],
          [-110, 30],
          [-110, 45],
          [-125, 45],
          [-125, 30],
        ],
      ],
    };
    await expect(cellsFor(huge)).rejects.toThrow(String(MAX_CELLS_PER_AREA));
  });

  it('uses resolution 7', () => {
    expect(H3_RESOLUTION).toBe(7);
    expect(cellForPoint(37.3, -121.9)).toHaveLength(15);
  });
});

describe('cross-check against @turf/boolean-intersects', () => {
  const shapes: [string, Polygon | MultiPolygon][] = [
    ['square', square],
    ['square with hole', squareWithHole],
    ['multipolygon', multi],
    ['thin diagonal sliver', sliver],
    ['L shape', lShape],
  ];

  it.each(shapes)('every H3 cell that intersects %s is in the result', async (_name, geometry) => {
    const result = new Set(await cellsFor(geometry));
    const feature = { type: 'Feature' as const, properties: {}, geometry };

    // Candidates: every cell within 2 rings of the result (covers anything touching the shape).
    const candidates = new Set<string>();
    for (const c of result) for (const n of gridDisk(c, 2)) candidates.add(n);

    const missing: string[] = [];
    for (const cell of candidates) {
      const ring = cellToBoundary(cell, true); // [lng, lat] order
      const first = ring[0];
      if (first) ring.push(first); // close the ring
      if (booleanIntersects(turfPolygon([ring]), feature) && !result.has(cell)) missing.push(cell);
    }
    expect(missing).toEqual([]);
  });
});
