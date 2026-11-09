import { describe, expect, it, vi } from 'vitest';
import {
  cellsForAlertArea,
  InvalidGeometryError,
  TooManyCellsError,
  validateGeometry,
} from '../../src/geo/index.js';
import type { ZoneGeometryProvider } from '../../src/geo/index.js';

const sq = [
  [-121.9, 37.3],
  [-121.8, 37.3],
  [-121.8, 37.4],
  [-121.9, 37.4],
  [-121.9, 37.3],
];
const poly = (...rings: unknown[]) => ({ type: 'Polygon', coordinates: rings });
const none: ZoneGeometryProvider = { getZoneGeometry: async () => null };

// The SEC-B-5 probe cases (scratchpad/probes/sec-b/p5.mts) plus the obvious neighbours.
const BAD: Array<[string, unknown]> = [
  [
    'one repeated point (4 positions)',
    poly([
      [-121.9, 37.3],
      [-121.9, 37.3],
      [-121.9, 37.3],
      [-121.9, 37.3],
    ]),
  ],
  [
    '3 positions',
    poly([
      [-121.9, 37.3],
      [-121.8, 37.3],
      [-121.9, 37.3],
    ]),
  ],
  [
    'collinear',
    poly([
      [-121.9, 37.3],
      [-121.8, 37.3],
      [-121.7, 37.3],
      [-121.9, 37.3],
    ]),
  ],
  [
    'out-of-range 1e308',
    poly([
      [1e308, 1e308],
      [-1e308, 1e308],
      [-1e308, -1e308],
      [1e308, 1e308],
    ]),
  ],
  ['empty coordinates', { type: 'Polygon', coordinates: [] }],
  ['one-number positions', poly([[-121.9], [-121.8], [-121.8], [-121.9]])],
  [
    'not closed',
    poly([
      [-121.9, 37.3],
      [-121.8, 37.3],
      [-121.8, 37.4],
      [-121.9, 37.4],
    ]),
  ],
  [
    'Infinity',
    poly([
      [-121.9, 37.3],
      [Infinity, 37.3],
      [-121.8, 37.4],
      [-121.9, 37.3],
    ]),
  ],
  [
    'NaN',
    poly([
      [-121.9, 37.3],
      [NaN, 37.3],
      [-121.8, 37.4],
      [-121.9, 37.3],
    ]),
  ],
  [
    'string coordinate',
    poly([
      [-121.9, 37.3],
      ['-121.8', 37.3],
      [-121.8, 37.4],
      [-121.9, 37.3],
    ]),
  ],
  [
    'degenerate hole',
    poly(sq, [
      [-121.85, 37.35],
      [-121.84, 37.35],
      [-121.83, 37.35],
      [-121.85, 37.35],
    ]),
  ],
  ['empty multipolygon', { type: 'MultiPolygon', coordinates: [] }],
  [
    'one bad polygon in a multipolygon',
    {
      type: 'MultiPolygon',
      coordinates: [
        [sq],
        [
          [
            [0, 0],
            [1, 1],
            [0, 0],
          ],
        ],
      ],
    },
  ],
  ['wrong type', { type: 'Point', coordinates: [-121.9, 37.3] }],
];

describe('validateGeometry (SEC-B-5)', () => {
  it.each(BAD)('rejects %s', (_name, g) => {
    expect(validateGeometry(g).ok).toBe(false);
  });

  it('accepts a normal polygon, a polygon with a hole and a multipolygon', () => {
    expect(validateGeometry(poly(sq)).ok).toBe(true);
    const hole = [
      [-121.85, 37.33],
      [-121.84, 37.33],
      [-121.84, 37.36],
      [-121.85, 37.33],
    ];
    expect(validateGeometry(poly(sq, hole)).ok).toBe(true);
    expect(validateGeometry({ type: 'MultiPolygon', coordinates: [[sq], [sq]] }).ok).toBe(true);
  });

  it('cellsForAlertArea throws the permanent InvalidGeometryError, not a generic h3 error', async () => {
    for (const [, g] of BAD.slice(0, 6)) {
      await expect(
        cellsForAlertArea({ geometry: g as never, ugcCodes: [] }, none),
      ).rejects.toBeInstanceOf(InvalidGeometryError);
    }
  });
});

describe('cellsForAlertArea: unknown zones and holes', () => {
  const known = poly([
    [-122.0, 37.2],
    [-121.8, 37.2],
    [-121.8, 37.4],
    [-122.0, 37.4],
    [-122.0, 37.2],
  ]);
  const zones: ZoneGeometryProvider = {
    getZoneGeometry: async (ugc) => (ugc === 'CAZ513' ? (known as never) : null),
  };

  it('REL-7: reports the unknown zones, and still returns the cells of the known one', async () => {
    const seen: Array<[string[], number]> = [];
    const warn = vi.fn();
    const cells = await cellsForAlertArea(
      { geometry: null, ugcCodes: ['CAZ513', 'CAZ999', 'CAZ998'] },
      zones,
      { warn, onUnknownZones: (u, total) => seen.push([u, total]) },
    );
    const knownOnly = await cellsForAlertArea({ geometry: known as never, ugcCodes: [] }, none);
    expect(cells.length).toBeGreaterThan(0);
    expect(cells).toEqual(knownOnly);
    expect(seen).toEqual([[['CAZ999', 'CAZ998'], 3]]);
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it('does not report anything when every zone is known', async () => {
    const onUnknownZones = vi.fn();
    await cellsForAlertArea({ geometry: null, ugcCodes: ['CAZ513'] }, zones, { onUnknownZones });
    expect(onUnknownZones).not.toHaveBeenCalled();
  });

  it('a zone with unusable geometry counts as unknown, the others still resolve', async () => {
    const mixed: ZoneGeometryProvider = {
      getZoneGeometry: async (ugc) =>
        (ugc === 'CAZ513'
          ? known
          : poly([
              [0, 0],
              [1, 1],
              [0, 0],
            ])) as never,
    };
    const onUnknownZones = vi.fn();
    const cells = await cellsForAlertArea(
      { geometry: null, ugcCodes: ['CAZ513', 'CAZ600'] },
      mixed,
      {
        onUnknownZones,
      },
    );
    expect(cells.length).toBeGreaterThan(0);
    expect(onUnknownZones).toHaveBeenCalledWith(['CAZ600'], 2);
  });

  it('SEC-B-10: a huge hole ring is size-checked like the outer ring', async () => {
    const hugeHole = [
      [-179, -80],
      [179, -80],
      [179, 80],
      [-179, 80],
      [-179, -80],
    ];
    await expect(
      cellsForAlertArea(
        {
          geometry: poly((known as { coordinates: unknown[] }).coordinates[0], hugeHole) as never,
          ugcCodes: [],
        },
        none,
      ),
    ).rejects.toBeInstanceOf(TooManyCellsError);
  });
});
