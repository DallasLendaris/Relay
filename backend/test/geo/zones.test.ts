import { describe, expect, it, vi } from 'vitest';
import type { Polygon } from 'geojson';
import { NwsZoneProvider } from '../../src/geo/index.js';

const geom: Polygon = {
  type: 'Polygon',
  coordinates: [
    [
      [0, 0],
      [1, 0],
      [1, 1],
      [0, 0],
    ],
  ],
};
const ok = (geometry: unknown) => ({ ok: true, status: 200, json: async () => ({ geometry }) });
const notFound = { ok: false, status: 404, json: async () => ({}) };

describe('NwsZoneProvider', () => {
  it('uses the county endpoint for C codes and caches the result', async () => {
    const fetchFn = vi.fn(async (_url: string) => ok(geom));
    const p = new NwsZoneProvider(fetchFn);
    expect(await p.getZoneGeometry('CAC085')).toEqual(geom);
    await p.getZoneGeometry('CAC085');
    expect(fetchFn).toHaveBeenCalledTimes(1);
    expect(fetchFn.mock.calls[0]?.[0]).toBe('https://api.weather.gov/zones/county/CAC085');
  });

  it('tries forecast then fire for Z codes', async () => {
    const fetchFn = vi.fn(async (url: string) =>
      url.includes('/forecast/') ? notFound : ok(geom),
    );
    const p = new NwsZoneProvider(fetchFn);
    expect(await p.getZoneGeometry('CAZ006')).toEqual(geom);
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([
      'https://api.weather.gov/zones/forecast/CAZ006',
      'https://api.weather.gov/zones/fire/CAZ006',
    ]);
  });

  it('returns null for unknown or malformed codes, and does not cache errors', async () => {
    const fetchFn = vi.fn(async (_url: string) => ({
      ok: false,
      status: 500,
      json: async () => ({}),
    }));
    const p = new NwsZoneProvider(fetchFn);
    expect(await p.getZoneGeometry('nonsense')).toBeNull();
    await expect(p.getZoneGeometry('CAC001')).rejects.toThrow('HTTP 500');
    await expect(p.getZoneGeometry('CAC001')).rejects.toThrow('HTTP 500');
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });
});
