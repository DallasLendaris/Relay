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

  it('a 404 means "unknown zone" (null, cached) while a network error rejects (not cached)', async () => {
    const missing = vi.fn(async (_url: string) => notFound);
    const p404 = new NwsZoneProvider(missing);
    expect(await p404.getZoneGeometry('CAC999')).toBeNull();
    expect(await p404.getZoneGeometry('CAC999')).toBeNull();
    expect(missing).toHaveBeenCalledTimes(1);

    const down = vi.fn(async (_url: string): Promise<never> => {
      throw new Error('ECONNRESET');
    });
    const pDown = new NwsZoneProvider(down);
    await expect(pDown.getZoneGeometry('CAC001')).rejects.toThrow('ECONNRESET');
    await expect(pDown.getZoneGeometry('CAC001')).rejects.toThrow('ECONNRESET');
    expect(down).toHaveBeenCalledTimes(2);
  });

  it('gives every fetch a 5 s abort signal; a timeout is retryable and not cached', async () => {
    // AbortSignal.timeout uses a real internal timer, so control it with our own controller.
    const controller = new AbortController();
    const timeoutSpy = vi.spyOn(AbortSignal, 'timeout').mockReturnValueOnce(controller.signal);
    try {
      let hang = true;
      const signals: Array<AbortSignal | undefined> = [];
      const fetchFn = vi.fn(async (_url: string, init?: { signal?: AbortSignal }) => {
        signals.push(init?.signal);
        if (!hang) return ok(geom);
        return new Promise<never>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => reject(new Error('timed out')));
        });
      });
      const p = new NwsZoneProvider(fetchFn);
      const first = p.getZoneGeometry('CAC085');
      expect(timeoutSpy).toHaveBeenCalledWith(5000);
      expect(signals[0]).toBe(controller.signal);
      controller.abort();
      await expect(first).rejects.toThrow('timed out');

      hang = false;
      expect(await p.getZoneGeometry('CAC085')).toEqual(geom); // retried, not cached
      expect(fetchFn).toHaveBeenCalledTimes(2);
    } finally {
      timeoutSpy.mockRestore();
    }
  });
});
