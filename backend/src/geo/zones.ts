import type { MultiPolygon, Polygon } from 'geojson';

/** Looks up the boundary of an NWS zone (UGC code such as CAZ006 or CAC085). */
export interface ZoneGeometryProvider {
  getZoneGeometry(ugc: string): Promise<Polygon | MultiPolygon | null>;
}

type FetchFn = (
  url: string,
  init?: { headers?: Record<string, string> },
) => Promise<{
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
}>;

/**
 * Fetches zone geometry from api.weather.gov, caching results (including "not found")
 * in memory for the life of the Lambda container. `fetchFn` is injectable for tests.
 */
export class NwsZoneProvider implements ZoneGeometryProvider {
  private cache = new Map<string, Promise<Polygon | MultiPolygon | null>>();

  constructor(
    private fetchFn: FetchFn = fetch as unknown as FetchFn,
    private baseUrl = 'https://api.weather.gov',
  ) {}

  getZoneGeometry(ugc: string): Promise<Polygon | MultiPolygon | null> {
    const key = ugc.toUpperCase();
    let hit = this.cache.get(key);
    if (!hit) {
      hit = this.load(key);
      this.cache.set(key, hit);
      // Do not cache failures (network errors); only successes and 404s stay cached.
      hit.catch(() => this.cache.delete(key));
    }
    return hit;
  }

  private async load(ugc: string): Promise<Polygon | MultiPolygon | null> {
    const match = /^([A-Z]{2})([ZC])(\d{3})$/.exec(ugc);
    if (!match) return null;
    // Third letter C = county; Z = public forecast zone, or a fire weather zone
    // (fire zone ids can differ from forecast zone ids, so try forecast, then fire).
    const types = match[2] === 'C' ? ['county'] : ['forecast', 'fire'];
    for (const type of types) {
      const geometry = await this.fetchZone(type, ugc);
      if (geometry) return geometry;
    }
    return null;
  }

  private async fetchZone(type: string, ugc: string) {
    const res = await this.fetchFn(`${this.baseUrl}/zones/${type}/${ugc}`, {
      headers: { Accept: 'application/geo+json', 'User-Agent': 'relay-capstone (SJSU)' },
    });
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`NWS zone lookup failed for ${ugc}: HTTP ${res.status}`);
    const body = (await res.json()) as { geometry?: Polygon | MultiPolygon | null };
    return body.geometry ?? null;
  }
}
