/**
 * A deterministic stand-in for the address geocoder (Amazon Location in AWS): no network. The same
 * address always gives the same point. Points land inside the alert's area (so every registered place
 * is one the active alert covers), except that about 1 in 100 first tries returns a point in Nevada,
 * which the real API must refuse (422 outside_california); the load driver then retries the place
 * with a new address, like a person correcting a typo.
 */
import { cellToLatLng } from 'h3-js';
import { cellForPoint } from '../../../../../backend/src/geo/h3-cells.js';
import type { Geocoder } from '../../../../../backend/src/ports.js';
import { COUNTY_BOUNDS } from '../../../seed.js';

export const addressFor = (n: number, attempt: number): string => `t9 ${n} try ${attempt}`;

/** Roughly one first try in a hundred is "outside California". */
export const isOutsideTry = (n: number, attempt: number): boolean => attempt === 0 && n % 97 === 3;

function mulberry(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export class FakeGeocoder implements Geocoder {
  private readonly fallback: { lat: number; lng: number };
  calls = 0;
  outside = 0;

  constructor(private readonly areaCells: ReadonlySet<string>) {
    const first = [...areaCells].sort()[0];
    if (!first) throw new Error('the alert area has no cells');
    const [lat, lng] = cellToLatLng(first);
    this.fallback = { lat: lat as number, lng: lng as number };
  }

  async geocode(address: string): Promise<{
    latitude: number;
    longitude: number;
    shortAddress: string;
    state: string;
  } | null> {
    this.calls++;
    const m = /^t9 (\d+) try (\d+)$/.exec(address);
    if (!m) return null;
    const n = Number(m[1]);
    const attempt = Number(m[2]);
    if (isOutsideTry(n, attempt)) {
      this.outside++;
      return {
        latitude: 36.17,
        longitude: -115.14,
        shortAddress: 'Fremont Street, Las Vegas',
        state: 'NV',
      };
    }
    const rng = mulberry(n * 7919 + attempt * 104729 + 17);
    for (let i = 0; i < 60; i++) {
      const lat = COUNTY_BOUNDS.south + rng() * (COUNTY_BOUNDS.north - COUNTY_BOUNDS.south);
      const lng = COUNTY_BOUNDS.west + rng() * (COUNTY_BOUNDS.east - COUNTY_BOUNDS.west);
      if (this.areaCells.has(cellForPoint(lat, lng))) {
        return {
          latitude: lat,
          longitude: lng,
          shortAddress: 'Main Street, San Jose',
          state: 'CA',
        };
      }
    }
    return {
      latitude: this.fallback.lat,
      longitude: this.fallback.lng,
      shortAddress: 'Main Street, San Jose',
      state: 'CA',
    };
  }
}
