/**
 * Picks where the simulated phones stand, from whatever alerts a feed actually holds.
 * A real recording will not have an alert over San José, so `simulate` asks the engine itself:
 * it ingests the feed once in a throwaway relay, then takes points inside the pushable alerts
 * (resident) and a California point no active alert covers (control).
 */
import { cellToLatLng, cellToParent, getResolution, latLngToCell } from 'h3-js';
import type { Alert } from '@relay/shared';
import { isInCalifornia } from '../api/california.js';
import type { LocalRelay } from './relay.js';

export interface Point {
  latitude: number;
  longitude: number;
}

export interface DemoPoints {
  /** Points inside pushable alerts, best first. Try them in order. */
  inside: Array<Point & { event: string }>;
  /** A California point outside every active alert, or null if none of the candidates is. */
  outside: Point | null;
}

const ORDER: Record<string, number> = { emergency: 0, warning: 1, watch: 2, advisory: 3 };

/** Control candidates spread across the state. */
const CONTROL_CANDIDATES: Point[] = [
  { latitude: 34.05, longitude: -118.24 }, // Los Angeles
  { latitude: 37.77, longitude: -122.42 }, // San Francisco
  { latitude: 38.58, longitude: -121.49 }, // Sacramento
  { latitude: 32.72, longitude: -117.16 }, // San Diego
  { latitude: 36.74, longitude: -119.79 }, // Fresno
  { latitude: 40.8, longitude: -124.16 }, // Eureka
  { latitude: 40.59, longitude: -122.39 }, // Redding
  { latitude: 33.83, longitude: -116.55 }, // Palm Springs
  { latitude: 35.37, longitude: -119.02 }, // Bakersfield
];

/** True when the point falls in any of the cells (cells may be of mixed resolution). */
export function covers(cells: readonly string[], p: Point): boolean {
  const set = new Set(cells);
  const resolutions = new Set(cells.map((c) => getResolution(c)));
  for (const res of resolutions) {
    const cell = latLngToCell(p.latitude, p.longitude, res);
    if (set.has(cell)) return true;
  }
  // Parent check catches a point cell finer than a stored coarse cell.
  const fine = latLngToCell(p.latitude, p.longitude, 7);
  return [...resolutions].some((r) => r < 7 && set.has(cellToParent(fine, r)));
}

export async function pickDemoPoints(makeRelay: () => LocalRelay): Promise<DemoPoints> {
  const dry = makeRelay();
  await dry.pollAndDeliver();
  const active: Alert[] = await dry.stores.alerts.listActiveBySource('nws', dry.clock.now());

  const pushable = active
    .filter((a) => a.relaySeverity in ORDER)
    .sort((a, b) => (ORDER[a.relaySeverity] ?? 9) - (ORDER[b.relaySeverity] ?? 9));

  const inside: DemoPoints['inside'] = [];
  for (const alert of pushable) {
    const cells = alert.area.h3Cells;
    // A few cells per alert, so one offshore or out-of-state cell does not end the search.
    const picks = [...new Set([0, Math.floor(cells.length / 2), cells.length - 1])];
    for (const i of picks) {
      const cell = cells[i];
      if (!cell) continue;
      const [latitude, longitude] = cellToLatLng(cell);
      if (isInCalifornia(latitude, longitude)) {
        inside.push({ latitude, longitude, event: alert.event });
      }
    }
  }

  const outside =
    CONTROL_CANDIDATES.find(
      (p) =>
        isInCalifornia(p.latitude, p.longitude) && !active.some((a) => covers(a.area.h3Cells, p)),
    ) ?? null;

  return { inside, outside };
}
