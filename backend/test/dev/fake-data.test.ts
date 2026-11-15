import { cellToLatLng } from 'h3-js';
import { describe, expect, it } from 'vitest';
import { AlertSchema, CheckInSchema } from '@relay/shared';
import { cellsForAlertArea } from '../../src/geo/index.js';
import {
  SANTA_CLARA_BOUNDS,
  fakeAlerts,
  fakeCheckIns,
  fakePlaces,
  generateDataset,
} from '../../src/dev/fake-data.js';

const { minLat, maxLat, minLng, maxLng } = SANTA_CLARA_BOUNDS;

describe('fake data seeds', () => {
  it('gives identical output for the same seed', async () => {
    const a = await fakeAlerts({ seed: 7, count: 4 });
    const b = await fakeAlerts({ seed: 7, count: 4 });
    expect(b).toEqual(a);

    const placesA = fakePlaces({ seed: 7, count: 50 });
    expect(fakePlaces({ seed: 7, count: 50 })).toEqual(placesA);

    const checkInsA = fakeCheckIns({ seed: 7, alert: a[0]!, count: 20 });
    expect(fakeCheckIns({ seed: 7, alert: a[0]!, count: 20 })).toEqual(checkInsA);
  });

  it('gives different output for a different seed', async () => {
    const a = await fakeAlerts({ seed: 1, count: 3 });
    const b = await fakeAlerts({ seed: 2, count: 3 });
    expect(JSON.stringify(b)).not.toEqual(JSON.stringify(a));

    const placesA = fakePlaces({ seed: 1, count: 20 });
    const placesB = fakePlaces({ seed: 2, count: 20 });
    expect(placesB).not.toEqual(placesA);

    const checkInsA = fakeCheckIns({ seed: 1, alert: a[0]!, count: 10 });
    const checkInsB = fakeCheckIns({ seed: 2, alert: a[0]!, count: 10 });
    expect(checkInsB).not.toEqual(checkInsA);
  });
});

describe('fakeAlerts', () => {
  it('passes AlertSchema and cycles through the three kinds', async () => {
    const alerts = await fakeAlerts({ seed: 42, count: 6 });
    expect(alerts).toHaveLength(6);
    for (const alert of alerts) expect(() => AlertSchema.parse(alert)).not.toThrow();

    expect(alerts.map((a) => a.event)).toEqual([
      'Flash Flood Warning',
      'Red Flag Warning',
      'Heat Advisory',
      'Flash Flood Warning',
      'Red Flag Warning',
      'Heat Advisory',
    ]);
    expect(alerts.map((a) => a.relaySeverity)).toEqual([
      'warning',
      'warning',
      'advisory',
      'warning',
      'warning',
      'advisory',
    ]);
  });

  it('keeps every polygon inside Santa Clara County with H3 cells that match its geometry', async () => {
    const alerts = await fakeAlerts({ seed: 42, count: 12 });
    for (const alert of alerts) {
      const geometry = alert.area.geometry;
      expect(geometry?.type).toBe('Polygon');
      const ring = geometry?.type === 'Polygon' ? (geometry.coordinates[0] ?? []) : [];
      for (const [lng, lat] of ring) {
        expect(lat).toBeGreaterThanOrEqual(minLat);
        expect(lat).toBeLessThanOrEqual(maxLat);
        expect(lng).toBeGreaterThanOrEqual(minLng);
        expect(lng).toBeLessThanOrEqual(maxLng);
      }
      // The stored cells are exactly what the matcher produces for this geometry.
      const cells = await cellsForAlertArea(
        { geometry: alert.area.geometry, ugcCodes: [] },
        { getZoneGeometry: async () => null },
      );
      expect(alert.area.h3Cells).toEqual(cells);
    }
  });

  it('uses the base time for effective and expires', async () => {
    const [alert] = await fakeAlerts({ seed: 42, count: 1, base: '2026-12-10T18:00:00Z' });
    expect(alert?.effective).toBe('2026-12-10T18:00:00.000Z');
    expect(alert?.expires).toBe('2026-12-11T00:00:00.000Z');
  });
});

describe('fakeCheckIns', () => {
  it('passes CheckInSchema and puts every check-in inside its alert cells', async () => {
    const [alert] = await fakeAlerts({ seed: 42, count: 1 });
    expect(alert).toBeDefined();
    const checkIns = fakeCheckIns({ seed: 42, alert: alert!, count: 200 });
    expect(checkIns).toHaveLength(200);

    const cells = new Set(alert!.area.h3Cells);
    const start = Date.parse(alert!.effective);
    const end = Date.parse(alert!.expires);
    for (const checkIn of checkIns) {
      expect(() => CheckInSchema.parse(checkIn)).not.toThrow();
      expect(cells.has(checkIn.h3Cell)).toBe(true);
      expect(checkIn.alertId).toBe(alert!.id);
      const at = Date.parse(checkIn.checkedInAt);
      expect(at).toBeGreaterThanOrEqual(start);
      expect(at).toBeLessThanOrEqual(end);
    }
  });

  it('gives every check-in its own idempotency key', async () => {
    const [alert] = await fakeAlerts({ seed: 3, count: 1 });
    const checkIns = fakeCheckIns({ seed: 3, alert: alert!, count: 100 });
    expect(new Set(checkIns.map((c) => c.idempotencyKey)).size).toBe(100);
  });
});

describe('fakePlaces', () => {
  it('places every place inside the county bounding box', () => {
    const places = fakePlaces({ seed: 42, count: 1000 });
    expect(places).toHaveLength(1000);
    for (const place of places) {
      // Places no longer carry coordinates: check the cell center, give or take one cell (~1.4 km).
      const [lat, lng] = cellToLatLng(place.h3Cell);
      const slack = 0.02;
      expect(lat).toBeGreaterThanOrEqual(minLat - slack);
      expect(lat).toBeLessThanOrEqual(maxLat + slack);
      expect(lng).toBeGreaterThanOrEqual(minLng - slack);
      expect(lng).toBeLessThanOrEqual(maxLng + slack);
      expect(place).not.toHaveProperty('latitude');
      expect(place).not.toHaveProperty('longitude');
    }
  });
});

describe('generateDataset', () => {
  it('shares check-ins across alerts so the total matches the request', async () => {
    const data = await generateDataset({ seed: 42, alerts: 6, checkins: 500, places: 10 });
    expect(data.alerts).toHaveLength(6);
    expect(data.checkins).toHaveLength(500);
    expect(data.places).toHaveLength(10);
  });

  it('refuses check-ins without any alert', async () => {
    await expect(generateDataset({ seed: 1, alerts: 0, checkins: 5, places: 0 })).rejects.toThrow(
      /at least one alert/,
    );
  });
});
