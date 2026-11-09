import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { devNull, tmpdir } from 'node:os';
import { join } from 'node:path';
import { cellToLatLng } from 'h3-js';
import { describe, expect, it } from 'vitest';
import {
  COUNTY_BOUNDS,
  SeedStats,
  URBAN_CENTERS,
  generateDevices,
  generateEntries,
  readNdjson,
  seedStats,
  tapStats,
  writeNdjson,
  type CellIndexEntry,
} from '../src/seed.js';

function hashEntries(count: number, seed: number): string {
  const h = createHash('sha256');
  for (const e of generateEntries({ devices: count, seed })) h.update(JSON.stringify(e));
  return h.digest('hex');
}

describe('seed generator', () => {
  it('is deterministic for a seed and differs between seeds', () => {
    expect(hashEntries(5000, 11)).toBe(hashEntries(5000, 11));
    expect(hashEntries(5000, 11)).not.toBe(hashEntries(5000, 12));
  });

  it('emits well-formed entries', () => {
    const entries = [...generateEntries({ devices: 2000, seed: 3 })];
    for (const e of entries) {
      expect(e.h3Cell).toMatch(/^87[0-9a-f]{13}$/); // H3 resolution 7
      expect(e.deviceId).toMatch(/^dev-\d{8}$/);
      expect(e.placeId).toMatch(/^p[123]$/);
      expect(['en', 'es']).toContain(e.language);
      if (e.target?.platform === 'ios') expect(e.target.token).toMatch(/^[0-9a-f]{64}$/);
      else expect(e.target?.token).toMatch(/^[A-Za-z0-9_-]{22}:APA91b[A-Za-z0-9_-]{132}$/);
    }
    // Entry keys (deviceId#placeId) are unique.
    expect(new Set(entries.map((e) => `${e.deviceId}#${e.placeId}`)).size).toBe(entries.length);
    // All places of one device share its target and language.
    const byDevice = new Map<string, CellIndexEntry[]>();
    for (const e of entries) byDevice.set(e.deviceId, [...(byDevice.get(e.deviceId) ?? []), e]);
    for (const list of byDevice.values()) {
      expect(new Set(list.map((e) => e.target?.token)).size).toBe(1);
      expect(new Set(list.map((e) => e.language)).size).toBe(1);
    }
    // Tokens are unique across devices.
    const tokens = new Set(entries.map((e) => e.target?.token));
    expect(tokens.size).toBe(byDevice.size);
  });

  it('has the stated platform, language, multi-place and urban mix (100k devices)', () => {
    const N = 100_000;
    let ios = 0;
    let es = 0;
    let multi = 0;
    let places = 0;
    let urban = 0;
    for (const d of generateDevices({ devices: N, seed: 21 })) {
      if (d.target.platform === 'ios') ios++;
      if (d.language === 'es') es++;
      if (d.places.length > 1) multi++;
      expect(d.places.length).toBeLessThanOrEqual(3);
      for (const p of d.places) {
        places++;
        if (p.urban) urban++;
      }
    }
    expect(Math.abs(ios / N - 0.55)).toBeLessThan(0.01);
    expect(Math.abs(es / N - 0.2)).toBeLessThan(0.01);
    expect(Math.abs(multi / N - 0.1)).toBeLessThan(0.01);
    expect(Math.abs(urban / places - 0.7)).toBeLessThan(0.01);
  });

  it('places devices inside the county and concentrates them around the urban centers', () => {
    let nearCenter = 0;
    let total = 0;
    const pad = 0.03; // a cell center can sit a little outside the box its points were drawn in
    for (const e of generateEntries({ devices: 20_000, seed: 5 })) {
      const [lat, lng] = cellToLatLng(e.h3Cell) as [number, number];
      expect(lat).toBeGreaterThan(COUNTY_BOUNDS.south - pad);
      expect(lat).toBeLessThan(COUNTY_BOUNDS.north + pad);
      expect(lng).toBeGreaterThan(COUNTY_BOUNDS.west - pad);
      expect(lng).toBeLessThan(COUNTY_BOUNDS.east + pad);
      total++;
      if (URBAN_CENTERS.some((c) => Math.hypot(lat - c.lat, lng - c.lng) < 0.15)) nearCenter++;
    }
    // 70% are drawn urban; a few uniform ones also land near a center, so a bit above 70%.
    expect(nearCenter / total).toBeGreaterThan(0.7);
    expect(nearCenter / total).toBeLessThan(0.85);
  });

  it('reports cells used, devices per cell and the densest cell', () => {
    const s = seedStats({ devices: 50_000, seed: 8 });
    expect(s.devices).toBe(50_000);
    expect(s.entries).toBeGreaterThan(50_000);
    expect(s.entries).toBeLessThan(50_000 * 1.25);
    expect(s.cellsUsed).toBeGreaterThan(100);
    expect(s.devicesPerCell.p50).toBeLessThanOrEqual(s.devicesPerCell.p95);
    expect(s.devicesPerCell.p95).toBeLessThanOrEqual(s.devicesPerCell.max);
    expect(s.densestCell?.devices).toBe(s.devicesPerCell.max);
    // Dense urban cells hold far more than the typical cell.
    expect(s.devicesPerCell.max).toBeGreaterThan(10 * s.devicesPerCell.p50);
    expect(s.platform.ios + s.platform.android).toBe(50_000);
    expect(s.language.en + s.language.es).toBe(50_000);
  });

  it('counts a device once per cell even when two of its places share a cell', () => {
    const stats = new SeedStats();
    const base = { deviceId: 'd1', target: null, language: 'en' as const };
    stats.add({ ...base, h3Cell: 'c1', placeId: 'p1' });
    stats.add({ ...base, h3Cell: 'c1', placeId: 'p2' });
    stats.add({ ...base, deviceId: 'd2', h3Cell: 'c1', placeId: 'p1' });
    const s = stats.summary();
    expect(s.devices).toBe(2);
    expect(s.entries).toBe(3);
    expect(s.devicesPerCell.max).toBe(2);
    expect(s.multiPlaceDevices).toBe(1);
  });

  it('writes NDJSON in chunks and reads it back', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'relay-seed-'));
    try {
      const file = join(dir, 'cells.ndjson');
      const stats = new SeedStats();
      const n = await writeNdjson(
        tapStats(generateEntries({ devices: 3000, seed: 2 }), stats),
        file,
        {
          chunkChars: 4096,
        },
      );
      const expected = [...generateEntries({ devices: 3000, seed: 2 })];
      expect(n).toBe(expected.length);
      expect(stats.summary().entries).toBe(n);
      expect(readFileSync(file, 'utf8').split('\n').filter(Boolean)).toHaveLength(n);
      const back: CellIndexEntry[] = [];
      for await (const e of readNdjson(file)) back.push(e);
      expect(back).toEqual(expected);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('streams 1,000,000 devices to /dev/null within 30 s and without holding them in memory', async () => {
    const baseline = process.memoryUsage().heapUsed;
    let peak = baseline;
    let seen = 0;
    const stats = new SeedStats();
    const sampled = (function* () {
      for (const e of tapStats(generateEntries({ devices: 1_000_000, seed: 1 }), stats)) {
        if (++seen % 100_000 === 0) peak = Math.max(peak, process.memoryUsage().heapUsed);
        yield e;
      }
    })();
    const started = performance.now();
    const written = await writeNdjson(sampled, devNull);
    const seconds = (performance.now() - started) / 1000;

    expect(written).toBe(seen);
    expect(seconds).toBeLessThan(30);
    // Holding 1.15M entries would take several hundred MB; streaming stays far below this.
    expect((peak - baseline) / 1e6).toBeLessThan(200);

    const s = stats.summary();
    expect(s.devices).toBe(1_000_000);
    expect(s.cellsUsed).toBeGreaterThan(800);
    // The densest cell is in the scale plan's ballpark of about 20,000 devices.
    expect(s.densestCell?.devices).toBeGreaterThan(10_000);
    expect(s.densestCell?.devices).toBeLessThan(40_000);
  }, 90_000);
});
