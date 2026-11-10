import { describe, expect, it } from 'vitest';
import { MemoryDeviceStore } from '../../backend/src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../backend/src/adapters/memory/place-store.js';
import type { Device, Place } from '../../backend/src/ports.js';
import type { PushTarget } from '../../backend/src/scale/contracts.js';
import { FakeGeocoder, isOutsideTry } from '../src/suite/tests/t9-surge/geocoder.js';
import {
  IndexedDeviceStore,
  IndexedPlaceStore,
  OverlayCellIndex,
} from '../src/suite/tests/t9-surge/stores.js';

function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const device = (id: string, at: string): Device => ({
  id,
  credentialHash: 'h',
  expoPushToken: null,
  language: 'en',
  createdAt: at,
});

describe('indexed device store matches the backend memory store', () => {
  it('gives the same answers over a random run of registrations and moves', async () => {
    const clock = { t: Date.parse('2026-05-01T00:00:00Z') };
    const now = { now: () => new Date(clock.t) };
    const a = new MemoryDeviceStore(now);
    const b = new IndexedDeviceStore(now);
    const r = rng(7);
    const ids = Array.from({ length: 40 }, (_, i) => `d${i}`);
    for (const id of ids) {
      const d = device(id, new Date(clock.t).toISOString());
      await a.create(d);
      await b.create(d);
    }
    const tokens = Array.from({ length: 8 }, (_, i) => `tok-${i}`);
    for (let step = 0; step < 600; step++) {
      clock.t += Math.floor(r() * 3 * 3600_000);
      const id = ids[Math.floor(r() * ids.length)] as string;
      const token = tokens[Math.floor(r() * tokens.length)] as string;
      const target: PushTarget =
        r() < 0.5
          ? { platform: 'ios', token }
          : { platform: 'android', token, ...(r() < 0.5 ? { project: 'p1' } : {}) };
      const run = async (s: MemoryDeviceStore | IndexedDeviceStore) =>
        s.assignPushTarget(id, target, 'en').then(
          (x) => ({ ok: true as const, displaced: [...x.displaced].sort() }),
          (e: unknown) => ({ ok: false as const, name: e instanceof Error ? e.name : 'x' }),
        );
      expect(await run(b), `step ${step}`).toEqual(await run(a));
      if (step % 50 === 0) {
        for (const d of ids) {
          expect(await b.get(d)).toEqual(await a.get(d));
          expect(await b.pushTargetMeta(d)).toEqual(await a.pushTargetMeta(d));
        }
      }
    }
    for (const d of ids) expect(await b.get(d)).toEqual(await a.get(d));
  });

  it('refuses the third move of a token inside a day (200, 200, 200, 409)', async () => {
    const now = { now: () => new Date('2026-05-01T00:00:00Z') };
    const s = new IndexedDeviceStore(now);
    for (const id of ['a', 'b', 'c', 'd']) await s.create(device(id, '2026-05-01T00:00:00Z'));
    const t: PushTarget = { platform: 'ios', token: 'x'.repeat(64) };
    await s.assignPushTarget('a', t, 'en');
    await s.assignPushTarget('b', t, 'en');
    await s.assignPushTarget('c', t, 'en');
    await expect(s.assignPushTarget('d', t, 'en')).rejects.toThrow(/push token/);
    expect(s.holding).toBe(1);
  });
});

describe('indexed place store and overlay index', () => {
  it('lists a device places in creation order like the memory store', async () => {
    const a = new MemoryPlaceStore();
    const b = new IndexedPlaceStore();
    const place = (id: string, deviceId: string, at: number): Place => ({
      id,
      deviceId,
      label: id,
      h3Cell: `c${at % 3}`,
      createdAt: new Date(at).toISOString(),
    });
    for (const [i, p] of [
      place('p1', 'd1', 3000),
      place('p2', 'd1', 1000),
      place('p3', 'd2', 2000),
    ].entries()) {
      await a.add(p);
      await b.add(p);
      expect(await b.listForDevice('d1')).toEqual(await a.listForDevice('d1'));
      expect(i).toBeGreaterThanOrEqual(0);
    }
    expect(await b.remove('d1', 'p1')).toBe(true);
    expect(await a.remove('d1', 'p1')).toBe(true);
    expect(await b.listForDevice('d1')).toEqual(await a.listForDevice('d1'));
    expect(await b.removeAllForDevice('d2')).toBe(1);
    expect(b.size).toBe(1);
  });

  it('keeps entries per cell, patches targets and counts the ones with a target', async () => {
    const idx = new OverlayCellIndex();
    const target: PushTarget = { platform: 'ios', token: 'a'.repeat(64) };
    await idx.put([
      { h3Cell: 'c1', deviceId: 'd1', placeId: 'p1', target, language: 'en' },
      { h3Cell: 'c1', deviceId: 'd2', placeId: 'p2', target: null, language: 'en' },
    ]);
    expect(idx.size).toBe(2);
    expect(idx.withTarget).toBe(1);
    await idx.updateDevice('d1', [{ h3Cell: 'c1', placeId: 'p1' }], {
      target: null,
      language: 'es',
    });
    expect(idx.withTarget).toBe(0);
    expect(
      idx
        .entriesForCell('c1')
        .map((e) => e.language)
        .sort(),
    ).toEqual(['en', 'es']);
    await idx.remove('c1', 'd1', 'p1');
    expect(idx.size).toBe(1);
  });
});

describe('fake geocoder', () => {
  it('is deterministic, lands in the area, and refuses about 1 first try in 100', async () => {
    const { cellForPoint } = await import('../../backend/src/geo/h3-cells.js');
    const { COUNTY_BOUNDS } = await import('../src/seed.js');
    const cells = new Set<string>();
    for (let i = 0; i < 400; i++) {
      const lat =
        COUNTY_BOUNDS.south +
        (((i * 37) % 100) / 100) * (COUNTY_BOUNDS.north - COUNTY_BOUNDS.south);
      const lng =
        COUNTY_BOUNDS.west + (((i * 53) % 100) / 100) * (COUNTY_BOUNDS.east - COUNTY_BOUNDS.west);
      cells.add(cellForPoint(lat, lng));
    }
    const g = new FakeGeocoder(cells);
    const one = await g.geocode('t9 10 try 0');
    expect(await g.geocode('t9 10 try 0')).toEqual(one);
    expect(one?.state).toBe('CA');
    expect(cells.has(cellForPoint(one?.latitude as number, one?.longitude as number))).toBe(true);
    const outside = await g.geocode('t9 3 try 0');
    expect(isOutsideTry(3, 0)).toBe(true);
    expect(outside?.state).toBe('NV');
    expect((await g.geocode('t9 3 try 1'))?.state).toBe('CA');
    expect(await g.geocode('garbage')).toBeNull();
  });
});
