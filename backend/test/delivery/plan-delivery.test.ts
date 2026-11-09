import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { gridDisk, latLngToCell } from 'h3-js';
import { planDelivery, type PlanDeps } from '../../src/delivery/index.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { verifyCheckInToken } from '../../src/tokens/index.js';
import {
  FakeDevices,
  FakePlaces,
  SJ,
  device,
  place,
  signed,
  tampered,
  verifyAlert,
} from './fakes.js';

const tokenKey = { id: 'k1', secret: 'secret' };
const clock = { now: () => new Date('2026-11-10T10:05:00Z') };
const logger = { info() {}, warn() {}, error() {} };

function setup(over: Partial<PlanDeps> = {}) {
  const places = new FakePlaces();
  const devices = new FakeDevices();
  const queue = new MemoryDeliveryQueue();
  const deps: PlanDeps = {
    places,
    devices,
    queue,
    tokenKey,
    verifyAlert,
    isDeliveryPaused: async () => false,
    clock,
    logger,
    ...over,
  };
  return { places, devices, queue, deps };
}

// A point about 100 km north, well outside the alert disk.
const FAR = { lat: 38.3, lng: -121.9 };

describe('planDelivery', () => {
  it('matches an inside place and not an outside one', async () => {
    const { places, devices, queue, deps } = setup();
    await devices.create(device('in'));
    await devices.create(device('out'));
    await places.add(place('p1', 'in', SJ.lat, SJ.lng));
    await places.add(place('p2', 'out', FAR.lat, FAR.lng));
    const r = await planDelivery(signed(), deps);
    expect(r).toMatchObject({ matchedPlaces: 1, devices: 1, enqueued: 1 });
    expect(queue.jobs.map((j) => j.deviceId)).toEqual(['in']);
  });

  it('sends one job per device even with several matching places', async () => {
    const { places, devices, queue, deps } = setup();
    await devices.create(device('d1'));
    await places.add(place('home', 'd1', SJ.lat, SJ.lng));
    await places.add(place('work', 'd1', SJ.lat + 0.01, SJ.lng + 0.01));
    const r = await planDelivery(signed(), deps);
    expect(r.matchedPlaces).toBe(2);
    expect(r.devices).toBe(1);
    expect(queue.jobs).toHaveLength(1);
  });

  it('skips devices without a push token', async () => {
    const { places, devices, queue, deps } = setup();
    await devices.create(device('d1', null));
    await places.add(place('p', 'd1', SJ.lat, SJ.lng));
    const r = await planDelivery(signed(), deps);
    expect(r).toMatchObject({ skippedNoToken: 1, enqueued: 0 });
    expect(queue.jobs).toHaveLength(0);
  });

  it('claims nothing, so planning the same alert twice is harmless', async () => {
    const { places, devices, queue, deps } = setup();
    await devices.create(device('d1'));
    await places.add(place('p', 'd1', SJ.lat, SJ.lng));
    const alert = signed();
    expect((await planDelivery(alert, deps)).enqueued).toBe(1);
    expect((await planDelivery(alert, deps)).enqueued).toBe(1);
    expect(queue.jobs).toHaveLength(2); // the worker drops the duplicate at send time
  });

  it('refuses a bad signature, a wrong status, inactive states and expiry', async () => {
    const { places, devices, queue, deps } = setup();
    await devices.create(device('d1'));
    await places.add(place('p', 'd1', SJ.lat, SJ.lng));
    const good = signed();
    const cases: Array<[string, Alert, string]> = [
      ['tampered row', tampered(good, { event: 'Evil' }), 'unsigned'],
      ['unsigned', { ...good, signature: null }, 'unsigned'],
      ['test status', signed({ status: 'Test' }), 'not_actual'],
      ['cancelled', signed({ state: 'cancelled' }), 'inactive'],
      ['expired state', signed({ state: 'expired' }), 'inactive'],
      ['past expiry', signed({ expires: '2026-11-10T10:00:00Z' }), 'expired'],
    ];
    for (const [name, alert, reason] of cases) {
      const r = await planDelivery(alert, deps);
      expect(r.refused, name).toBe(reason);
      expect(r.enqueued, name).toBe(0);
    }
    // `ends` later than `expires` keeps the alert alive.
    const r = await planDelivery(
      signed({ expires: '2026-11-10T10:00:00Z', ends: '2026-11-10T12:00:00Z' }),
      deps,
    );
    expect(r.enqueued).toBe(1);
    expect(queue.jobs).toHaveLength(1);
  });

  it('refuses while the kill switch is paused', async () => {
    const { queue, deps } = setup({ isDeliveryPaused: async () => true });
    expect(await planDelivery(signed(), deps)).toMatchObject({ sent: 0, refused: 'paused' });
    expect(queue.jobs).toHaveLength(0);
  });

  it('puts a verifiable check-in token and the grouping key in the job', async () => {
    const { places, devices, queue, deps } = setup();
    await devices.create(device('d1'));
    await places.add(place('p', 'd1', SJ.lat, SJ.lng));
    const alert = signed();
    await planDelivery(alert, deps);
    const job = queue.jobs[0];
    if (!job) throw new Error('no job');
    expect(job.groupingKey).toBe(alert.id);
    expect(job.expiresAt).toBe(alert.expires);
    const keys = new Map([[tokenKey.id, tokenKey.secret]]);
    expect(
      verifyCheckInToken(
        job.checkInToken,
        { alertId: alert.id, deviceId: 'd1', now: clock.now() },
        keys,
      ),
    ).toEqual({ ok: true });
  });

  it('queries places in chunks of 1000 cells and enqueues in batches of 10', async () => {
    const { places, devices, queue, deps } = setup();
    const cells = gridDisk(latLngToCell(SJ.lat, SJ.lng, 7), 30); // 2791 cells
    for (let i = 0; i < 25; i++) {
      await devices.create(device(`d${i}`));
      await places.add(place(`p${i}`, `d${i}`, SJ.lat, SJ.lng));
    }
    await planDelivery(signed({}, cells), deps);
    expect(places.queries).toEqual([1000, 1000, cells.length - 2000]);
    expect(queue.batchSizes).toEqual([10, 10, 5]);
  });

  it('matches 100,000 places over a county-sized alert in under 5 s', async () => {
    const { places, devices, queue, deps } = setup();
    // Santa Clara County is roughly 37.0-37.5 N, 122.2-121.2 W.
    let seed = 42;
    const rand = () => (seed = (seed * 1664525 + 1013904223) % 2 ** 32) / 2 ** 32;
    for (let i = 0; i < 100_000; i++) {
      const id = `d${i % 60_000}`; // some devices have several places
      if (i < 60_000) await devices.create(device(id));
      await places.add(place(`p${i}`, id, 37.0 + rand() * 0.5, -122.2 + rand()));
    }
    // ~4,900 cells, covers the whole county.
    const cells = gridDisk(latLngToCell(37.25, -121.7, 7), 40);
    const t0 = performance.now();
    const r = await planDelivery(signed({}, cells), deps);
    const ms = performance.now() - t0;
    expect(ms).toBeLessThan(5000);
    expect(r.matchedPlaces).toBeGreaterThan(90_000);
    expect(queue.jobs.length).toBe(r.devices);
  }, 60_000);
});
