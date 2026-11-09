/**
 * The December demo path, end to end, on in-memory adapters only:
 * NWS feed -> poll -> normalize -> H3 cells -> summaries + signature -> store
 * -> match saved places -> idempotency -> queue -> send -> verify check-in token.
 */
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { InMemorySeenStore } from '../../src/adapters/memory/seen-store.js';
import { noopLogger, planDelivery, sendJobs } from '../../src/delivery/index.js';
import { cellForPoint } from '../../src/geo/index.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { createNwsClient } from '../../src/ingest/nws-client.js';
import { normalizeFeature } from '../../src/ingest/normalize.js';
import { pollOnce } from '../../src/ingest/poll.js';
import { createAlertVerifier, verifyAlertSignature } from '../../src/signing/index.js';
import { verifyCheckInToken } from '../../src/tokens/index.js';
import type { Keyring } from '../../src/config/secrets.js';

const fixture = readFileSync(
  new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url),
  'utf8',
);
const clock = { now: () => new Date('2026-11-09T15:00:00Z') };

const ring = (id: string, secret: string): Keyring => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});
const alertKeys = ring('a1', 'alert-secret');
const tokenKeys = ring('t1', 'token-secret');

/**
 * Serves the fixture for the alerts feed and a small zone polygon near Fresno for zone lookups
 * (two fixture alerts have no geometry of their own). An alert with an unknown zone and no
 * geometry would now fail with "0 cells" instead of being saved silently.
 */
const zoneBody = JSON.stringify({
  geometry: {
    type: 'Polygon',
    coordinates: [
      [
        [-119.9, 36.6],
        [-119.7, 36.6],
        [-119.7, 36.8],
        [-119.9, 36.8],
        [-119.9, 36.6],
      ],
    ],
  },
});
const fakeFetch = (async (input: string | URL | Request) => {
  const url = String(input);
  if (url.includes('/alerts/active')) {
    return new Response(fixture, {
      status: 200,
      headers: { 'content-type': 'application/geo+json' },
    });
  }
  if (url.includes('/zones/')) return new Response(zoneBody, { status: 200 });
  return new Response('{}', { status: 404 });
}) as typeof fetch;

/** A queue whose first `failures` enqueue calls throw (SQS outage), then behaves normally. */
class FlakyQueue extends MemoryDeliveryQueue {
  attempts = 0;
  constructor(private failures: number) {
    super();
  }
  override async enqueue(jobs: Parameters<MemoryDeliveryQueue['enqueue']>[0]): Promise<void> {
    this.attempts += 1;
    if (this.attempts <= this.failures) throw new Error('sqs unavailable');
    await super.enqueue(jobs);
  }
}

function setup(paused = false, queue: MemoryDeliveryQueue = new MemoryDeliveryQueue()) {
  const alertStore = new InMemoryAlertStore();
  const places = new MemoryPlaceStore();
  const devices = new MemoryDeviceStore();
  const idempotency = new MemoryIdempotencyStore(clock);
  const seen = new InMemorySeenStore();
  const engine = createIngestPipeline({
    alertStore,
    places,
    devices,
    queue,
    alertKeys,
    tokenKeys,
    isDeliveryPaused: async () => paused,
    fetch: fakeFetch,
    userAgent: 'relay-test',
    clock,
    logger: noopLogger,
  });
  const poll = () =>
    pollOnce({
      client: createNwsClient({ fetch: fakeFetch, userAgent: 'relay-test' }),
      archive: new MemoryRawArchive(),
      seen,
      ...engine,
      log: () => {},
    });

  const addDevice = async (id: string, lat: number, lng: number) => {
    await devices.create({
      id,
      credentialHash: 'x',
      expoPushToken: `ExponentPushToken[${id}]`,
      language: 'en',
      createdAt: '2026-11-01T00:00:00Z',
    });
    await places.add({
      id: `place-${id}`,
      deviceId: id,
      label: 'Home',
      shortAddress: '1 Main St',
      latitude: lat,
      longitude: lng,
      h3Cell: cellForPoint(lat, lng),
      createdAt: '2026-11-01T00:00:00Z',
    });
  };
  return { alertStore, places, devices, queue, idempotency, poll, addDevice };
}

describe('demo path', () => {
  it('turns a new NWS alert into one queued push for the device inside the polygon', async () => {
    const s = setup();
    await s.addDevice('inside', 37.35, -121.9); // inside the Flash Flood Warning polygon
    await s.addDevice('outside', 34.05, -118.24); // Los Angeles, not covered
    await s.addDevice('outside-fire', 38.5, -121.5); // Sacramento, not covered

    const counts = await s.poll();
    expect(counts.errors).toBe(0);
    expect(counts.applied).toBe(4);

    // Exactly one job, for the inside device.
    expect(s.queue.jobs).toHaveLength(1);
    const job = s.queue.jobs[0];
    expect(job?.deviceId).toBe('inside');

    // The stored alert is signed, verifies, and has both summaries.
    const stored = await s.alertStore.get(job?.alertId ?? '');
    expect(stored?.event).toBe('Flash Flood Warning');
    expect(stored && verifyAlertSignature(stored, alertKeys.all)).toEqual({ ok: true });
    expect(stored?.summaries.map((x) => x.language).sort()).toEqual(['en', 'es']);

    // Send it and check the token in the push data.
    const sender = new MemoryPushSender();
    const results = await sendJobs(s.queue.jobs, {
      sender,
      alerts: s.alertStore,
      logger: noopLogger,
    });
    expect(results).toEqual([{ ok: true }]);
    expect(sender.sent).toHaveLength(1);
    const message = sender.sent[0];
    expect(message?.to).toBe('ExponentPushToken[inside]');
    expect(
      verifyCheckInToken(
        message?.data['checkInToken'],
        { alertId: job?.alertId ?? '', deviceId: 'inside', now: clock.now() },
        tokenKeys.all,
      ),
    ).toEqual({ ok: true });

    // Polling the same feed again creates no new jobs (dedupe).
    await s.poll();
    expect(s.queue.jobs).toHaveLength(1);
  });

  it('retries a failed fan-out: poll 1 has the queue throwing, poll 2 delivers exactly once', async () => {
    const s = setup(false, new FlakyQueue(1));
    await s.addDevice('inside', 37.35, -121.9);

    const first = await s.poll();
    expect(first.errors).toBe(1); // the failed fan-out is counted, not swallowed
    expect(s.queue.jobs).toHaveLength(0);

    // Not marked seen, so the next poll plans it again. The failed attempt's idempotency
    // claim was released, so the device is not skipped as a duplicate.
    const second = await s.poll();
    expect(second.errors).toBe(0);
    expect(s.queue.jobs).toHaveLength(1);
    expect(s.queue.jobs[0]?.deviceId).toBe('inside');

    // Now it is marked seen: a third poll sends nothing more.
    const third = await s.poll();
    expect(third.errors).toBe(0);
    expect(s.queue.jobs).toHaveLength(1);
  });

  it('enqueues nothing while the kill switch is paused', async () => {
    const s = setup(true);
    await s.addDevice('inside', 37.35, -121.9);
    const counts = await s.poll();
    expect(counts.applied).toBe(4); // alerts are still ingested and stored
    expect(s.queue.jobs).toHaveLength(0);
  });

  it('refuses an alert saved straight to the inner store (unsigned)', async () => {
    const s = setup();
    await s.addDevice('inside', 37.35, -121.9);
    const feature = (JSON.parse(fixture) as { features: unknown[] }).features[0];
    const norm = normalizeFeature(feature, { newId: randomUUID });
    if (!norm.ok) throw new Error(norm.reason);
    norm.alert.area.h3Cells = [cellForPoint(37.35, -121.9)];
    await s.alertStore.save(norm.alert); // bypasses the SigningAlertStore

    const stored = await s.alertStore.get(norm.alert.id);
    if (!stored) throw new Error('not stored');
    const plan = await planDelivery(stored, {
      places: s.places,
      devices: s.devices,
      queue: s.queue,
      tokenKey: tokenKeys.current,
      verifyAlert: createAlertVerifier(alertKeys.all),
      isDeliveryPaused: async () => false,
      clock,
      logger: noopLogger,
    });
    expect(plan.refused).toBe('unsigned');
    expect(s.queue.jobs).toHaveLength(0);
  });
});
