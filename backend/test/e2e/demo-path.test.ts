/**
 * The December demo path, end to end, on in-memory adapters only:
 * NWS feed -> poll -> normalize -> H3 cells -> summaries + signature -> store
 * -> match saved places -> queue -> SQS event -> deliveryWorker (claim, send) -> PushSender.
 * Every scenario asserts on what the PushSender actually received, i.e. what reaches the phone.
 */
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import type { Alert } from '@relay/shared';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryPushSender } from '../../src/adapters/memory/push-sender.js';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { InMemorySeenStore } from '../../src/adapters/memory/seen-store.js';
import { deliveryWorker, noopLogger, planDelivery } from '../../src/delivery/index.js';
import { cellForPoint } from '../../src/geo/index.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { createNwsClient } from '../../src/ingest/nws-client.js';
import { normalizeFeature } from '../../src/ingest/normalize.js';
import { pollOnce } from '../../src/ingest/poll.js';
import { createAlertVerifier, verifyAlertSignature } from '../../src/signing/index.js';
import { verifyCheckInToken } from '../../src/tokens/index.js';
import type { Keyring } from '../../src/config/secrets.js';

const readFixture = (name: string) =>
  readFileSync(new URL(`../fixtures/nws/${name}.json`, import.meta.url), 'utf8');
const mixed = readFixture('active-ca-mixed');
const updateChain = JSON.parse(readFixture('update-chain')) as {
  features: Array<{ properties: Record<string, unknown> }>;
};
const clock = { now: () => new Date('2026-11-09T15:00:00Z') };

const ring = (id: string, secret: string): Keyring => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});
const alertKeys = ring('a1', 'alert-secret');
const tokenKeys = ring('t1', 'token-secret');

// A point inside the Flash Flood Warning polygon (active-ca-mixed) and the Flood Warning
// polygon (update-chain); both are in Santa Clara County.
const INSIDE = { lat: 37.35, lng: -121.9 };

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

/** A feed holding one message from update-chain.json (0 = original, 1 = update), with edits. */
function chainFeed(which: 0 | 1, edit: Record<string, unknown> = {}): string {
  const source = updateChain.features[which];
  if (!source) throw new Error('missing fixture feature');
  const feature = { ...source, properties: { ...source.properties, ...edit } };
  return JSON.stringify({ ...updateChain, features: [feature] });
}

/**
 * Per-test setup. `feed.body` is what the fake NWS serves; change it between polls.
 * Zone lookups get a small polygon, because an alert with no geometry and an unknown zone fails.
 */
function setup(opts: { queue?: MemoryDeliveryQueue; feed?: string } = {}) {
  const feed = { body: opts.feed ?? mixed };
  const fakeFetch = (async (input: string | URL | Request) => {
    const url = String(input);
    if (url.includes('/alerts/active')) {
      return new Response(feed.body, {
        status: 200,
        headers: { 'content-type': 'application/geo+json' },
      });
    }
    if (url.includes('/zones/')) return new Response(zoneBody, { status: 200 });
    return new Response('{}', { status: 404 });
  }) as typeof fetch;

  const queue = opts.queue ?? new MemoryDeliveryQueue();
  const alertStore = new InMemoryAlertStore();
  const places = new MemoryPlaceStore();
  const devices = new MemoryDeviceStore();
  const seen = new InMemorySeenStore();
  const sender = new MemoryPushSender();
  const idempotency = new MemoryIdempotencyStore(clock);
  const kill = { workerPaused: false, ingestPaused: false };

  const engine = createIngestPipeline({
    alertStore,
    places,
    devices,
    queue,
    alertKeys,
    tokenKeys,
    isDeliveryPaused: async () => kill.ingestPaused,
    fetch: fakeFetch,
    userAgent: 'relay-test',
    clock,
    logger: noopLogger,
  });
  const client = createNwsClient({ fetch: fakeFetch, userAgent: 'relay-test' });
  const poll = () =>
    pollOnce({ client, archive: new MemoryRawArchive(), seen, ...engine, log: () => {} });

  // A tiny SQS: every queued job becomes a record; a record stays until the worker acks it.
  let delivered = 0; // how many queue.jobs are already turned into records
  let nextId = 0;
  const inflight: Array<{ messageId: string; body: string }> = [];
  /** Run the worker once over everything not yet acked. Returns the failed message ids. */
  const drain = async (): Promise<string[]> => {
    for (const job of queue.jobs.slice(delivered)) {
      inflight.push({ messageId: `m${nextId++}`, body: JSON.stringify(job) });
    }
    delivered = queue.jobs.length;
    const batch = [...inflight];
    if (!batch.length) return [];
    const res = await deliveryWorker({ Records: batch } as unknown as SQSEvent, {
      sender,
      alerts: alertStore,
      idempotency,
      verifyAlert: createAlertVerifier(alertKeys.all),
      isDeliveryPaused: async () => kill.workerPaused,
      onDeadToken: (token) => devices.clearPushToken(token),
      clock,
      logger: noopLogger,
    });
    const failed = res.batchItemFailures.map((f) => f.itemIdentifier);
    for (const r of batch) {
      if (!failed.includes(r.messageId)) inflight.splice(inflight.indexOf(r), 1);
    }
    return failed;
  };

  const addDevice = async (id: string, lat: number, lng: number, language: 'en' | 'es' = 'en') => {
    await devices.create({
      id,
      credentialHash: 'x',
      expoPushToken: `ExponentPushToken[${id}]`,
      language,
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
  const pushesTo = (id: string) => sender.sent.filter((m) => m.to === `ExponentPushToken[${id}]`);

  return { feed, queue, alertStore, devices, sender, kill, poll, drain, addDevice, pushesTo };
}

/** Throws on the (failAt)th enqueue call, once. Earlier calls succeed, like SQS failing midway. */
class FlakyQueue extends MemoryDeliveryQueue {
  calls = 0;
  constructor(private failAt: number) {
    super();
  }
  override async enqueue(jobs: Parameters<MemoryDeliveryQueue['enqueue']>[0]): Promise<void> {
    this.calls += 1;
    if (this.calls === this.failAt) throw new Error('sqs unavailable');
    await super.enqueue(jobs);
  }
}

describe('demo path', () => {
  it('delivers exactly one push to the device inside the polygon and none outside', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    await s.addDevice('outside', 34.05, -118.24); // Los Angeles, not covered
    await s.addDevice('outside-fire', 38.5, -121.5); // Sacramento, not covered

    const counts = await s.poll();
    expect(counts.errors).toBe(0);
    expect(counts.applied).toBe(4);
    expect(await s.drain()).toEqual([]);

    expect(s.sender.sent).toHaveLength(1);
    expect(s.pushesTo('outside')).toHaveLength(0);
    expect(s.pushesTo('outside-fire')).toHaveLength(0);
    const message = s.pushesTo('inside')[0];
    expect(message?.title).toContain('Flash Flood Warning');

    // The check-in token in the push verifies for exactly this alert and device.
    const alertId = String(message?.data['alertId']);
    expect(
      verifyCheckInToken(
        message?.data['checkInToken'],
        { alertId, deviceId: 'inside', now: clock.now() },
        tokenKeys.all,
      ),
    ).toEqual({ ok: true });

    // The stored alert is signed, verifies, and has both summaries.
    const stored = await s.alertStore.get(alertId);
    expect(stored?.event).toBe('Flash Flood Warning');
    expect(stored?.signature).toBeTruthy();
    expect(stored && verifyAlertSignature(stored, alertKeys.all)).toEqual({ ok: true });
    expect(stored?.summaries.map((x) => x.language).sort()).toEqual(['en', 'es']);

    // Polling the same feed again plans and sends nothing more.
    await s.poll();
    await s.drain();
    expect(s.sender.sent).toHaveLength(1);
  });

  it('retries a fan-out that fails halfway: every device still gets exactly one push', async () => {
    // 12 devices = two queue batches of 10 and 2. The second enqueue call throws.
    const s = setup({ queue: new FlakyQueue(2) });
    const ids = Array.from({ length: 12 }, (_, i) => `d${i}`);
    for (const id of ids) await s.addDevice(id, INSIDE.lat, INSIDE.lng);

    const first = await s.poll();
    expect(first.errors).toBe(1); // counted, and the alert is not marked seen
    expect(s.queue.jobs).toHaveLength(10); // the first batch made it onto the queue

    const second = await s.poll(); // re-plans all 12; the first 10 are queued twice
    expect(second.errors).toBe(0);
    expect(s.queue.jobs).toHaveLength(22);

    expect(await s.drain()).toEqual([]);
    expect(s.sender.sent).toHaveLength(12);
    for (const id of ids) expect(s.pushesTo(id)).toHaveLength(1);
  });

  it('retries a retryable push failure: the SQS record is redelivered and sends once', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    await s.poll();

    s.sender.failTokens.set('ExponentPushToken[inside]', 'MessageRateExceeded');
    const failed = await s.drain();
    expect(failed).toHaveLength(1); // record goes back to the queue
    expect(s.sender.sent).toHaveLength(0);

    s.sender.failTokens.clear();
    expect(await s.drain()).toEqual([]); // the same record, retried
    expect(s.sender.sent).toHaveLength(1);

    expect(await s.drain()).toEqual([]); // nothing left to redeliver
    expect(s.sender.sent).toHaveLength(1);
  });

  it('DeviceNotRegistered clears the token and is not retried', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    await s.poll();

    s.sender.failTokens.set('ExponentPushToken[inside]', 'DeviceNotRegistered');
    expect(await s.drain()).toEqual([]); // acknowledged, so SQS does not retry
    expect((await s.devices.get('inside'))?.expoPushToken).toBeNull();

    s.sender.failTokens.clear();
    expect(await s.drain()).toEqual([]);
    expect(s.sender.sent).toHaveLength(0);
  });

  describe('NWS updates', () => {
    it('does not re-notify for a same-severity update', async () => {
      const s = setup({ feed: chainFeed(0) });
      await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
      await s.poll();
      await s.drain();
      expect(s.sender.sent).toHaveLength(1);

      s.feed.body = chainFeed(1); // Update, still a Flood Warning
      const counts = await s.poll();
      expect(counts.applied).toBe(1);
      expect(s.queue.jobs).toHaveLength(2); // planned again...
      expect(await s.drain()).toEqual([]);
      expect(s.sender.sent).toHaveLength(1); // ...but the worker drops the duplicate
    });

    it('re-notifies when the update raises severity (Watch -> Warning)', async () => {
      const s = setup({ feed: chainFeed(0, { event: 'Flood Watch' }) });
      await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
      await s.poll();
      await s.drain();
      expect(s.sender.sent).toHaveLength(1);
      expect(s.sender.sent[0]?.title).toContain('Watch');

      s.feed.body = chainFeed(1); // Update to Flood Warning
      await s.poll();
      await s.drain();
      expect(s.sender.sent).toHaveLength(2);
      expect(s.sender.sent[1]?.title).toContain('Warning');
      // Same alert (the update keeps the Relay id), so the same alertId in both pushes.
      expect(s.sender.sent[1]?.data['alertId']).toBe(s.sender.sent[0]?.data['alertId']);
    });
  });

  it('sends nothing while the kill switch is paused, then sends once after unpausing', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    await s.poll();

    s.kill.workerPaused = true;
    expect(await s.drain()).toHaveLength(1); // record left for retry
    expect(await s.drain()).toHaveLength(1);
    expect(s.sender.sent).toHaveLength(0);

    s.kill.workerPaused = false;
    expect(await s.drain()).toEqual([]);
    expect(s.sender.sent).toHaveLength(1);
    expect(await s.drain()).toEqual([]);
    expect(s.sender.sent).toHaveLength(1);
  });

  it('an alert that arrives while paused is planned after unpausing (not lost)', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);

    s.kill.ingestPaused = true;
    const paused = await s.poll();
    expect(s.queue.jobs).toHaveLength(0);
    expect(paused.errors).toBe(0); // paused is expected, not an error
    await s.poll(); // still paused: planned again, still nothing
    expect(s.queue.jobs).toHaveLength(0);

    s.kill.ingestPaused = false;
    await s.poll();
    expect(s.queue.jobs).toHaveLength(1);
    expect(await s.drain()).toEqual([]);
    expect(s.sender.sent).toHaveLength(1);
  });

  it('plans nothing when the kill switch is already on at ingest time', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    await s.poll();
    const stored = await s.alertStore.get(s.queue.jobs[0]?.alertId ?? '');
    if (!stored) throw new Error('not stored');

    const queue = new MemoryDeliveryQueue();
    const plan = await planDelivery(stored, {
      places: new MemoryPlaceStore(),
      devices: s.devices,
      queue,
      tokenKey: tokenKeys.current,
      verifyAlert: createAlertVerifier(alertKeys.all),
      isDeliveryPaused: async () => true,
      clock,
      logger: noopLogger,
    });
    expect(plan.refused).toBe('paused');
    expect(queue.jobs).toHaveLength(0);
  });

  it('refuses to send an alert that was edited after signing', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    await s.poll();
    expect(s.queue.jobs).toHaveLength(1); // planned while the alert was still intact

    // Someone edits the stored row without re-signing.
    const id = s.queue.jobs[0]?.alertId ?? '';
    const stored = await s.alertStore.get(id);
    if (!stored) throw new Error('not stored');
    const edited: Alert = {
      ...stored,
      official: { ...stored.official, headline: 'Free gift cards at example.test' },
    };
    await s.alertStore.save(edited);

    expect(await s.drain()).toEqual([]); // refused alerts are dropped, not retried forever
    expect(s.sender.sent).toHaveLength(0);
  });

  it('refuses an alert saved straight to the inner store (unsigned)', async () => {
    const s = setup();
    await s.addDevice('inside', INSIDE.lat, INSIDE.lng);
    const feature = (JSON.parse(mixed) as { features: unknown[] }).features[0];
    const norm = normalizeFeature(feature, { newId: randomUUID });
    if (!norm.ok) throw new Error(norm.reason);
    norm.alert.area.h3Cells = [cellForPoint(INSIDE.lat, INSIDE.lng)];
    await s.alertStore.save(norm.alert); // bypasses the SigningAlertStore

    const queue = new MemoryDeliveryQueue();
    const plan = await planDelivery(norm.alert, {
      places: new MemoryPlaceStore(),
      devices: s.devices,
      queue,
      tokenKey: tokenKeys.current,
      verifyAlert: createAlertVerifier(alertKeys.all),
      isDeliveryPaused: async () => false,
      clock,
      logger: noopLogger,
    });
    expect(plan.refused).toBe('unsigned');
    expect(queue.jobs).toHaveLength(0);
  });

  it('sends a Spanish body to a Spanish-language device and English to an English one', async () => {
    const s = setup();
    await s.addDevice('en-device', INSIDE.lat, INSIDE.lng, 'en');
    await s.addDevice('es-device', INSIDE.lat, INSIDE.lng, 'es');
    await s.poll();
    expect(await s.drain()).toEqual([]);

    const es = s.pushesTo('es-device');
    const en = s.pushesTo('en-device');
    expect(es).toHaveLength(1);
    expect(en).toHaveLength(1);

    const stored = await s.alertStore.get(String(es[0]?.data['alertId']));
    const spanish = stored?.summaries.find((x) => x.language === 'es')?.text.trim();
    expect(spanish).toBeTruthy();
    expect(es[0]?.body).toBe(spanish);
    expect(es[0]?.title).toMatch(/^Aviso:/);
    expect(en[0]?.body).not.toBe(spanish);
    expect(en[0]?.title).toMatch(/^Warning:/);
  });
});
