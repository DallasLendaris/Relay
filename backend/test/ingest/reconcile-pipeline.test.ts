/**
 * Feed reconciliation through the real ingest pipeline (SAFE-12 follow-ups):
 *  - expiredReferences are read from each feature's parameters (NWS shape), from a fixture
 *  - an alert ended for feed absence that comes back is restored, republished, planned again, and
 *    the fan-out neither double-pushes nor misses a device
 */
import { readFileSync } from 'node:fs';
import { describe, expect, it, beforeEach } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { InMemorySeenStore } from '../../src/adapters/memory/seen-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import type { NwsClient, NwsFetchResult } from '../../src/ingest/nws-client.js';
import { clearReconcileState, expiredReferencesOf, pollOnce } from '../../src/ingest/poll.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import type {
  CellIndexEntry,
  NativePushMessage,
  NativePushResult,
  NativePushSender,
} from '../../src/scale/contracts.js';
import { makeCheckInTokenFor } from '../../src/scale/fanout/handlers.js';
import { planShards } from '../../src/scale/fanout/planner.js';
import {
  MemoryPlanQueue,
  MemorySendQueue,
  MemoryShardDispatcher,
} from '../../src/scale/fanout/queues.js';
import { processShard } from '../../src/scale/fanout/shard-worker.js';
import { parseBatches } from '../../src/scale/sender/consumer.js';
import { processBatch } from '../../src/scale/sender/sender.js';
import { createAlertVerifier } from '../../src/signing/index.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

const fixture = (name: string): { features: unknown[] } =>
  JSON.parse(readFileSync(new URL(`../fixtures/nws/${name}`, import.meta.url), 'utf8')) as {
    features: unknown[];
  };
const expiredFixture = readFileSync(
  new URL('../fixtures/nws/expired-references.json', import.meta.url),
  'utf8',
);
const mixed = fixture('active-ca-mixed.json');

const MIN = 60_000;
const ring = (id: string, secret: string) => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});
const TOKEN_KEYS = ring('t1', 'token-secret');

/** A client that serves one body per poll (the last one repeats). */
function clientFor(bodies: Array<() => string>): NwsClient & { commits: number } {
  let i = 0;
  return {
    commits: 0,
    commit() {
      this.commits += 1;
    },
    async fetchActiveCalifornia(): Promise<NwsFetchResult> {
      const body = (bodies[Math.min(i++, bodies.length - 1)] as () => string)();
      return { status: 'ok', body, fetchedAt: new Date() };
    },
  };
}

function harness(opts: { fanout?: 'queue' | 'inline' } = {}) {
  let nowMs = Date.parse('2026-11-09T15:00:00Z');
  const alertStore = new InMemoryAlertStore();
  const planQueue = new MemoryPlanQueue();
  const seen = new InMemorySeenStore();
  const engine = createIngestPipeline({
    alertStore,
    places: new MemoryPlaceStore(),
    devices: new MemoryDeviceStore(),
    queue: new MemoryDeliveryQueue(),
    alertSigning: TEST_SIGNING,
    tokenKeys: TOKEN_KEYS,
    isDeliveryPaused: async () => false,
    fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
    userAgent: 'relay-test',
    clock: { now: () => new Date(nowMs) },
    logger: noopLogger,
    env: { FANOUT_MODE: opts.fanout ?? 'queue', PLAN_QUEUE_URL: 'unused-in-memory' },
    planQueue,
  });
  const run = async (client: NwsClient) =>
    pollOnce({
      ...engine,
      client,
      archive: new MemoryRawArchive(),
      seen,
      log: (e) => {
        if (process.env.DBG) console.log(JSON.stringify(e));
      },
    });
  return {
    alertStore,
    planQueue,
    seen,
    engine,
    run,
    now: () => new Date(nowMs),
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

beforeEach(() => clearReconcileState());

describe('expiredReferences (per feature, parameters.expiredReferences)', () => {
  it('parses "<sender>,<identifier>,<sent>" from each feature of a fixture', () => {
    expect([...expiredReferencesOf(expiredFixture)].sort()).toEqual([
      'urn:oid:2.49.0.1.840.0.dd0001.001.1',
      'urn:oid:2.49.0.1.840.0.ee0001.001.1',
    ]);
    // a feed without them, and garbage, give nothing
    expect(expiredReferencesOf(JSON.stringify(mixed)).size).toBe(0);
    expect(
      expiredReferencesOf('{"features":[{"properties":{"parameters":{"expiredReferences":7}}}]}')
        .size,
    ).toBe(0);
  });

  it('cancels the stored alert it names, and not the one an Update replaces', async () => {
    const h = harness({ fanout: 'inline' });
    const doc = JSON.parse(expiredFixture) as { features: Array<Record<string, any>> };
    const [dead, , update] = doc.features as [Record<string, any>, unknown, Record<string, any>];
    // First poll: the first message of two chains (dd..1 and ee..1) are stored.
    const first = structuredClone(update);
    first['properties'].messageType = 'Alert';
    first['properties'].references = [];
    first['properties'].parameters = {};
    first['properties'].sent = '2026-11-09T06:10:00-08:00';
    first['id'] =
      first['properties'].id =
      first['properties']['@id'] =
        'https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.ee0001.001.1';
    const deadFirst = structuredClone(dead);
    await h.run(
      clientFor([
        () => JSON.stringify({ type: 'FeatureCollection', features: [first, deadFirst] }),
      ]),
    );
    const bySource = async (id: string) =>
      h.alertStore.findBySourceId(`urn:oid:2.49.0.1.840.0.${id}`);
    expect((await bySource('dd0001.001.1'))?.state).toBe('active');
    expect((await bySource('ee0001.001.1'))?.state).toBe('active');

    // Second poll: the fixture. dd..1 is named in expiredReferences; ee..1 is replaced by an
    // Update that lists it as expired.
    await h.run(clientFor([() => expiredFixture]));
    expect((await bySource('dd0001.001.1'))?.state).toBe('cancelled');
    const ee = await bySource('ee0001.001.2');
    expect(ee?.state).toBe('updated');
    expect(ee?.signature).not.toBeNull();
  });
});

/** The fake push service: counts successful pushes per (device, cell) read from the v2 token. */
class FakePush implements NativePushSender {
  readonly ok = new Map<string, number>();
  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    return messages.map((m) => {
      const token = String(m.data['checkInToken']);
      const p = JSON.parse(Buffer.from(token.split('.')[0] as string, 'base64url').toString()) as {
        d: string;
        c: string;
      };
      this.ok.set(p.d, (this.ok.get(p.d) ?? 0) + 1);
      return { ok: true };
    });
  }
}

describe('restore: no duplicate push and no missed push', () => {
  it('devices already notified are not pushed again; the rest are pushed once', async () => {
    const h = harness();
    const feature0 = mixed.features[0];
    const withAlert = () => JSON.stringify({ type: 'FeatureCollection', features: [feature0] });
    const without = () => JSON.stringify({ type: 'FeatureCollection', features: [] });
    const clientSteps: Array<() => string> = [withAlert];
    const client = clientFor(clientSteps);

    // 1. The alert arrives; ingest publishes AlertReady.
    await h.run(client);
    const first = h.planQueue.messages[0];
    if (!first) throw new Error('no AlertReady');
    const alert = (await h.alertStore.findBySourceId(first.sourceId)) as Alert;
    expect(alert.state).toBe('active');
    const cells = alert.area.h3Cells.slice(0, 4);

    // 2. 40 devices over those cells.
    const cellIndex = new MemoryCellIndex();
    const entries: CellIndexEntry[] = [];
    for (let i = 0; i < 40; i++) {
      const h3Cell = cells[i % cells.length] as string;
      entries.push({
        h3Cell,
        deviceId: `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`,
        placeId: `p${i}`,
        target: { platform: 'android', token: `fcm-token-${i}`.padEnd(120, 'f') },
        language: 'en',
        deviceCells: [h3Cell],
      });
    }
    await cellIndex.put(entries);

    const push = new FakePush();
    const sendQueue = new MemorySendQueue();
    const clock = { now: () => h.now() };
    const verifyAlert = createAlertVerifier(TEST_SIGNING.publicKeys);
    const senderDeps = {
      idempotency: new MemoryIdempotencyStore(clock),
      alerts: h.alertStore,
      verifyAlert,
      isDeliveryPaused: async () => false,
      push,
      sendQueue,
      onDeadTarget: async () => {},
      clock,
      logger: noopLogger,
      sleep: async () => {},
    };
    const plan = async (): Promise<void> => {
      const ready = h.planQueue.messages.shift();
      if (!ready) throw new Error('no AlertReady on the plan queue');
      const shardDispatcher = new MemoryShardDispatcher();
      const common = { alerts: h.alertStore, verifyAlert, isDeliveryPaused: async () => false };
      await planShards(ready, { ...common, clock, logger: noopLogger, shardDispatcher });
      for (const job of shardDispatcher.jobs) {
        await processShard(job, {
          ...common,
          clock,
          logger: noopLogger,
          cellIndex,
          sendQueue,
          makeCheckInToken: makeCheckInTokenFor(TOKEN_KEYS.current),
        });
      }
    };
    const drain = async (limit = Infinity): Promise<number> => {
      const bodies = [...sendQueue.messages.urgent, ...sendQueue.messages.standard];
      sendQueue.messages.urgent.length = 0;
      sendQueue.messages.standard.length = 0;
      const todo = bodies.slice(0, limit);
      for (const body of bodies.slice(limit)) sendQueue.messages.standard.push(body); // stays queued
      for (const body of todo) {
        for (const b of parseBatches(body) ?? []) await processBatch(b, senderDeps);
      }
      return todo.length;
    };

    // 3. Plan; the sender gets through only part of the queue before the alert is ended.
    await plan();
    const queued = sendQueue.messages.urgent.length + sendQueue.messages.standard.length;
    expect(queued).toBeGreaterThan(3);
    await drain(Math.floor(queued / 2));
    const notifiedBefore = new Set(push.ok.keys());
    expect(notifiedBefore.size).toBeGreaterThan(0);
    expect(notifiedBefore.size).toBeLessThan(entries.length);

    // 4. NWS edge cache flaps: the alert is missing for long enough to be ended.
    clientSteps.splice(0, clientSteps.length, without);
    for (let n = 0; n < 11; n++) {
      h.advance(30_000);
      await h.run(client);
    }
    expect((await h.alertStore.get(alert.id))?.state).toBe('cancelled');
    expect((await h.seen.getEnded(alert.id))?.endedBy).toBe('feed_absence');
    // while it is ended, nothing more goes out for it
    const pushedWhileEnded = new Map(push.ok);
    await drain();
    expect(push.ok).toEqual(pushedWhileEnded);

    // 5. It comes back (the seen store already has its sourceId, so only restoration can help).
    clientSteps.splice(0, clientSteps.length, withAlert);
    h.advance(30_000);
    await h.run(client);
    const restored = await h.alertStore.get(alert.id);
    expect(restored?.state).toBe('active');
    expect(restored?.signature).not.toBeNull();
    expect(h.planQueue.messages).toHaveLength(1); // planned again, same message as the first time
    expect(h.planQueue.messages[0]).toEqual(first);
    expect(await h.seen.getEnded(alert.id)).toBeNull();

    await plan();
    await drain();

    // Every device was pushed exactly once: the already-notified ones were skipped (same batch
    // keys), the rest were sent now.
    expect(push.ok.size).toBe(entries.length);
    for (const [device, n] of push.ok) expect(n, device).toBe(1);
    for (const device of notifiedBefore) expect(push.ok.get(device)).toBe(1);

    // And it stays active on later reads.
    h.advance(30 * MIN);
    await h.run(client);
    expect((await h.alertStore.get(alert.id))?.state).toBe('active');
  });
});
