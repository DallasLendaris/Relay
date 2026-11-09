/**
 * The whole scale path in memory, wired the way production is:
 *
 *   ingest (FANOUT_MODE=queue) -> plan queue -> planShards -> shard dispatcher -> processShard
 *   -> send queue (wire format) -> parseBatches -> processBatch (fake APNs/FCM/Expo)
 *   -> a recipient's v2 check-in through the stateless API route -> check-ins table put
 *   -> stream record -> aggregator -> per-cell total.
 */
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import type { DynamoDBStreamEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { DynamoCheckInStore } from '../../src/adapters/aws/dynamo-checkin-store.js';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { createApi } from '../../src/api/index.js';
import { noopLogger } from '../../src/delivery/index.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import { createAggregator } from '../../src/scale/checkins/aggregator.js';
import type {
  CellIndexEntry,
  NativePushMessage,
  NativePushResult,
  NativePushSender,
  PushTarget,
} from '../../src/scale/contracts.js';
import { makeCheckInTokenFor } from '../../src/scale/fanout/handlers.js';
import { fnv1a32 } from '../../src/scale/fanout/hash.js';
import { planShards } from '../../src/scale/fanout/planner.js';
import {
  MemoryPlanQueue,
  MemorySendQueue,
  MemoryShardDispatcher,
} from '../../src/scale/fanout/queues.js';
import { processShard } from '../../src/scale/fanout/shard-worker.js';
import { parseBatches } from '../../src/scale/sender/consumer.js';
import { createDeadTargetClearer } from '../../src/scale/sender/dead-target.js';
import { processBatch, type BatchOutcome } from '../../src/scale/sender/sender.js';
import { createAlertVerifier } from '../../src/signing/index.js';
import { createDeviceCredential } from '../../src/tokens/index.js';

const feed = JSON.parse(
  readFileSync(new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
) as { features: unknown[] };

const ring = (id: string, secret: string) => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});
const ALERT_KEYS = ring('a1', 'alert-secret');
const TOKEN_KEYS = ring('t1', 'token-secret');
const CRED_KEY = { id: 'c1', secret: 'credential-secret' };
const SEND_NOW = new Date('2026-11-09T15:00:00Z');
const CHECKIN_NOW = new Date('2026-11-09T15:05:00Z');
const clock = { now: () => SEND_NOW };

const TOTAL_ENTRIES = 5_000;
const SECOND_PLACES = 200; // devices that also saved a place in another cell
const NO_TARGET_EVERY = 33; // about 3% of devices have no push target yet

const deviceIdOf = (i: number) => `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`;

function targetFor(i: number): PushTarget | null {
  if (i % NO_TARGET_EVERY === 0) return null;
  const kind = i % 3;
  if (kind === 0) return { platform: 'ios', token: i.toString(16).padStart(64, '0') };
  if (kind === 1) return { platform: 'android', token: `fcm-token-${i}`.padEnd(120, 'f') };
  return { platform: 'expo', token: `ExponentPushToken[dev-${i}]` };
}

/** Which devices the fake push service reports as dead (about 0.5%), by target token. */
const isDead = (token: string) => fnv1a32(`dead:${token}`) % 1000 < 5;
/** Which sends fail once with a retryable error (about 1%), by check-in token. */
const failsOnce = (checkInToken: string) => fnv1a32(`flaky:${checkInToken}`) % 1000 < 10;

/** The fake APNs/FCM/Expo: records every outcome per (device, cell) read from the v2 token. */
class FakePushService implements NativePushSender {
  readonly ok = new Map<string, number>();
  readonly deadSeen = new Set<string>();
  readonly attempts = new Map<string, number>();
  calls = 0;
  retryable = 0;
  permanent = 0;
  firstOk: { deviceId: string; cell: string; token: string } | undefined;

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    return messages.map((m): NativePushResult => {
      this.calls++;
      const token = String(m.data['checkInToken']);
      const payload = JSON.parse(
        Buffer.from(token.split('.')[0] as string, 'base64url').toString('utf8'),
      ) as { d: string; c: string };
      const key = `${payload.d}|${payload.c}`;
      const attempt = (this.attempts.get(key) ?? 0) + 1;
      this.attempts.set(key, attempt);
      if (isDead(m.target.token)) {
        this.permanent++;
        this.deadSeen.add(payload.d);
        return { ok: false, kind: 'permanent', error: 'Unregistered' };
      }
      if (attempt === 1 && failsOnce(token)) {
        this.retryable++;
        return { ok: false, kind: 'retryable', error: 'Unavailable' };
      }
      this.ok.set(key, (this.ok.get(key) ?? 0) + 1);
      this.firstOk ??= { deviceId: payload.d, cell: payload.c, token };
      return { ok: true };
    });
  }
}

/** The aggregator's totals table: transactions with ADD and conditional marker puts. */
class FakeTotalsTable {
  readonly items = new Map<string, Record<string, unknown>>();
  async send(cmd: unknown): Promise<object> {
    if (!(cmd instanceof TransactWriteCommand)) throw new Error('unexpected command');
    const tx = cmd.input.TransactItems ?? [];
    const reasons = tx.map((t) =>
      t.Put && this.items.has(String((t.Put.Item as { pk: string }).pk))
        ? { Code: 'ConditionalCheckFailed' }
        : { Code: 'None' },
    );
    if (reasons.some((r) => r.Code !== 'None')) {
      throw new TransactionCanceledException({
        message: 'cancelled',
        $metadata: {},
        CancellationReasons: reasons,
      });
    }
    for (const t of tx) {
      if (t.Put) this.items.set(String((t.Put.Item as { pk: string }).pk), { ...t.Put.Item });
      if (t.Update) {
        const pk = String((t.Update.Key as { pk: string }).pk);
        const v = t.Update.ExpressionAttributeValues as Record<string, unknown>;
        const row = this.items.get(pk) ?? { pk };
        row['checkedIn'] = Number(row['checkedIn'] ?? 0) + Number(v[':n']);
        row['alertId'] = v[':a'];
        row['h3Cell'] = v[':c'];
        this.items.set(pk, row);
      }
    }
    return {};
  }
}

/** Fails the test if the stateless check-in route touches anything but the check-ins table. */
function forbidden<T extends object>(name: string): T {
  return new Proxy({} as T, {
    get: (_t, prop) => () => {
      throw new Error(`${name}.${String(prop)} must not be called on the stateless path`);
    },
  });
}

describe('statewide scale path, in memory', () => {
  it('5,000 index entries: ingest -> fan-out -> sender -> check-in -> aggregator', async () => {
    // ---------- 1. Ingest with FANOUT_MODE=queue ----------
    const alertStore = new InMemoryAlertStore();
    const planQueue = new MemoryPlanQueue();
    const directQueue = new MemoryDeliveryQueue();
    const engine = createIngestPipeline({
      alertStore,
      places: new MemoryPlaceStore(),
      devices: new MemoryDeviceStore(),
      queue: directQueue,
      alertKeys: ALERT_KEYS,
      tokenKeys: TOKEN_KEYS,
      isDeliveryPaused: async () => false,
      fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
      userAgent: 'relay-test',
      clock,
      logger: noopLogger,
      env: { FANOUT_MODE: 'queue', PLAN_QUEUE_URL: 'unused-in-memory' },
      planQueue,
    });
    const normalized = engine.normalize(feed.features[0]);
    if (!normalized.ok) throw new Error(normalized.reason);
    normalized.alert.area.h3Cells = await engine.cellsFor(normalized.alert.area);
    const applied = await engine.apply(normalized.alert);
    await engine.onApplied?.(applied);
    const alert = applied.alert;
    expect(planQueue.messages).toHaveLength(1);
    expect(directQueue.jobs).toHaveLength(0); // queue mode never plans in-line
    const alertCells = (await alertStore.get(alert.id))?.area.h3Cells ?? [];
    expect(alertCells.length).toBeGreaterThanOrEqual(2);
    const cells = alertCells.slice(0, 60);

    // ---------- 2. Seed 5,000 cell-index entries (ios, android, expo) ----------
    const cellIndex = new MemoryCellIndex();
    const devices = new MemoryDeviceStore();
    const places = new MemoryPlaceStore();
    const entries: CellIndexEntry[] = [];
    const expected = new Map<string, PushTarget>(); // `${deviceId}|${cell}` -> target to receive a push
    const targetOfDevice = new Map<string, PushTarget | null>();
    const deviceCount = TOTAL_ENTRIES - SECOND_PLACES;
    for (let i = 0; i < deviceCount; i++) {
      const deviceId = deviceIdOf(i);
      const target = targetFor(i);
      targetOfDevice.set(deviceId, target);
      await devices.create({
        id: deviceId,
        credentialHash: 'x',
        expoPushToken: target?.platform === 'expo' ? target.token : null,
        pushPlatform: target?.platform ?? null,
        pushToken: target?.token ?? null,
        language: i % 5 === 0 ? 'es' : 'en',
        createdAt: '2026-11-01T00:00:00Z',
      });
      const placeCells = [cells[i % cells.length] as string];
      if (i < SECOND_PLACES) placeCells.push(cells[(i + 7) % cells.length] as string);
      // Every entry carries all of the device's cells; only the smallest one sends (the whole
      // alert area is covered here).
      const deviceCells = [...new Set(placeCells)].sort();
      for (const [n, h3Cell] of placeCells.entries()) {
        const placeId = `p${n}-${i}`;
        await places.add({
          id: placeId,
          deviceId,
          label: 'x',
          shortAddress: 'x',
          h3Cell,
          createdAt: '2026-11-01T00:00:00Z',
        });
        entries.push({
          h3Cell,
          deviceId,
          placeId,
          target,
          language: i % 5 === 0 ? 'es' : 'en',
          deviceCells,
        });
        if (target && h3Cell === deviceCells[0]) expected.set(`${deviceId}|${h3Cell}`, target);
      }
    }
    expect(entries).toHaveLength(TOTAL_ENTRIES);
    await cellIndex.put(entries);
    const platforms = new Set([...expected.values()].map((t) => t.platform));
    expect(platforms).toEqual(new Set(['ios', 'android', 'expo']));

    // ---------- 3. The flow: plan -> shards -> send queue -> sender ----------
    const push = new FakePushService();
    const sendQueue = new MemorySendQueue();
    const idempotency = new MemoryIdempotencyStore(clock);
    const verifyAlert = createAlertVerifier(ALERT_KEYS.all);
    const outcomes: BatchOutcome[] = [];
    const senderDeps = {
      idempotency,
      alerts: alertStore,
      verifyAlert,
      isDeliveryPaused: async () => false,
      push,
      sendQueue,
      onDeadTarget: createDeadTargetClearer({ devices, places, cellIndex }),
      clock,
      logger: noopLogger,
      sleep: async () => {},
    };

    async function runFlow(): Promise<{ batches: number; messages: number }> {
      const ready = planQueue.messages.shift();
      if (!ready) throw new Error('no AlertReady on the plan queue');
      const shardDispatcher = new MemoryShardDispatcher();
      const common = { alerts: alertStore, verifyAlert, isDeliveryPaused: async () => false };
      const planned = await planShards(ready, {
        ...common,
        clock,
        logger: noopLogger,
        shardDispatcher,
      });
      expect(planned.refused).toBeUndefined();
      for (const job of shardDispatcher.jobs) {
        const r = await processShard(job, {
          ...common,
          clock,
          logger: noopLogger,
          cellIndex,
          sendQueue,
          makeCheckInToken: makeCheckInTokenFor(TOKEN_KEYS.current),
        });
        expect(r.refused).toBeUndefined();
      }
      // The sender drains both queues; retries land back on the same queues.
      let messages = 0;
      let batches = 0;
      for (;;) {
        const bodies = [...sendQueue.messages.urgent, ...sendQueue.messages.standard];
        if (bodies.length === 0) break;
        sendQueue.messages.urgent.length = 0;
        sendQueue.messages.standard.length = 0;
        for (const body of bodies) {
          messages++;
          const parsed = parseBatches(body);
          if (!parsed) throw new Error('the sender cannot parse a fan-out message');
          const done = await Promise.all(parsed.map((b) => processBatch(b, senderDeps)));
          batches += done.length;
          outcomes.push(...done);
        }
      }
      return { batches, messages };
    }

    const first = await runFlow();
    expect(first.batches).toBeGreaterThan(0);
    expect(outcomes.every((o) => o.ack)).toBe(true);

    // ---------- 4. Verify pushes ----------
    // The fake service really injected both failure kinds.
    expect(push.retryable).toBeGreaterThan(0);
    expect(push.permanent).toBeGreaterThan(0);
    expect(outcomes.reduce((n, o) => n + o.retried, 0)).toBe(push.retryable);
    expect(outcomes.some((o) => o.gaveUp > 0)).toBe(false);

    // Every device with a target that is not dead got exactly one successful push, in its primary cell.
    let live = 0;
    for (const [key, target] of expected) {
      const got = push.ok.get(key) ?? 0;
      if (isDead(target.token)) expect(got, key).toBe(0);
      else {
        expect(got, key).toBe(1);
        live++;
      }
    }
    expect(live).toBeGreaterThan(expected.size * 0.97);
    // Nobody got a push they should not have: no target, or an unknown device/cell pair.
    for (const key of push.ok.keys()) expect(expected.has(key), key).toBe(true);
    // Devices with a second place in another cell were pushed ONCE, from the smaller cell, and
    // never in the other: zero cross-cell duplicates.
    const [lo, hi] = [cells[1] as string, cells[8] as string].sort() as [string, string];
    expect([...expected.keys()].filter((k) => k.startsWith(deviceIdOf(1)))).toEqual([
      `${deviceIdOf(1)}|${lo}`,
    ]);
    expect(push.ok.get(`${deviceIdOf(1)}|${lo}`)).toBe(1);
    expect(push.ok.get(`${deviceIdOf(1)}|${hi}`)).toBeUndefined();
    const perDevice = new Map<string, number>();
    for (const [key, n] of push.ok) {
      const id = key.split('|')[0] as string;
      perDevice.set(id, (perDevice.get(id) ?? 0) + n);
    }
    for (const [id, n] of perDevice) expect(n, id).toBe(1);

    // ---------- 5. Dead targets are cleared, nothing else is ----------
    const dead = [...targetOfDevice].filter(([, t]) => t && isDead(t.token)).map(([id]) => id);
    expect(dead.length).toBeGreaterThan(0);
    expect(new Set(dead)).toEqual(push.deadSeen);
    const deadSet = new Set(dead);
    let checkedEntries = 0;
    for (const cell of cells) {
      let cursor: string | undefined;
      do {
        const page = await cellIndex.listForCell(cell, cursor);
        cursor = page.cursor;
        for (const e of page.entries) {
          checkedEntries++;
          if (deadSet.has(e.deviceId)) expect(e.target, e.deviceId).toBeNull();
          else expect(e.target, e.deviceId).toEqual(targetOfDevice.get(e.deviceId));
        }
      } while (cursor);
    }
    expect(checkedEntries).toBe(TOTAL_ENTRIES);
    for (const id of dead) {
      expect(await devices.get(id), id).toMatchObject({ pushPlatform: null, pushToken: null });
      expect((await devices.get(id))?.expoPushToken).toBeNull();
    }

    // ---------- 6. Re-plan: the same alert again sends nothing new ----------
    await engine.onApplied?.(applied); // ingest sees it again and publishes another AlertReady
    expect(planQueue.messages).toHaveLength(1);
    const callsBefore = push.calls;
    const okBefore = new Map(push.ok);
    const outcomesBefore = outcomes.length;
    const second = await runFlow();
    expect(second.batches).toBeGreaterThan(0); // batches were planned again...
    expect(push.calls).toBe(callsBefore); // ...but not one push was made
    expect(push.ok).toEqual(okBefore);
    const replay = outcomes.slice(outcomesBefore);
    expect(replay.every((o) => o.ack && o.reason === 'duplicate' && o.sent === 0)).toBe(true);

    // ---------- 7. One recipient checks in: stateless API route -> aggregator ----------
    const sample = push.firstOk;
    if (!sample) throw new Error('no successful push to check in from');
    const puts: Array<Record<string, unknown>> = [];
    const checkinsDoc = {
      send: async (cmd: unknown) => {
        if (!(cmd instanceof PutCommand)) throw new Error('unexpected command');
        puts.push(cmd.input.Item as Record<string, unknown>);
        return {};
      },
    };
    const route = createApi({
      devices: forbidden('devices'),
      places: forbidden('places'),
      alerts: forbidden('alerts'),
      geocoder: forbidden('geocoder'),
      cellIndex: forbidden('cellIndex'),
      checkins: new DynamoCheckInStore(checkinsDoc as never, 'checkins'),
      tokenKeys: TOKEN_KEYS.all,
      deviceCredentialKeys: new Map([[CRED_KEY.id, CRED_KEY.secret]]),
      clock: { now: () => CHECKIN_NOW },
      newId: randomUUID,
    });
    const res = await route({
      rawPath: `/alerts/${alert.id}/checkins`,
      headers: { authorization: `Bearer ${createDeviceCredential(sample.deviceId, CRED_KEY)}` },
      body: JSON.stringify({ status: 'ok', token: sample.token, idempotencyKey: 'e2e-1' }),
      requestContext: { http: { method: 'POST' } },
    });
    expect(res.statusCode).toBe(200);
    expect(puts).toHaveLength(1);
    expect(puts[0]).toMatchObject({
      pk: `${alert.id}#${sample.deviceId}`,
      alertId: alert.id,
      h3Cell: sample.cell, // the cell came from the token the fan-out signed
    });

    const totals = new FakeTotalsTable();
    const aggregate = createAggregator({ doc: totals as never, totalsTable: 'totals' });
    const stream = {
      Records: [
        {
          eventID: 'evt-1',
          eventName: 'INSERT',
          eventSource: 'aws:dynamodb',
          dynamodb: {
            SequenceNumber: '1001',
            NewImage: marshall(puts[0] as Record<string, unknown>),
          },
        },
      ],
    } as unknown as DynamoDBStreamEvent;
    expect(await aggregate(stream)).toEqual({ batchItemFailures: [] });
    expect(totals.items.get(`${alert.id}#${sample.cell}`)).toMatchObject({
      alertId: alert.id,
      h3Cell: sample.cell,
      checkedIn: 1,
    });
    // Delivered again by Streams: the marker keeps the total at 1.
    expect(await aggregate(stream)).toEqual({ batchItemFailures: [] });
    expect(totals.items.get(`${alert.id}#${sample.cell}`)?.['checkedIn']).toBe(1);
  }, 60_000);
});
