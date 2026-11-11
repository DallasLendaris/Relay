/**
 * Cancellation and update follow-ups through the whole scale path in memory (docs/cancellation.md):
 *
 *   ingest (FANOUT_MODE=queue) -> plan queue -> planShards / planFollowUp -> shard dispatcher
 *   -> processShard / processFollowUpShard -> send queue (wire format) -> processBatch -> fake push
 *
 * Alert -> fan-out (part of it held back, still queued) -> CAP Cancel -> follow-up: every device that
 * was sent the alert gets exactly one quiet follow-up; devices that were not (no target, joined the
 * cell index after the fan-out, or in a batch still queued when the Cancel landed) get nothing, and
 * the held-back original batches are never sent after the Cancel.
 */
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { FOLLOW_UP_DELAY_SECONDS } from '../../src/delivery/follow-up.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { MemoryCellIndex } from '../../src/scale/cell-index/index.js';
import type {
  AlertReady,
  CellIndexEntry,
  NativePushMessage,
  NativePushResult,
  NativePushSender,
  SendBatch,
  SendQueue,
  SendQueueName,
} from '../../src/scale/contracts.js';
import { planFollowUp, processFollowUpShard } from '../../src/scale/fanout/follow-up.js';
import { makeCheckInTokenFor } from '../../src/scale/fanout/handlers.js';
import { planShards } from '../../src/scale/fanout/planner.js';
import { MemoryPlanQueue, MemoryShardDispatcher } from '../../src/scale/fanout/queues.js';
import { processShard } from '../../src/scale/fanout/shard-worker.js';
import { packBatches, serializeMessage } from '../../src/scale/fanout/pack.js';
import { parseBatches } from '../../src/scale/sender/consumer.js';
import { processBatch, type BatchOutcome } from '../../src/scale/sender/sender.js';
import { createAlertVerifier } from '../../src/signing/index.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

const feed = JSON.parse(
  readFileSync(new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
) as { features: unknown[] };

const TOKEN_KEYS = { current: { id: 't1', secret: 's' }, all: new Map([['t1', 's']]) };
const NOW = new Date('2026-11-09T15:00:00Z');
const clock = { now: () => NOW };

/** Records every enqueued message with its delay, in the wire format. */
class DelayRecordingQueue implements SendQueue {
  readonly messages: Array<{ queue: SendQueueName; body: string; delaySeconds?: number }> = [];
  async enqueue(queue: SendQueueName, batches: SendBatch[], opts?: { delaySeconds?: number }) {
    for (const m of packBatches(batches)) {
      this.messages.push({
        queue,
        body: serializeMessage(m),
        ...(opts?.delaySeconds !== undefined ? { delaySeconds: opts.delaySeconds } : {}),
      });
    }
  }
  take(): Array<{ queue: SendQueueName; body: string; delaySeconds?: number }> {
    return this.messages.splice(0);
  }
}

class RecordingPush implements NativePushSender {
  readonly sent: Array<{ deviceId: string; m: NativePushMessage }> = [];
  constructor(private readonly deviceOf: Map<string, string>) {}
  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    for (const m of messages) {
      this.sent.push({ deviceId: this.deviceOf.get(m.target.token) ?? '?', m });
    }
    return messages.map(() => ({ ok: true }));
  }
  of(deviceId: string): NativePushMessage[] {
    return this.sent.filter((s) => s.deviceId === deviceId).map((s) => s.m);
  }
}

async function world() {
  const alertStore = new InMemoryAlertStore();
  const planQueue = new MemoryPlanQueue();
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
    clock,
    logger: noopLogger,
    env: { FANOUT_MODE: 'queue', PLAN_QUEUE_URL: 'unused-in-memory' },
    planQueue,
  });
  const normalized = engine.normalize(feed.features[0]);
  if (!normalized.ok) throw new Error(normalized.reason);
  const original = normalized.alert;
  original.area.h3Cells = await engine.cellsFor(original.area);
  const applied = await engine.apply(original);
  await engine.onApplied?.(applied);
  const alertId = applied.alert.id;
  const cells = ((await alertStore.get(alertId))?.area.h3Cells ?? []).slice(0, 8);
  expect(cells.length).toBe(8);

  const cellIndex = new MemoryCellIndex();
  const deviceOf = new Map<string, string>();
  const add = async (cell: string, opts: { target?: boolean; second?: string } = {}) => {
    const deviceId = randomUUID();
    const token = `tok-${deviceId}`;
    deviceOf.set(token, deviceId);
    const deviceCells = [...new Set([cell, ...(opts.second ? [opts.second] : [])])].sort();
    const entries: CellIndexEntry[] = deviceCells.map((h3Cell, n) => ({
      h3Cell,
      deviceId,
      placeId: `p${n}-${deviceId}`,
      target: opts.target === false ? null : { platform: 'ios', token },
      language: deviceId < '8' ? 'en' : 'es',
      deviceCells,
    }));
    await cellIndex.put(entries);
    return deviceId;
  };

  const sendQueue = new DelayRecordingQueue();
  const idempotency = new MemoryIdempotencyStore(clock);
  const verifyAlert = createAlertVerifier(TEST_SIGNING.publicKeys);
  const push = new RecordingPush(deviceOf);
  const outcomes: BatchOutcome[] = [];
  const common = { alerts: alertStore, verifyAlert, isDeliveryPaused: async () => false };
  const shardDeps = {
    ...common,
    clock,
    logger: noopLogger,
    cellIndex,
    sendQueue,
    idempotency,
    makeCheckInToken: makeCheckInTokenFor(TOKEN_KEYS.current),
  };
  const senderDeps = {
    idempotency,
    alerts: alertStore,
    verifyAlert,
    isDeliveryPaused: async () => false,
    push,
    sendQueue,
    onDeadTarget: async () => {},
    clock,
    logger: noopLogger,
    sleep: async () => {},
  };

  /** Plans everything on the plan queue, but only for the given cells (the test's area). */
  async function plan(): Promise<void> {
    for (const ready of planQueue.messages.splice(0) as AlertReady[]) {
      const dispatcher = new MemoryShardDispatcher();
      const deps = { ...common, clock, logger: noopLogger, shardDispatcher: dispatcher };
      const r = ready.followUp
        ? await planFollowUp(ready, {
            ...deps,
            droppedCells: (id, src) => alertStore.listDroppedCells(id, src),
          })
        : await planShards(ready, deps);
      expect(r.refused).toBeUndefined();
      const wanted = new Set(cells);
      for (const job of dispatcher.jobs) {
        const mine = { ...job, cells: job.cells.filter((c) => wanted.has(c)) };
        if (mine.cells.length === 0) continue;
        const out = mine.followUp
          ? await processFollowUpShard(mine, shardDeps)
          : await processShard(mine, shardDeps);
        expect(out.refused).toBeUndefined();
      }
    }
  }

  /** Sends the queued messages; `keep` decides which ones stay queued (not delivered yet). */
  async function deliver(
    keep: (b: SendBatch) => boolean = () => false,
  ): Promise<Array<{ queue: SendQueueName; body: string; delaySeconds?: number }>> {
    const held: Array<{ queue: SendQueueName; body: string; delaySeconds?: number }> = [];
    const delivered: typeof held = [];
    for (;;) {
      const msgs = sendQueue.take();
      if (msgs.length === 0) break;
      for (const msg of msgs) {
        const batches = parseBatches(msg.body) ?? [];
        const stay = batches.filter(keep);
        if (stay.length > 0) held.push({ ...msg, body: serializeMessage(stay) });
        const go = batches.filter((x) => !keep(x));
        if (go.length === 0) continue;
        delivered.push(msg);
        outcomes.push(...(await Promise.all(go.map((x) => processBatch(x, senderDeps)))));
      }
    }
    sendQueue.messages.push(...held);
    return delivered;
  }

  return {
    engine,
    alertStore,
    alertId,
    original,
    cells,
    add,
    plan,
    deliver,
    push,
    outcomes,
    idempotency,
    sendQueue,
  };
}

/** A CAP message of the same chain as `original` (no geometry, no zones: a bare Cancel). */
function nextMessage(
  original: Alert,
  over: Partial<Alert> & { sourceId: string; messageType: Alert['messageType'] },
  previous: Alert = original,
): Alert {
  return {
    ...original,
    id: randomUUID(),
    sent: '2026-11-09T07:30:00-08:00',
    references: [{ sourceId: previous.sourceId, sent: previous.sent }],
    area: { ...original.area, geometry: null, ugcCodes: [], sameCodes: [], h3Cells: [] },
    official: { ...original.official, headline: 'The warning has been cancelled.' },
    ...over,
  };
}

describe('cancellation follow-ups, end to end in memory', () => {
  it('alert -> fan-out -> Cancel: each device that got it gets one quiet follow-up, nobody else', async () => {
    const w = await world();
    const [c0, c1, c2, c3, c4, c5, c6, c7] = w.cells as [
      string,
      string,
      string,
      string,
      string,
      string,
      string,
      string,
    ];
    // 40 devices per cell in c0..c5; c6 and c7 are the cells whose batches stay queued.
    const sentCells = [c0, c1, c2, c3, c4, c5];
    const got: string[] = [];
    for (const cell of sentCells) for (let i = 0; i < 40; i++) got.push(await w.add(cell));
    // Two devices with places in two cells: sent once (primary cell), followed up once.
    const multi = [await w.add(c1, { second: c4 }), await w.add(c2, { second: c5 })];
    got.push(...multi);
    const noTarget = await w.add(c0, { target: false });
    const heldBack: string[] = [];
    for (const cell of [c6, c7]) for (let i = 0; i < 30; i++) heldBack.push(await w.add(cell));

    // ---------- the original fan-out; batches of c6 and c7 stay queued ----------
    await w.plan();
    const heldCell = (b: SendBatch) =>
      b.batchKey.includes(`:${c6}:`) || b.batchKey.includes(`:${c7}:`);
    await w.deliver(heldCell);
    for (const id of got) expect(w.push.of(id), id).toHaveLength(1);
    for (const id of heldBack) expect(w.push.of(id), id).toHaveLength(0);
    const original = w.push.sent.length;

    // ---------- a device joins c3 after the fan-out (not in any sent batch) ----------
    const lateJoiner = await w.add(c3);

    // ---------- the CAP Cancel ----------
    const cancel = nextMessage(w.original, { sourceId: 'urn:oid:cancel.1', messageType: 'Cancel' });
    // A bare Cancel (no polygon, no zones): the poll loop does not resolve a Cancel's area.
    expect(cancel.area.h3Cells).toEqual([]);
    const result = await w.engine.apply(cancel);
    expect(result.action).toBe('cancelled');
    await w.engine.onApplied?.(result);
    const stored = await w.alertStore.get(w.alertId);
    // The cancelled record keeps the area it cancels, and says which version it replaced.
    expect(stored?.state).toBe('cancelled');
    expect(stored?.area.h3Cells.length).toBeGreaterThanOrEqual(w.cells.length);
    expect(stored?.supersedes).toEqual({ sourceId: w.original.sourceId, relaySeverity: 'warning' });

    await w.plan();
    // Every follow-up message of a cancel is delayed (in-flight originals land first) and quiet.
    const pending = [...w.sendQueue.messages];
    const followUps = pending.filter((m) => m.body.includes(':fu.cancel.'));
    expect(followUps.length).toBeGreaterThan(0);
    for (const m of followUps) {
      expect(m.delaySeconds).toBe(FOLLOW_UP_DELAY_SECONDS);
      expect(m.queue).toBe('standard');
    }
    // The held-back original batches go out now (after the Cancel): the sender refuses them.
    await w.deliver();
    const after = w.push.sent.slice(original);

    // Exactly one follow-up per device that got the alert, quiet, saying it was cancelled.
    for (const id of got) {
      const mine = after.filter((s) => s.deviceId === id).map((s) => s.m);
      expect(mine, id).toHaveLength(1);
      const m = mine[0] as NativePushMessage;
      expect(m.data['followUp']).toBe('cancel');
      expect(m.priority).toBe('normal');
      expect(m.sound).toBe(false);
      expect(m.collapseKey).toBe(w.alertId);
      expect(m.title).toMatch(/^(Cancelled|Cancelado): /);
      expect(m.data['checkInToken']).toBeTruthy();
    }
    // Nobody else: the late joiner, the device without a target, and the held-back devices got
    // neither the original nor a follow-up.
    for (const id of [lateJoiner, noTarget, ...heldBack]) expect(w.push.of(id), id).toHaveLength(0);
    expect(after).toHaveLength(got.length);
    // The held-back original batches were tombstoned by the follow-up worker (a duplicate to the
    // sender), so they could not go out even through a stale cached read of the alert.
    expect(w.outcomes.filter((o) => o.reason === 'duplicate').length).toBeGreaterThan(0);

    // ---------- the same Cancel again (a replay, or the next poll) sends nothing ----------
    await w.engine.onApplied?.({ action: 'cancelled', alert: stored as Alert });
    await w.plan();
    await w.deliver();
    expect(w.push.sent.length).toBe(original + got.length);
  });

  it('Update then Cancel: the update follow-up still queued is dropped, the cancel is the last word', async () => {
    const w = await world();
    const [c0, c1] = w.cells as [string, string];
    const devices: string[] = [];
    for (const cell of [c0, c1]) for (let i = 0; i < 20; i++) devices.push(await w.add(cell));
    await w.plan();
    await w.deliver();
    for (const id of devices) expect(w.push.of(id)).toHaveLength(1);
    const base = w.push.sent.length;

    // A same-severity Update keeping the area: its follow-up batches are queued, not sent yet.
    const update = nextMessage(w.original, {
      sourceId: 'urn:oid:update.1',
      messageType: 'Update',
      area: w.original.area,
      official: { ...w.original.official, headline: 'Flash Flood Warning extended until 6 PM.' },
    });
    update.area.h3Cells = (await w.alertStore.get(w.alertId))?.area.h3Cells ?? [];
    const u = await w.engine.apply(update);
    expect(u.action).toBe('updated');
    await w.engine.onApplied?.(u);
    await w.plan();
    const updateBatches = w.sendQueue.messages.filter((m) => m.body.includes(':fu.update.'));
    expect(updateBatches.length).toBeGreaterThan(0);
    // Loud like the warning it updates.
    expect(updateBatches.every((m) => m.queue === 'urgent')).toBe(true);

    // The Cancel arrives before those batches are sent.
    const cancel = nextMessage(
      w.original,
      { sourceId: 'urn:oid:cancel.2', messageType: 'Cancel', sent: '2026-11-09T07:40:00-08:00' },
      update,
    );
    const c = await w.engine.apply(cancel);
    await w.engine.onApplied?.(c);
    await w.plan();
    await w.deliver();
    const after = w.push.sent.slice(base);
    for (const id of devices) {
      const mine = after.filter((s) => s.deviceId === id).map((s) => s.m.data['followUp']);
      expect(mine, id).toEqual(['cancel']);
    }
    expect(w.outcomes.some((o) => o.reason === 'stale_version')).toBe(true);
  });

  it('Update that drops a cell: kept cells hear "Updated" (loud), the dropped cell "no longer included" (quiet)', async () => {
    const w = await world();
    const [c0, c1] = w.cells as [string, string];
    const keptDevices: string[] = [];
    const droppedDevices: string[] = [];
    for (let i = 0; i < 15; i++) keptDevices.push(await w.add(c0));
    for (let i = 0; i < 15; i++) droppedDevices.push(await w.add(c1));
    await w.plan();
    await w.deliver();
    const base = w.push.sent.length;

    const all = (await w.alertStore.get(w.alertId))?.area.h3Cells ?? [];
    const update = nextMessage(w.original, {
      sourceId: 'urn:oid:update.2',
      messageType: 'Update',
      area: { ...w.original.area, h3Cells: all.filter((x) => x !== c1) },
    });
    const u = await w.engine.apply(update);
    await w.engine.onApplied?.(u);
    expect(await w.alertStore.listDroppedCells(w.alertId, 'urn:oid:update.2')).toEqual([c1]);
    await w.plan();
    await w.deliver();
    const after = w.push.sent.slice(base);
    for (const id of keptDevices) {
      const mine = after.filter((s) => s.deviceId === id).map((s) => s.m);
      expect(
        mine.map((m) => m.data['followUp']),
        id,
      ).toEqual(['update']);
      expect(mine[0]?.priority).toBe('high');
      expect(mine[0]?.sound).toBe(true);
    }
    for (const id of droppedDevices) {
      const mine = after.filter((s) => s.deviceId === id).map((s) => s.m);
      expect(
        mine.map((m) => m.data['followUp']),
        id,
      ).toEqual(['removed']);
      expect(mine[0]?.priority).toBe('normal');
      expect(mine[0]?.sound).toBe(false);
    }
    expect(after).toHaveLength(30);
  });
});
