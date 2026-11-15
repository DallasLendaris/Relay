import { randomBytes, randomUUID } from 'node:crypto';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndexPage, SendBatch } from '../../../src/scale/contracts.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import {
  PLAN_WAIT_DELAY_SECONDS,
  PLAN_WAIT_FIRST_DELAY_SECONDS,
  PLAN_WAIT_GIVE_UP_MS,
  planWaitDelaySeconds,
  type LateJoinIntent,
  type LateJoinQueue,
} from '../../../src/scale/latejoin/index.js';
import {
  buildLateJoinHandler,
  LATEJOIN_RECORD_CONCURRENCY,
} from '../../../src/scale/latejoin/handler.js';
import type { LateJoinDeps } from '../../../src/scale/latejoin/latejoin.js';
import { processBatch } from '../../../src/scale/sender/sender.js';
import { makeDeps, silent } from '../sender/fakes.js';
import { SJ_CELL, harness, makeAlert } from './helpers.js';

/** The API's cell index, with hooks that run inside the shard worker's page reads. */
class HookedIndex extends MemoryCellIndex {
  before?: () => Promise<void>;
  after?: () => Promise<void>;
  override async listForCell(h3Cell: string, cursor?: string, limit?: number) {
    const b = this.before;
    this.before = undefined;
    if (b) await b();
    const page: CellIndexPage = await super.listForCell(h3Cell, cursor, limit);
    const a = this.after;
    this.after = undefined;
    if (a) await a();
    return page;
  }
}

/** The late-join queue with SQS DelaySeconds: an intent is visible again only when it is due. */
class DelayedQueue implements LateJoinQueue {
  readonly pending: Array<{ due: number; intent: LateJoinIntent }> = [];
  readonly delays: number[] = [];
  constructor(private readonly now: () => number) {}
  async enqueue(intent: LateJoinIntent, opts?: { delaySeconds?: number }): Promise<void> {
    const d = opts?.delaySeconds ?? 0;
    this.delays.push(d);
    this.pending.push({ due: this.now() + d * 1000, intent });
  }
  /** Removes and returns the intents due by now. */
  takeDue(): LateJoinIntent[] {
    const now = this.now();
    const due = this.pending.filter((p) => p.due <= now);
    for (const p of due) this.pending.splice(this.pending.indexOf(p), 1);
    return due.map((p) => p.intent);
  }
  nextDue(): number | undefined {
    return this.pending.reduce<number | undefined>(
      (m, p) => (m === undefined ? p.due : Math.min(m, p.due)),
      undefined,
    );
  }
}

/** API, shard worker, real sender and the real late-join handler on one clock (as in t9). */
async function world() {
  const index = new HookedIndex();
  const h = harness({ cellIndex: index });
  const alert = makeAlert();
  await h.alerts.save(alert);
  const sender = makeDeps({
    idempotency: h.idempotency,
    alerts: h.alerts,
    verifyAlert: () => true,
    clock: h.clock,
  });
  const queue = new DelayedQueue(() => h.time.now.getTime());
  const handle = buildLateJoinHandler(h.deps, undefined, { queue, planWait: {} });
  const run = async (intents: LateJoinIntent[]) => {
    if (intents.length === 0) return;
    const out = await handle({
      Records: intents.map((i) => ({
        messageId: randomUUID(),
        body: JSON.stringify(i),
        attributes: {},
      })),
    } as unknown as SQSEvent);
    expect(out.batchItemFailures).toEqual([]);
  };
  /** Intents the API queued (no delay) go through the handler now. */
  const lateJoinNew = () => run(h.intents.intents.splice(0));
  const plan = () =>
    processShard(
      {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        shardId: 's0',
        cells: [SJ_CELL],
      },
      {
        alerts: h.alerts,
        verifyAlert: () => true,
        isDeliveryPaused: async () => false,
        clock: h.clock,
        logger: silent,
        cellIndex: index,
        sendQueue: h.sendQueue,
        makeCheckInToken: h.deps.makeCheckInToken,
        idempotency: h.idempotency,
      },
    );
  const taken = new Set<number>();
  const deliver = async () => {
    const all = h.sendQueue.batches('urgent');
    for (let i = 0; i < all.length; i++) {
      if (taken.has(i)) continue;
      taken.add(i);
      await processBatch(all[i] as SendBatch, sender.deps);
    }
  };
  const pushesTo = (token: string) =>
    sender.push.calls.flat().filter((m) => m.target.token === token).length;
  const register = async () => {
    const token = randomBytes(32).toString('hex');
    const { deviceId, credential } = await h.register();
    await h.setToken(credential, token);
    await h.addPlace(credential);
    return { deviceId, token };
  };
  const seed = async (n: number) => {
    h.time.now = new Date('2026-11-09T10:00:00Z');
    for (let i = 0; i < n; i++) await register();
    h.time.now = new Date('2026-11-09T12:00:00Z');
    h.intents.intents.length = 0;
  };
  const lateBatches = () =>
    h.sendQueue.batches('urgent').filter((b) => b.batchKey.includes(':late:'));
  /**
   * Moves the clock from re-check to re-check (the queue's next due time), running due intents;
   * `events` run at their own times (seconds after t0) when one comes first. Returns the seconds
   * after t0 of the last re-check, when nothing is waiting any more.
   */
  const waitOut = async (
    t0: number,
    events: Array<{ at: number; run: () => Promise<void> }> = [],
  ): Promise<number> => {
    const todo = [...events].sort((a, b) => a.at - b.at);
    for (let i = 0; i < 100; i++) {
      const next = queue.nextDue();
      if (next === undefined) return (h.time.now.getTime() - t0) / 1000;
      const ev = todo[0];
      if (ev && t0 + ev.at * 1000 <= next) {
        todo.shift();
        h.time.now = new Date(t0 + ev.at * 1000);
        await ev.run();
        continue;
      }
      h.time.now = new Date(next);
      await run(queue.takeDue());
    }
    throw new Error('never settled');
  };
  return {
    h,
    index,
    queue,
    plan,
    deliver,
    pushesTo,
    register,
    seed,
    lateBatches,
    lateJoinNew,
    waitOut,
  };
}

describe('plan wait: how soon a deferred device is decided', () => {
  it('backs off from 2 s to the 15 s cap; the 5 minute bound is unchanged', () => {
    const seq: number[] = [];
    let waited = 0;
    while (waited < PLAN_WAIT_GIVE_UP_MS) {
      const d = planWaitDelaySeconds(waited);
      seq.push(d);
      waited += d * 1000;
    }
    expect(seq.slice(0, 9)).toEqual([2, 2, 2, 3, 5, 7, 11, 15, 15]);
    expect(Math.max(...seq)).toBe(PLAN_WAIT_DELAY_SECONDS);
    expect(seq.length).toBeLessThanOrEqual(26); // re-runs over the whole wait (fixed 15 s: 20)
    // A smaller cap (the load test's smoke runs use 1 s) wins over the first delay.
    expect(planWaitDelaySeconds(0, 1)).toBe(1);
    expect(planWaitDelaySeconds(10_000_000)).toBe(PLAN_WAIT_DELAY_SECONDS);
    expect(PLAN_WAIT_FIRST_DELAY_SECONDS).toBe(2);
  });

  it('in the planned batch, sent 5 s later: decided (skipped) within one short re-check of the send', async () => {
    const w = await world();
    await w.seed(30);
    let d = { deviceId: '', token: '' };
    const t0 = w.h.time.now.getTime();
    w.index.before = async () => {
      d = await w.register(); // the read includes it
      await w.lateJoinNew(); // the cell is not planned yet: deferred
    };
    await w.plan(); // the record has the device; its batch is queued, not sent
    expect(new Set(w.queue.delays)).toEqual(new Set([2])); // the first re-check (was 15 s)

    // The sender pushes the planned batch (with the device) 5 s after the deferral.
    const SENT_AT = 5;
    const decidedAt = await w.waitOut(t0, [{ at: SENT_AT, run: () => w.deliver() }]);
    // Decided at the first re-check after the send: within max(2, 5 / 2) s of it (was up to 15 s).
    expect(decidedAt).toBeGreaterThanOrEqual(SENT_AT);
    expect(decidedAt - SENT_AT).toBeLessThanOrEqual(3);
    expect(w.pushesTo(d.token)).toBe(1); // the planned push only
    expect(w.lateBatches()).toEqual([]);
  });

  it('read before it registered (not in the plan): sent at the first re-check, 2 s after', async () => {
    const w = await world();
    await w.seed(30);
    let d = { deviceId: '', token: '' };
    const t0 = w.h.time.now.getTime();
    w.index.after = async () => {
      d = await w.register(); // after the page was read: the plan misses it
      await w.lateJoinNew(); // no plan record yet: deferred
    };
    await w.plan();
    expect(w.lateBatches()).toEqual([]);
    const sentAt = await w.waitOut(t0);
    expect(w.lateBatches().map((b) => b.recipients[0]?.deviceId)).toEqual([d.deviceId]);
    expect(sentAt).toBe(2); // was 15 s
    await w.deliver();
    expect(w.pushesTo(d.token)).toBe(1);
  });

  it('a cell that is never planned still waits the full bound, then sends (no miss)', async () => {
    const w = await world();
    const t0 = w.h.time.now.getTime();
    // A plan record that never arrives: the device registers and nothing plans its cell.
    const d = await w.register();
    // The handler only waits when the cell is not planned; nothing plans it here.
    await w.lateJoinNew();
    const waitedS = await w.waitOut(t0);
    expect(waitedS).toBeGreaterThanOrEqual(PLAN_WAIT_GIVE_UP_MS / 1000);
    expect(waitedS).toBeLessThanOrEqual(PLAN_WAIT_GIVE_UP_MS / 1000 + PLAN_WAIT_DELAY_SECONDS);
    expect(Math.max(...w.queue.delays)).toBe(PLAN_WAIT_DELAY_SECONDS);
    expect(w.lateBatches().map((b) => b.recipients[0]?.deviceId)).toEqual([d.deviceId]);
  });
});

describe('late-join handler: records of a batch', () => {
  /** A device store whose reads take a while, counting how many are in flight at once. */
  function slowDeps(h: ReturnType<typeof harness>) {
    const seen = { inflight: 0, max: 0, perDevice: new Map<string, number>(), maxPerDevice: 0 };
    const deps: LateJoinDeps = {
      ...h.deps,
      devices: {
        get: async (id) => {
          seen.inflight++;
          seen.max = Math.max(seen.max, seen.inflight);
          const n = (seen.perDevice.get(id) ?? 0) + 1;
          seen.perDevice.set(id, n);
          seen.maxPerDevice = Math.max(seen.maxPerDevice, n);
          await new Promise((r) => setTimeout(r, 15));
          seen.inflight--;
          seen.perDevice.set(id, (seen.perDevice.get(id) ?? 1) - 1);
          return h.devices.get(id);
        },
        pushTargetMeta: (id) => h.devices.pushTargetMeta(id),
      },
    };
    return { deps, seen };
  }
  const event = (bodies: unknown[]) =>
    ({
      Records: bodies.map((b) => ({
        messageId: randomUUID(),
        body: JSON.stringify(b),
        attributes: {},
      })),
    }) as unknown as SQSEvent;

  it('handles distinct devices side by side (not one after another)', async () => {
    const h = harness();
    await h.alerts.save(makeAlert());
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      const { deviceId, credential } = await h.register();
      await h.setToken(credential, randomBytes(32).toString('hex'));
      await h.addPlace(credential);
      ids.push(deviceId);
    }
    const { deps, seen } = slowDeps(h);
    const out = await buildLateJoinHandler(deps)(
      event(ids.map((deviceId) => ({ deviceId, reason: 'place_added' }))),
    );
    expect(out.batchItemFailures).toEqual([]);
    expect(seen.max).toBe(LATEJOIN_RECORD_CONCURRENCY);
    const late = h.sendQueue.batches('urgent').map((b) => b.recipients[0]?.deviceId);
    expect(late.sort()).toEqual([...ids].sort());
  });

  it("keeps one device's records in order: both succeed, one push, no claim race", async () => {
    const h = harness();
    await h.alerts.save(makeAlert());
    const { deviceId, credential } = await h.register();
    await h.setToken(credential, randomBytes(32).toString('hex'));
    await h.addPlace(credential);
    const other = await h.register();
    await h.setToken(other.credential, randomBytes(32).toString('hex'));
    await h.addPlace(other.credential);
    const { deps, seen } = slowDeps(h);
    const out = await buildLateJoinHandler(deps)(
      event([
        { deviceId, reason: 'push_target' },
        { deviceId: other.deviceId, reason: 'place_added' },
        { deviceId: deviceId.toUpperCase(), reason: 'place_added' },
      ]),
    );
    expect(out.batchItemFailures).toEqual([]);
    expect(seen.maxPerDevice).toBe(1);
    expect(seen.max).toBe(2);
    const late = h.sendQueue.batches('urgent').map((b) => b.recipients[0]?.deviceId);
    expect(late.sort()).toEqual([deviceId, other.deviceId].sort());
  });

  it('reports exactly the failed records, whatever order they finish in', async () => {
    const h = harness();
    await h.alerts.save(makeAlert());
    const { deviceId, credential } = await h.register();
    await h.setToken(credential, randomBytes(32).toString('hex'));
    await h.addPlace(credential);
    const e = event([{ deviceId, reason: 'place_added' }, { nope: true }, 'not json']);
    const out = await buildLateJoinHandler(h.deps)(e);
    expect(out.batchItemFailures.map((f) => f.itemIdentifier)).toEqual(
      e.Records.slice(1).map((r) => r.messageId),
    );
  });
});
