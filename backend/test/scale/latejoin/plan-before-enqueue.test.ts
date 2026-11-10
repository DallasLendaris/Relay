import { randomBytes, randomUUID } from 'node:crypto';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndexPage, NativePushResult, SendBatch } from '../../../src/scale/contracts.js';
import { bucketFor, densePartFor } from '../../../src/scale/fanout/hash.js';
import { bucketKey, planMarkerKey } from '../../../src/scale/fanout/keys.js';
import { targetMemberOf } from '../../../src/scale/fanout/member-set.js';
import { processShard, type ShardWorkerDeps } from '../../../src/scale/fanout/shard-worker.js';
import { PLAN_WAIT_DELAY_SECONDS } from '../../../src/scale/latejoin/index.js';
import { buildLateJoinHandler } from '../../../src/scale/latejoin/handler.js';
import { processBatch } from '../../../src/scale/sender/sender.js';
import { FakePush, makeDeps, silent } from '../sender/fakes.js';
import { SJ_CELL, harness, makeAlert } from './helpers.js';

/**
 * G5c: a plan record's target set is kept only on the key's first record, so late join may trust
 * it only if no batch of that key was enqueued before the first record existed. Every path of the
 * shard worker (contended dense buckets, set-aside buckets and their re-read, a crash in between,
 * a failed record write) must write a part's record before any of the part's batches leave it.
 */
class HookedIndex extends MemoryCellIndex {
  /** Throw on the first read of a page that was already read (a dense part's re-read). */
  crashOnReread = false;
  private readonly read = new Set<string>();
  override async listForCell(h3Cell: string, cursor?: string, limit?: number) {
    const k = `${h3Cell}|${cursor ?? ''}`;
    if (this.crashOnReread && this.read.has(k)) {
      this.crashOnReread = false;
      throw new Error('crash before the re-read');
    }
    this.read.add(k);
    const page: CellIndexPage = await super.listForCell(h3Cell, cursor, limit);
    return page;
  }
}

const DEAD: NativePushResult = { ok: false, kind: 'permanent', error: 'Unregistered' };

async function setup() {
  const index = new HookedIndex();
  const h = harness({ cellIndex: index });
  const alert = makeAlert(); // sent 11:00
  await h.alerts.save(alert);
  const dead = new Set<string>();
  const push = new FakePush((m) => (dead.has(m.target.token) ? DEAD : { ok: true }));
  const sender = makeDeps({
    idempotency: h.idempotency,
    alerts: h.alerts,
    verifyAlert: () => true,
    clock: h.clock,
    push,
    isDeliveryPaused: async () => h.control.paused,
  });
  const plan = (over: Partial<ShardWorkerDeps> = {}) =>
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
        denseCellRecipients: 8,
        ...over,
      },
    );
  const acked = new Set<number>();
  const deliver = async () => {
    const all = h.sendQueue.batches('urgent');
    for (let i = 0; i < all.length; i++) {
      if (acked.has(i)) continue;
      if ((await processBatch(all[i] as SendBatch, sender.deps)).ack) acked.add(i);
    }
  };
  const attempts = (token: string) =>
    push.calls.flat().filter((m) => m.target.token === token).length;
  const delivered = (token: string) => (dead.has(token) ? 0 : attempts(token));
  const seed = async (n: number) => {
    h.time.now = new Date('2026-11-09T10:00:00Z');
    const out = [];
    for (let i = 0; i < n; i++) {
      const token = randomBytes(32).toString('hex');
      const { deviceId, credential } = await h.register();
      await h.setToken(credential, token);
      await h.addPlace(credential);
      out.push({ deviceId, token, credential });
    }
    h.time.now = new Date('2026-11-09T12:00:00Z');
    h.intents.intents.length = 0;
    return out;
  };
  const lateJoin = async () => {
    const handle = buildLateJoinHandler(h.deps, undefined, { queue: h.intents, planWait: {} });
    const batch = h.intents.intents.splice(0);
    for (const intent of batch) {
      const out = await handle({
        Records: [{ messageId: randomUUID(), body: JSON.stringify(intent), attributes: {} }],
      } as unknown as SQSEvent);
      expect(out.batchItemFailures).toEqual([]);
    }
    return batch.length;
  };
  const settle = async () => {
    for (let i = 0; i < 40; i++) {
      await deliver();
      if ((await lateJoin()) === 0) return;
      h.time.now = new Date(h.time.now.getTime() + PLAN_WAIT_DELAY_SECONDS * 1000);
    }
    throw new Error('did not settle');
  };
  /** App reinstall: the old token dies, the device registers a new one. */
  const reinstall = async (d: { token: string; credential: string }) => {
    dead.add(d.token);
    const token = randomBytes(32).toString('hex');
    await h.setToken(d.credential, token); // the API emits a push_target late-join intent
    return token;
  };
  /** Every dense batch enqueued so far, with the plan record late join reads for it. */
  const unrecorded = async () => {
    const out: string[] = [];
    for (const b of h.sendQueue.batches('urgent')) {
      const m = /:(\d+)\.(\d+)\/16/.exec(b.batchKey);
      if (!m) continue;
      const rec = await h.idempotency.inspect(planMarkerKey(alert, SJ_CELL, Number(m[2])));
      if (rec.state !== 'sent') out.push(b.batchKey);
    }
    return out;
  };
  return {
    h,
    index,
    alert,
    plan,
    attempts,
    delivered,
    seed,
    lateJoin,
    settle,
    reinstall,
    unrecorded,
  };
}

type World = Awaited<ReturnType<typeof setup>>;
const carrying = (w: World, deviceId: string) =>
  w.h.sendQueue.batches('urgent').filter((b) => b.recipients.some((r) => r.deviceId === deviceId));

describe('G5c: a dense part is recorded before any of its batches is enqueued', () => {
  it('contended bucket: the retry does not write a first record over batches already enqueued', async () => {
    const w = await setup();
    const devs = await w.seed(40);
    const d = devs[0]!;
    const other = devs.find((o) => bucketFor(o.deviceId) !== bucketFor(d.deviceId))!;
    const held = bucketKey(w.alert, SJ_CELL, bucketFor(other.deviceId));
    await w.h.idempotency.claim(held, new Date(w.h.time.now.getTime() + 45_000));
    await expect(w.plan()).rejects.toThrow(); // DenseBucketContendedError (no retryLater)
    expect(carrying(w, d.deviceId)).toHaveLength(1); // attempt 1 enqueued d's batch (token A)
    expect(await w.unrecorded()).toEqual([]);
    const rec = await w.h.idempotency.inspect(
      planMarkerKey(w.alert, SJ_CELL, densePartFor(d.deviceId)),
    );
    expect(rec.members).toBeUndefined(); // not all of the part yet: late join waits, as before

    const token = await w.reinstall(d);
    await w.lateJoin();
    await w.h.idempotency.markSent(held, new Date('2026-11-10T00:00:00Z'));
    await w.plan(); // the retry, with a newer read: (d, token B)
    await w.settle();
    expect(w.attempts(d.token)).toBe(1); // attempt 1's batch, to the dead token
    expect(w.delivered(token)).toBe(1);
  });

  it('set-aside buckets, crash before the re-read: the batches already out are recorded', async () => {
    const w = await setup();
    const devs = await w.seed(40);
    w.index.crashOnReread = true;
    // One entry per page and a hold cap of 1: a part with two buckets sets some aside.
    await expect(w.plan({ pageSize: 1, partHoldCap: 1, flushBatches: 1 })).rejects.toThrow(
      'crash before the re-read',
    );
    const out = w.h.sendQueue.batches('urgent');
    expect(out.length).toBeGreaterThan(0);
    expect(await w.unrecorded()).toEqual([]);
    // A device in the last batch out (the set-aside part's first buckets) reinstalls. The retry
    // reads it with its new token and sets nothing aside, so its record would otherwise be first.
    const id = out[out.length - 1]!.recipients[0]!.deviceId;
    const d = devs.find((x) => x.deviceId === id)!;
    const token = await w.reinstall(d);
    await w.lateJoin();
    await w.plan();
    await w.settle();
    expect(w.attempts(d.token)).toBe(1);
    expect(w.delivered(token)).toBe(1);
  });

  it('a failed record write fails the attempt before that part is enqueued', async () => {
    const w = await setup();
    await w.seed(40);
    const store = w.h.idempotency;
    let failures = 0;
    const flaky: ShardWorkerDeps['idempotency'] = {
      claim: (k, l) => store.claim(k, l),
      markSent: (k, e, x) => store.markSent(k, e, x),
      peek: (k) => store.peek(k),
      // Every record without a target set fails (here: a set-aside part's full set after its
      // re-read, and the empty parts).
      markPlanned: async (k, m, e, x) => {
        if (!x?.targets) {
          failures++;
          throw new Error('throttled');
        }
        return store.markPlanned(k, m, e, x);
      },
    };
    await expect(
      w.plan({ pageSize: 1, partHoldCap: 1, flushBatches: 1, idempotency: flaky }),
    ).rejects.toThrow('throttled');
    expect(failures).toBeGreaterThan(0);
    expect(await w.unrecorded()).toEqual([]);
  });
});

describe('G5c: the target member includes the FCM project', () => {
  it('a token moved to another Firebase project is another target', () => {
    const t = { platform: 'android', token: 'tok' };
    expect(targetMemberOf('d', { ...t, project: 'a' })).not.toBe(
      targetMemberOf('d', { ...t, project: 'b' }),
    );
    expect(targetMemberOf('d', { ...t, project: 'a' })).not.toBe(targetMemberOf('d', t));
    expect(targetMemberOf('d', t)).toBe('d\nandroid\ntok'); // unchanged without a project
  });
});
