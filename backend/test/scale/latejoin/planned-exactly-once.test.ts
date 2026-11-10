import { randomBytes, randomUUID } from 'node:crypto';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { DynamoIdempotencyStore } from '../../../src/adapters/aws/dynamo-idempotency-store.js';
import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndexPage, SendBatch } from '../../../src/scale/contracts.js';
import { bucketFor, densePartFor } from '../../../src/scale/fanout/hash.js';
import { bucketKey, densePartKey, planMarkerKey } from '../../../src/scale/fanout/keys.js';
import {
  encodeMemberSet,
  hasMember,
  MAX_SET_BYTES,
  PLAN_HASH_BYTES,
  SENT_HASH_BYTES,
} from '../../../src/scale/fanout/member-set.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import {
  PLAN_WAIT_DELAY_SECONDS,
  PLAN_WAIT_GIVE_UP_MS,
  processLateJoin,
} from '../../../src/scale/latejoin/index.js';
import { buildLateJoinHandler } from '../../../src/scale/latejoin/handler.js';
import { processBatch, sentDetail } from '../../../src/scale/sender/sender.js';
import { makeDeps, silent } from '../sender/fakes.js';
import { SJ_CELL, harness, makeAlert } from './helpers.js';

/** The API's cell index, with a hook that runs inside the shard worker's page reads. */
class HookedIndex extends MemoryCellIndex {
  /** Runs once, before the next page is read (a registration the page will include). */
  before?: () => Promise<void>;
  /** Runs once, after the next page is read but before the worker sees it (one it will not). */
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

/**
 * API, shard worker, real sender (processBatch) and the late-join handler on shared memory stores,
 * the way they share Aurora, the cell index and the idempotency table in production.
 */
async function world(opts: { denseCellRecipients?: number } = {}) {
  const index = new HookedIndex();
  const h = harness({ cellIndex: index });
  const alert = makeAlert(); // sent 11:00; the clock is at 12:00
  await h.alerts.save(alert);
  const sender = makeDeps({
    idempotency: h.idempotency,
    alerts: h.alerts,
    verifyAlert: () => true,
    clock: h.clock,
  });

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
        ...(opts.denseCellRecipients ? { denseCellRecipients: opts.denseCellRecipients } : {}),
      },
    );

  /** The sender takes every queued batch it has not seen yet, in order (filter picks some). */
  const taken = new Set<number>();
  const deliver = async (filter: (b: SendBatch) => boolean = () => true) => {
    const all = h.sendQueue.batches('urgent');
    for (let i = 0; i < all.length; i++) {
      const b = all[i] as SendBatch;
      if (taken.has(i) || !filter(b)) continue;
      taken.add(i);
      await processBatch(b, sender.deps);
    }
  };
  const pushesTo = (token: string) =>
    sender.push.calls.flat().filter((m) => m.target.token === token).length;

  /** A device registering through the API: its place and target go to the index and an intent. */
  const register = async () => {
    const token = randomBytes(32).toString('hex');
    const { deviceId, credential } = await h.register();
    await h.setToken(credential, token);
    await h.addPlace(credential);
    return { deviceId, token };
  };
  /**
   * Devices that registered before the alert. n = 'all-buckets': until every bucket of the cell
   * has one, so the planned batch of any later device's bucket exists without it.
   */
  const seed = async (n: number | 'all-buckets') => {
    h.time.now = new Date('2026-11-09T10:00:00Z');
    const out: Array<{ deviceId: string; token: string }> = [];
    const buckets = new Set<number>();
    while (n === 'all-buckets' ? buckets.size < 64 : out.length < n) {
      const d = await register();
      buckets.add(bucketFor(d.deviceId));
      out.push(d);
    }
    h.time.now = new Date('2026-11-09T12:00:00Z');
    h.intents.intents.length = 0;
    return out;
  };

  /** Runs every queued intent once through the real handler, parking to the same queue. */
  const handle = buildLateJoinHandler(h.deps, undefined, { queue: h.intents, planWait: {} });
  const lateJoin = async () => {
    const batch = h.intents.intents.splice(0);
    for (const intent of batch) {
      const out = await handle({
        Records: [{ messageId: randomUUID(), body: JSON.stringify(intent), attributes: {} }],
      } as unknown as SQSEvent);
      expect(out.batchItemFailures).toEqual([]);
    }
    return batch.length;
  };
  /** Late join, then the sender, until neither has anything left (bounded). */
  const settle = async () => {
    for (let i = 0; i < 20; i++) {
      await deliver();
      if ((await lateJoin()) === 0) return;
      h.time.now = new Date(h.time.now.getTime() + PLAN_WAIT_DELAY_SECONDS * 1000);
    }
    throw new Error('did not settle');
  };
  return { h, index, alert, sender, plan, deliver, pushesTo, register, seed, lateJoin, settle };
}

const lateBatches = (h: { sendQueue: { batches(q: 'urgent'): SendBatch[] } }) =>
  h.sendQueue.batches('urgent').filter((b) => b.batchKey.includes(':late:'));

describe('G5: a device registering during an active fan-out gets exactly one push', () => {
  it('registers during the read and IS in the planned batch: late join waits, then skips', async () => {
    const w = await world();
    await w.seed(30);
    let d = { deviceId: '', token: '' };
    w.index.before = async () => {
      d = await w.register(); // written before the page is read: the plan includes it
      await w.lateJoin(); // the intent runs while the shard is still reading
    };
    await w.plan();
    expect(w.h.intents.log.at(-1)?.delaySeconds).toBe(PLAN_WAIT_DELAY_SECONDS); // deferred
    expect(lateBatches(w.h)).toEqual([]);

    // Planned now (the record has the device), batch still queued: late join keeps waiting.
    await w.lateJoin();
    expect(lateBatches(w.h)).toEqual([]);
    expect(w.h.intents.intents.length).toBeGreaterThan(0);

    await w.settle();
    expect(w.pushesTo(d.token)).toBe(1);
    expect(lateBatches(w.h)).toEqual([]);
  });

  it('registers during the read and is NOT in the planned batch: late join sends once', async () => {
    const w = await world();
    await w.seed(30);
    let d = { deviceId: '', token: '' };
    w.index.after = async () => {
      d = await w.register(); // after the page was read: the plan misses it
      await w.lateJoin(); // cell not planned yet: wait
    };
    await w.plan();
    expect(lateBatches(w.h)).toEqual([]);
    // The cell's plan record does not have the device: send now, without waiting for the sender.
    await w.lateJoin();
    expect(lateBatches(w.h).map((b) => b.recipients[0]?.deviceId)).toEqual([d.deviceId]);

    await w.settle();
    expect(w.pushesTo(d.token)).toBe(1);
  });

  it('registers after the cell is planned: sent at once, even while its batch is queued', async () => {
    const w = await world();
    const seeded = await w.seed(30);
    await w.plan();
    const d = await w.register();
    await w.lateJoin();
    expect(w.h.intents.intents).toEqual([]); // no waiting
    expect(lateBatches(w.h)).toHaveLength(1);
    await w.settle();
    expect(w.pushesTo(d.token)).toBe(1);
    for (const s of seeded) expect(w.pushesTo(s.token)).toBe(1);
  });

  it('planned batch in flight (leased): waits, then skips once it is sent with the device', async () => {
    const w = await world();
    await w.seed(30);
    let d = { deviceId: '', token: '' };
    w.index.before = async () => {
      d = await w.register();
    };
    await w.plan();
    const key = bucketKey(w.alert, SJ_CELL, bucketFor(d.deviceId));
    // The sender holds the claim (mid-send).
    expect(await w.h.idempotency.claim(key, new Date(w.h.time.now.getTime() + 45_000))).toBe(
      'claimed',
    );
    expect(
      await processLateJoin({ deviceId: d.deviceId }, w.h.deps, { waitForPlan: true }),
    ).toMatchObject({ deferred: 1, sent: 0 });
    const batch = w.h.sendQueue.batches('urgent').find((b) => b.batchKey === key) as SendBatch;
    await w.h.idempotency.markSent(
      key,
      new Date('2026-11-10T00:00:00Z'),
      sentDetail(batch, silent),
    );
    expect(
      await processLateJoin({ deviceId: d.deviceId }, w.h.deps, { waitForPlan: true }),
    ).toMatchObject({ deferred: 0, sent: 0, skipped: expect.objectContaining({ planned: 1 }) });
  });

  it('two plans of one key, the one WITHOUT the device sent first: late join still sends', async () => {
    const w = await world();
    await w.seed('all-buckets');
    await w.plan(); // plan 1: before the device
    let d = { deviceId: '', token: '' };
    w.index.before = async () => {
      d = await w.register();
    };
    await w.plan(); // a re-plan (retry, redrive): its record has the device
    // The sender takes plan 1's copy of the device's key first; plan 2's copy is a duplicate.
    await w.settle();
    expect(w.pushesTo(d.token)).toBe(1);
    expect(lateBatches(w.h).map((b) => b.recipients[0]?.deviceId)).toEqual([d.deviceId]);
  });

  it('a device in the plan whose batch never goes out is sent after the wait bound', async () => {
    const w = await world();
    await w.seed(30);
    let d = { deviceId: '', token: '' };
    w.index.before = async () => {
      d = await w.register();
    };
    await w.plan();
    // The planned batch is stuck (say, in a DLQ): the sender never takes the device's key.
    const key = bucketKey(w.alert, SJ_CELL, bucketFor(d.deviceId));
    const others = (b: SendBatch) => b.batchKey !== key;
    for (let i = 0; i * PLAN_WAIT_DELAY_SECONDS * 1000 < PLAN_WAIT_GIVE_UP_MS; i++) {
      await w.lateJoin();
      await w.deliver(others);
      expect(w.pushesTo(d.token)).toBe(0);
      w.h.time.now = new Date(w.h.time.now.getTime() + PLAN_WAIT_DELAY_SECONDS * 1000);
    }
    await w.lateJoin(); // waited 5 minutes: send (a duplicate is better than a miss)
    expect(w.h.intents.intents).toEqual([]);
    await w.deliver(others);
    expect(w.pushesTo(d.token)).toBe(1);
    expect(w.h.lines.some((l) => l.includes('latejoin.plan_wait_gave_up'))).toBe(true);
  });

  it('an alert cancelled while the intent waits is never sent by late join', async () => {
    const w = await world();
    await w.seed(5);
    let d = { deviceId: '', token: '' };
    w.index.after = async () => {
      d = await w.register();
      await w.lateJoin(); // waits: the cell is not planned
    };
    await w.plan();
    expect(w.h.intents.intents.length).toBeGreaterThan(0);
    await w.h.alerts.save({ ...w.alert, state: 'cancelled' }); // before the intent comes back
    await w.lateJoin();
    expect(lateBatches(w.h)).toEqual([]);
    expect(w.h.intents.intents).toEqual([]); // nothing left to wait for
    expect(w.pushesTo(d.token)).toBe(0);
  });

  it('dense cell: in the part plan waits then skips; after the part plan sends at once', async () => {
    const w = await world({ denseCellRecipients: 8 });
    await w.seed(40);
    let inPlan = { deviceId: '', token: '' };
    w.index.before = async () => {
      inPlan = await w.register();
    };
    const r = await w.plan();
    expect(r.denseCells).toBe(1);
    expect(r.planRecords).toBe(16); // one per part
    const part = densePartKey(
      w.alert,
      SJ_CELL,
      bucketFor(inPlan.deviceId),
      densePartFor(inPlan.deviceId),
    );
    expect(w.h.sendQueue.batches('urgent').some((b) => b.batchKey === part)).toBe(true);
    await w.lateJoin();
    expect(w.h.intents.intents.length).toBeGreaterThan(0); // waiting for its part to be sent

    const after = await w.register();
    await w.settle();
    expect(w.pushesTo(inPlan.token)).toBe(1);
    expect(w.pushesTo(after.token)).toBe(1);
    expect(lateBatches(w.h).map((b) => b.recipients[0]?.deviceId)).toEqual([after.deviceId]);
  });

  it('a cell with no recipients is planned too (empty record): no waiting there', async () => {
    const w = await world();
    const r = await w.plan();
    expect(r.planRecords).toBe(1);
    expect(await w.h.idempotency.inspect(planMarkerKey(w.alert, SJ_CELL))).toEqual({
      state: 'sent',
      members: new Uint8Array(0),
      targets: new Uint8Array(0),
    });
    const d = await w.register();
    await w.settle();
    expect(w.pushesTo(d.token)).toBe(1);
  });
});

describe('G5: without exact records late join keeps the REL-12 rule', () => {
  it('no waiting option: the cell not planned yet means send (as before)', async () => {
    const w = await world();
    const d = await w.register();
    w.h.intents.intents.length = 0;
    expect(await processLateJoin({ deviceId: d.deviceId }, w.h.deps)).toMatchObject({
      sent: 1,
      deferred: 0,
    });
  });

  it('a predating device NOT in the plan record is sent even when its key was sent without a set', async () => {
    const w = await world();
    const [d] = await w.seed(1);
    const key = bucketKey(w.alert, SJ_CELL, bucketFor(d?.deviceId as string));
    // An older sender marked the key sent with no set, and the plan record does not have d.
    await w.h.idempotency.markSent(key, new Date('2026-11-10T00:00:00Z'));
    await w.h.idempotency.markPlanned(
      planMarkerKey(w.alert, SJ_CELL),
      encodeMemberSet([], PLAN_HASH_BYTES) as Uint8Array,
      new Date('2026-11-10T00:00:00Z'),
    );
    expect((await processLateJoin({ deviceId: d?.deviceId as string }, w.h.deps)).sent).toBe(1);
  });
});

describe('member sets', () => {
  it('membership is exact for the ids put in and (with 8 bytes) for 10,000 others', () => {
    const ids = Array.from({ length: 300 }, () => randomUUID());
    const set = encodeMemberSet(ids, SENT_HASH_BYTES) as Uint8Array;
    expect(set.length).toBe(300 * 8); // about 2.4 KB per 300 recipients
    for (const id of ids) expect(hasMember(set, id, SENT_HASH_BYTES)).toBe(true);
    for (let i = 0; i < 10_000; i++)
      expect(hasMember(set, randomUUID(), SENT_HASH_BYTES)).toBe(false);
    expect(encodeMemberSet([...ids, ...ids], SENT_HASH_BYTES)).toEqual(set); // de-duplicated
  });

  it('over the cap there is no set (callers fall back), and a malformed set has no members', () => {
    expect(encodeMemberSet(['a', 'b', 'c'], 8, 16)).toBeUndefined();
    expect(MAX_SET_BYTES).toBeLessThan(400 * 1024);
    expect(hasMember(new Uint8Array(7), 'a', 8)).toBe(false);
  });

  it('the sender records a planned batch, not a retry or a late-join batch', async () => {
    const w = await world();
    const seeded = await w.seed(3);
    await w.plan();
    await w.deliver();
    const key = bucketKey(w.alert, SJ_CELL, bucketFor(seeded[0]?.deviceId as string));
    const got = await w.h.idempotency.inspect(key);
    expect(got.state).toBe('sent');
    expect(hasMember(got.members as Uint8Array, seeded[0]?.deviceId as string, 8)).toBe(true);
    const batch = { batchKey: key, attempt: 0, recipients: [] };
    expect(sentDetail({ ...batch, attempt: 1 }, silent)).toBeUndefined();
    expect(sentDetail({ ...batch, batchKey: `${w.alert.id}:warning:late:ab` }, silent)).toBe(
      undefined,
    );
  });
});

describe('DynamoIdempotencyStore member sets', () => {
  it('markSent stores the set under its condition, markPlanned is one PutItem, inspect one GetItem', async () => {
    const calls: Array<{ name: string; input: Record<string, any> }> = [];
    let item: Record<string, unknown> | undefined;
    const store = new DynamoIdempotencyStore(
      {
        send: async (cmd: { constructor: { name: string }; input: Record<string, any> }) => {
          calls.push({ name: cmd.constructor.name, input: cmd.input });
          return { Item: item };
        },
      } as never,
      'tbl',
      { now: () => new Date('2026-11-09T12:00:00Z') },
    );
    const members = encodeMemberSet(['a', 'b'], SENT_HASH_BYTES) as Uint8Array;
    const until = new Date('2026-11-10T00:00:00Z');

    await store.markSent('k', until, { members });
    const update = calls.at(-1);
    expect(update?.name).toBe('UpdateCommand');
    expect(update?.input['UpdateExpression']).toContain('members = :members');
    expect(update?.input['ConditionExpression']).toContain('#state <> :sent');
    expect(update?.input['ExpressionAttributeValues'][':members']).toBe(members);
    await store.markSent('k2', until);
    expect(calls.at(-1)?.input['UpdateExpression']).not.toContain('members');

    await store.markPlanned('p', members, until);
    expect(calls.at(-1)).toMatchObject({
      name: 'PutCommand',
      input: { Item: { pk: 'p', state: 'sent', ttl: until.getTime() / 1000, members } },
    });
    await store.markPlanned('p', null, until);
    expect(calls.at(-1)?.input['Item']).not.toHaveProperty('members');

    item = { pk: 'p', state: 'sent', members };
    expect(await store.inspect('p')).toEqual({ state: 'sent', members });
    item = { pk: 'p', state: 'leased', leaseUntil: Date.parse('2026-11-09T12:01:00Z') };
    expect(await store.inspect('p')).toEqual({ state: 'leased' });
    expect(calls.at(-1)).toMatchObject({ name: 'GetCommand', input: { ConsistentRead: true } });
  });
});

describe('G5: plan records are only written for settled cells', () => {
  it('a dense cell with a contended bucket writes no plan record (its retry does)', async () => {
    const w = await world({ denseCellRecipients: 8 });
    const [d] = await w.seed(40);
    const held = bucketKey(w.alert, SJ_CELL, bucketFor(d?.deviceId as string));
    // A sender is mid-way through the normal batch of one bucket: that bucket is contended.
    await w.h.idempotency.claim(held, new Date(w.h.time.now.getTime() + 45_000));
    await expect(w.plan()).rejects.toThrow('held by another attempt');
    for (let part = 0; part < 16; part++) {
      expect((await w.h.idempotency.inspect(planMarkerKey(w.alert, SJ_CELL, part))).state).toBe(
        'absent',
      );
    }
    // So a device registering now waits rather than racing the retry's parts.
    await w.register();
    await w.lateJoin();
    expect(lateBatches(w.h)).toEqual([]);
  });
});
