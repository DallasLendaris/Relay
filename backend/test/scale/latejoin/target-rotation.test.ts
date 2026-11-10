import { randomBytes, randomUUID } from 'node:crypto';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { DynamoIdempotencyStore } from '../../../src/adapters/aws/dynamo-idempotency-store.js';
import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndexPage, NativePushResult, SendBatch } from '../../../src/scale/contracts.js';
import { planMarkerKey } from '../../../src/scale/fanout/keys.js';
import {
  encodeMemberSet,
  hasMember,
  TARGET_HASH_BYTES,
  targetMemberOf,
} from '../../../src/scale/fanout/member-set.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { PLAN_WAIT_DELAY_SECONDS } from '../../../src/scale/latejoin/index.js';
import { buildLateJoinHandler } from '../../../src/scale/latejoin/handler.js';
import { processBatch } from '../../../src/scale/sender/sender.js';
import { FakePush, makeDeps, silent } from '../sender/fakes.js';
import { SJ_CELL, harness, makeAlert } from './helpers.js';

/**
 * G5b: late join's "the planned batch carried this device" must mean "carried its CURRENT push
 * target". The sent set names devices only; the plan record's target set (or REL-12's timestamp)
 * says whether the target the batch pushed is still the device's.
 */
class HookedIndex extends MemoryCellIndex {
  before?: () => Promise<void>;
  override async listForCell(h3Cell: string, cursor?: string, limit?: number) {
    const b = this.before;
    this.before = undefined;
    if (b) await b();
    const page: CellIndexPage = await super.listForCell(h3Cell, cursor, limit);
    return page;
  }
}

const DEAD: NativePushResult = { ok: false, kind: 'permanent', error: 'Unregistered' };

async function world(opts: { denseCellRecipients?: number } = {}) {
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
  /** The sender takes every queued batch it has not acked yet, in order. */
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
  const register = async () => {
    const token = randomBytes(32).toString('hex');
    const { deviceId, credential } = await h.register();
    await h.setToken(credential, token);
    await h.addPlace(credential);
    return { deviceId, token, credential };
  };
  const seed = async (n: number) => {
    h.time.now = new Date('2026-11-09T10:00:00Z');
    const out = [];
    for (let i = 0; i < n; i++) out.push(await register());
    h.time.now = new Date('2026-11-09T12:00:00Z');
    h.intents.intents.length = 0;
    return out;
  };
  return { h, index, alert, dead, plan, deliver, attempts, delivered, register, seed };
}

type World = Awaited<ReturnType<typeof world>>;
const helpers = (w: World) => ({
  lateJoin: async () => {
    const handle = buildLateJoinHandler(w.h.deps, undefined, { queue: w.h.intents, planWait: {} });
    const batch = w.h.intents.intents.splice(0);
    for (const intent of batch) {
      const out = await handle({
        Records: [{ messageId: randomUUID(), body: JSON.stringify(intent), attributes: {} }],
      } as unknown as SQSEvent);
      expect(out.batchItemFailures).toEqual([]);
    }
    return batch.length;
  },
});

async function setup(opts: { denseCellRecipients?: number } = {}) {
  const w = await world(opts);
  const { lateJoin } = helpers(w);
  const settle = async () => {
    for (let i = 0; i < 40; i++) {
      await w.deliver();
      if ((await lateJoin()) === 0) return;
      w.h.time.now = new Date(w.h.time.now.getTime() + PLAN_WAIT_DELAY_SECONDS * 1000);
    }
    throw new Error('did not settle');
  };
  const rotate = async (d: { credential: string }) => {
    const token = randomBytes(32).toString('hex');
    await w.h.setToken(d.credential, token); // the API emits a push_target late-join intent
    return token;
  };
  return { ...w, lateJoin, settle, rotate };
}

describe('G5b: a push target rotated after the plan read gets the alert', () => {
  it('rotated after the plan, before its batch is sent (old token dead): the new token is pushed', async () => {
    const w = await setup();
    const [d] = await w.seed(30);
    if (!d) throw new Error('no device');
    await w.plan(); // D is planned with its old token
    w.dead.add(d.token); // app reinstall: the old token is dead
    const fresh = await w.rotate(d);
    await w.settle();
    expect(w.attempts(d.token)).toBe(1); // the planned batch still pushes the old one
    expect(w.delivered(fresh)).toBe(1);
  });

  it('rotated after the planned batch was SENT to the dead old token: the new token is pushed', async () => {
    const w = await setup();
    const [d] = await w.seed(30);
    if (!d) throw new Error('no device');
    w.dead.add(d.token);
    await w.plan();
    await w.deliver();
    expect(w.attempts(d.token)).toBe(1);
    const fresh = await w.rotate(d);
    await w.settle();
    expect(w.delivered(fresh)).toBe(1);
  });

  it('without a target set (a re-planned key) the timestamp rule still sends a rotated target', async () => {
    const w = await setup();
    const [d] = await w.seed(30);
    if (!d) throw new Error('no device');
    w.dead.add(d.token);
    await w.plan();
    await w.plan(); // a second plan of the same cell: its record carries no target set
    const record = await w.h.idempotency.inspect(planMarkerKey(w.alert, SJ_CELL));
    expect(record.members).toBeDefined();
    expect(record.targets).toBeUndefined();
    await w.deliver();
    const fresh = await w.rotate(d);
    await w.settle();
    expect(w.delivered(fresh)).toBe(1);
  });

  it('dense cell: a rotated target is pushed, an unchanged one in its part is not resent', async () => {
    const w = await setup({ denseCellRecipients: 8 });
    const [d, other] = await w.seed(40);
    if (!d || !other) throw new Error('no device');
    w.dead.add(d.token);
    expect((await w.plan()).denseCells).toBe(1);
    await w.deliver();
    const fresh = await w.rotate(d);
    await w.h.setToken(other.credential, other.token); // same token again: no change
    await w.settle();
    expect(w.delivered(fresh)).toBe(1);
    expect(w.attempts(other.token)).toBe(1);
  });

  it('a live rotated token costs at most one duplicate on the phone, never a miss', async () => {
    const w = await setup();
    const [d] = await w.seed(30);
    if (!d) throw new Error('no device');
    await w.plan();
    const fresh = await w.rotate(d);
    await w.settle();
    expect(w.delivered(fresh)).toBe(1);
    expect(w.attempts(d.token) + w.attempts(fresh)).toBeLessThanOrEqual(2);
  });

  it('an unchanged target in the sent batch is still skipped (no new duplicate)', async () => {
    const w = await setup();
    let d = { deviceId: '', token: '', credential: '' };
    await w.seed(30);
    w.index.before = async () => {
      d = await w.register(); // registers during the read: its target postdates the alert
    };
    await w.plan();
    const record = await w.h.idempotency.inspect(planMarkerKey(w.alert, SJ_CELL));
    expect(
      hasMember(
        record.targets as Uint8Array,
        targetMemberOf(d.deviceId, { platform: 'ios', token: d.token }),
        TARGET_HASH_BYTES,
      ),
    ).toBe(true);
    await w.settle();
    expect(w.attempts(d.token)).toBe(1);
  });
});

describe('G5b: a delivery pause restarts the plan wait', () => {
  it('a waiting intent parked through a pause longer than the wait bound does not duplicate', async () => {
    const w = await setup();
    await w.seed(30);
    let d = { deviceId: '', token: '', credential: '' };
    w.index.before = async () => {
      d = await w.register();
    };
    await w.plan();
    await w.lateJoin(); // in the plan, batch not sent: waiting
    w.h.control.paused = true;
    await w.deliver(); // paused: nothing is acked
    for (let i = 0; i < 3; i++) {
      await w.lateJoin(); // parked
      w.h.time.now = new Date(w.h.time.now.getTime() + 900_000);
    }
    w.h.control.paused = false;
    await w.lateJoin(); // first after the pause: its plan wait starts again rather than being spent
    await w.settle();
    expect(w.attempts(d.token)).toBe(1);
  });
});

describe('DynamoIdempotencyStore plan target sets', () => {
  it('stores the target set only on the first record of a key', async () => {
    const calls: Array<{ name: string; input: Record<string, any> }> = [];
    let exists = false;
    const store = new DynamoIdempotencyStore(
      {
        send: async (cmd: { constructor: { name: string }; input: Record<string, any> }) => {
          calls.push({ name: cmd.constructor.name, input: cmd.input });
          if (cmd.input['ConditionExpression'] && exists) {
            throw new ConditionalCheckFailedException({ message: 'exists', $metadata: {} });
          }
          exists = true;
          return {};
        },
      } as never,
      'tbl',
      { now: () => new Date('2026-11-09T12:00:00Z') },
    );
    const members = encodeMemberSet(['a'], 4) as Uint8Array;
    const targets = encodeMemberSet(['a\nios\nt'], TARGET_HASH_BYTES) as Uint8Array;
    const until = new Date('2026-11-10T00:00:00Z');

    await store.markPlanned('p', members, until, { targets });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      name: 'PutCommand',
      input: {
        Item: { pk: 'p', members, targets },
        ConditionExpression: 'attribute_not_exists(pk)',
      },
    });
    await store.markPlanned('p', members, until, { targets });
    expect(calls).toHaveLength(3);
    expect(calls[2]?.input['Item']).toMatchObject({ pk: 'p', members });
    expect(calls[2]?.input['Item']).not.toHaveProperty('targets');
    expect(calls[2]?.input).not.toHaveProperty('ConditionExpression');
  });
});
