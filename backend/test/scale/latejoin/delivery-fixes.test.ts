import { randomUUID } from 'node:crypto';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import { DynamoIdempotencyStore } from '../../../src/adapters/aws/dynamo-idempotency-store.js';
import { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import { MemoryCellIndex, drainOutbox } from '../../../src/scale/cell-index/index.js';
import type { CellIndexOutbox, OutboxRow } from '../../../src/scale/cell-index/outbox.js';
import type { CellIndex } from '../../../src/scale/contracts.js';
import { makeCheckInTokenFor } from '../../../src/scale/fanout/handlers.js';
import { bucketFor } from '../../../src/scale/fanout/hash.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { buildLateJoinHandler } from '../../../src/scale/latejoin/handler.js';
import { processLateJoin } from '../../../src/scale/latejoin/index.js';
import { SJ_CELL, TOKEN_KEY, harness, makeAlert, START } from './helpers.js';

const HOUR = 3_600_000;
const at = (hoursFromStart: number) => new Date(START.getTime() + hoursFromStart * HOUR);

/** A cell index that can be told to fail, like DynamoDB being down during the API's write-through. */
class FlakyIndex extends MemoryCellIndex {
  down = false;
  override async put(entries: Parameters<CellIndex['put']>[0]) {
    if (this.down) throw new Error('dynamo down');
    return super.put(entries);
  }
  override async remove(...a: Parameters<CellIndex['remove']>) {
    if (this.down) throw new Error('dynamo down');
    return super.remove(...a);
  }
  override async updateDevice(...a: Parameters<CellIndex['updateDevice']>) {
    if (this.down) throw new Error('dynamo down');
    return super.updateDevice(...a);
  }
}

class MemoryOutbox implements CellIndexOutbox {
  readonly rows: OutboxRow[] = [];
  async remove(ids: string[]) {
    for (const id of ids) {
      const i = this.rows.findIndex((r) => r.id === id);
      if (i >= 0) this.rows.splice(i, 1);
    }
  }
  async claimDue(_olderThanMs: number, limit: number) {
    return this.rows.slice(0, limit).map((r) => ({ ...r, attempts: ++r.attempts }));
  }
  async stats() {
    return { pending: this.rows.length, maxAttempts: 0, oldestAgeSeconds: 0 };
  }
}

type Harness = ReturnType<typeof harness>;

const recipientsFor = (h: Harness, deviceId: string) =>
  (['urgent', 'standard'] as const)
    .flatMap((q) => h.sendQueue.batches(q))
    .flatMap((b) => b.recipients)
    .filter((r) => r.deviceId === deviceId);

const drainerDeps = (h: Harness, outbox: CellIndexOutbox, index: CellIndex) => ({
  devices: h.devices,
  places: h.places,
  cellIndex: index,
  outbox,
  lateJoin: h.intents,
  now: () => h.time.now,
  log: () => {},
  metric: () => {},
});

const row = (deviceId: string, op: OutboxRow['op'], placeId: string): OutboxRow => ({
  id: randomUUID(),
  deviceId,
  op,
  h3Cell: SJ_CELL,
  placeId,
  attempts: 0,
  createdAt: START.toISOString(),
});

describe('late-join: a device the fan-out never saw (index written late by the drainer)', () => {
  it('write-through fails, alert fans out, drainer runs: the device gets exactly one push', async () => {
    const index = new FlakyIndex();
    const h = harness({ cellIndex: index });
    const outbox = new MemoryOutbox();

    // Two hours before the alert: the device registers, gets a target and a place, but the API's
    // write-through to the cell index fails. Aurora and the outbox row have it; the index does not.
    h.time.now = at(-2);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    index.down = true;
    const placeRes = await h.addPlace(credential);
    index.down = false;
    const placeId = placeRes.body?.['id'] as string;
    outbox.rows.push(row(deviceId, 'put', placeId));
    await h.drain(); // the API's own intents: no alert is active yet
    expect(h.sendQueue.batches('urgent')).toEqual([]);

    // The alert (sent 11:00) fans out. Another device in the same cell AND bucket was indexed, so
    // that (cell, bucket) batch is sent: its key is marked sent.
    const alert = makeAlert();
    await h.alerts.save(alert);
    h.time.now = at(-0.5);
    let otherId = '';
    do otherId = randomUUID();
    while (bucketFor(otherId) !== bucketFor(deviceId));
    await index.put([
      {
        h3Cell: SJ_CELL,
        deviceId: otherId,
        placeId: randomUUID(),
        target: { platform: 'ios', token: 'cd34'.repeat(16) },
        language: 'en',
        deviceCells: [SJ_CELL],
      },
    ]);
    await processShard(
      {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: 'warning',
        shardId: 's0',
        cells: [SJ_CELL],
      },
      {
        alerts: h.alerts,
        verifyAlert: () => true,
        isDeliveryPaused: async () => false,
        clock: h.clock,
        logger: h.deps.logger,
        cellIndex: index,
        sendQueue: h.sendQueue,
        makeCheckInToken: makeCheckInTokenFor(TOKEN_KEY),
      },
    );
    const key = `${alert.id}:warning:${SJ_CELL}:${bucketFor(deviceId)}`;
    const until = new Date(h.time.now.getTime() + HOUR);
    expect(await h.idempotency.claim(key, until)).toBe('claimed');
    await h.idempotency.markSent(key, until);
    expect(recipientsFor(h, deviceId)).toHaveLength(0); // the fan-out never saw the device

    // The old rule: an ordinary intent sees "batch sent, place predates the alert" and skips.
    const skipped = await processLateJoin({ deviceId }, h.deps);
    expect(skipped.sent).toBe(0);
    expect(skipped.skipped.planned).toBe(1);

    // The drainer redoes the write and asks for a late join.
    h.time.now = at(-0.4);
    const run = await drainOutbox(drainerDeps(h, outbox, index));
    expect(run).toMatchObject({ applied: 1, failed: 0 });
    expect(outbox.rows).toEqual([]);
    expect((await index.listForCell(SJ_CELL)).entries.map((e) => e.deviceId)).toContain(deviceId);
    expect(h.intents.intents).toEqual([{ deviceId, reason: 'outbox' }]);

    // The Lambda handler carries the reason through; the device gets the alert...
    const deliver = async () => {
      const body = JSON.stringify(h.intents.intents.shift());
      return buildLateJoinHandler(h.deps)({
        Records: [{ messageId: 'm1', body }],
      } as unknown as SQSEvent);
    };
    expect((await deliver()).batchItemFailures).toEqual([]);
    expect(recipientsFor(h, deviceId)).toHaveLength(1);

    // ...and a repeat of the intent (a retry, or a second drainer pass) is a duplicate target.
    h.intents.intents.push({ deviceId, reason: 'outbox' });
    await deliver();
    expect(recipientsFor(h, deviceId)).toHaveLength(1);
  });

  it('the drainer asks only for devices that have a working target, once per device per run', async () => {
    const index = new MemoryCellIndex();
    const h = harness({ cellIndex: index });
    const outbox = new MemoryOutbox();
    const withTarget = await h.register();
    await h.setToken(withTarget.credential);
    const noTarget = await h.register();
    const p1 = (await h.addPlace(withTarget.credential)).body?.['id'] as string;
    const p2 = (await h.addPlace(noTarget.credential)).body?.['id'] as string;
    outbox.rows.push(
      row(withTarget.deviceId, 'put', p1),
      row(withTarget.deviceId, 'update', ''),
      row(noTarget.deviceId, 'put', p2),
      row(withTarget.deviceId, 'remove', 'gone'),
    );
    h.intents.intents.length = 0;
    await drainOutbox(drainerDeps(h, outbox, index));
    expect(h.intents.intents).toEqual([{ deviceId: withTarget.deviceId, reason: 'outbox' }]);
  });

  it('an enqueue failure leaves the outbox row for the next run', async () => {
    const index = new MemoryCellIndex();
    const h = harness({ cellIndex: index });
    const outbox = new MemoryOutbox();
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    const placeId = (await h.addPlace(credential)).body?.['id'] as string;
    outbox.rows.push(row(deviceId, 'put', placeId));
    const r = await drainOutbox({
      ...drainerDeps(h, outbox, index),
      lateJoin: {
        enqueue: async () => {
          throw new Error('sqs down');
        },
      },
    });
    expect(r).toMatchObject({ applied: 0, failed: 1 });
    expect(outbox.rows).toHaveLength(1);
  });
});

describe('late-join: the planned-batch probe is a read, not a claim', () => {
  it('never claims the batch key (a claim would make the real sender see "leased")', async () => {
    const h = harness();
    h.time.now = at(-2);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    h.intents.intents.length = 0;
    const alert = makeAlert();
    await h.alerts.save(alert);
    h.time.now = at(0);
    const batchKey = `${alert.id}:warning:${SJ_CELL}:${bucketFor(deviceId)}`;
    const claimed: string[] = [];
    const store = new MemoryIdempotencyStore(h.clock);
    const idempotency = {
      claim: (k: string, until: Date) => {
        claimed.push(k);
        return store.claim(k, until);
      },
      markSent: store.markSent.bind(store),
      release: store.release.bind(store),
      peek: store.peek.bind(store),
    };
    // The planned batch is still in flight: the sender holds its lease.
    expect(await store.claim(batchKey, at(1))).toBe('claimed');
    claimed.length = 0;
    const r = await processLateJoin({ deviceId }, { ...h.deps, idempotency });
    expect(r.skipped.planned).toBe(1);
    expect(claimed).not.toContain(batchKey);
    expect(await store.peek(batchKey)).toBe('leased'); // untouched
    await store.markSent(batchKey, at(2)); // the sender's markSent still succeeds
    expect(await store.peek(batchKey)).toBe('sent');
  });

  it('a batch nobody has claimed is not planned: the device is sent', async () => {
    const h = harness();
    h.time.now = at(-2);
    const { credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    await h.alerts.save(makeAlert());
    h.time.now = at(0);
    await h.drain();
    expect(h.sendQueue.batches('urgent')).toHaveLength(1);
  });

  it('MemoryIdempotencyStore.peek: absent, leased, sent, released, lease expiry', async () => {
    let now = START;
    const s = new MemoryIdempotencyStore({ now: () => now });
    expect(await s.peek('k')).toBe('absent');
    await s.claim('k', new Date(now.getTime() + 1000));
    expect(await s.peek('k')).toBe('leased');
    now = new Date(now.getTime() + 2000);
    expect(await s.peek('k')).toBe('absent'); // lease ran out unsent
    await s.claim('k', new Date(now.getTime() + 1000));
    await s.release('k');
    expect(await s.peek('k')).toBe('absent');
    await s.claim('k', new Date(now.getTime() + 1000));
    await s.markSent('k', new Date(now.getTime() + 5000));
    expect(await s.peek('k')).toBe('sent');
  });

  it('DynamoIdempotencyStore.peek is one strongly consistent GetItem and writes nothing', async () => {
    const sent: Array<{ name: string; input: Record<string, unknown> }> = [];
    let item: Record<string, unknown> | undefined;
    const store = new DynamoIdempotencyStore(
      {
        send: async (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
          sent.push({ name: cmd.constructor.name, input: cmd.input });
          return { Item: item };
        },
      } as never,
      'tbl',
      { now: () => START },
    );
    expect(await store.peek('k')).toBe('absent');
    item = { pk: 'k', state: 'leased', leaseUntil: START.getTime() + 1000 };
    expect(await store.peek('k')).toBe('leased');
    item = { pk: 'k', state: 'leased', leaseUntil: START.getTime() - 1 };
    expect(await store.peek('k')).toBe('absent');
    item = { pk: 'k', state: 'sent' };
    expect(await store.peek('k')).toBe('sent');
    item = { pk: 'k' }; // written before leases existed
    expect(await store.peek('k')).toBe('sent');
    expect(sent.every((c) => c.name === 'GetCommand' && c.input['ConsistentRead'] === true)).toBe(
      true,
    );
  });
});

describe('late-join while delivery is paused', () => {
  /** One SQS-like tick: deliver every queued intent through the real Lambda handler at `due`. */
  async function pump(h: Harness, due: Date) {
    const handler = buildLateJoinHandler(h.deps, undefined, { queue: h.intents });
    const failures: string[] = [];
    h.time.now = due;
    for (const intent of h.intents.intents.splice(0)) {
      const out = await handler({
        Records: [
          {
            messageId: randomUUID(),
            body: JSON.stringify(intent),
            // SQS stamps the first send; parked copies carry their own enqueuedAt.
            attributes: { SentTimestamp: String(START.getTime()) },
          },
        ],
      } as unknown as SQSEvent);
      failures.push(...out.batchItemFailures.map((f) => f.itemIdentifier));
    }
    return failures;
  }

  it('pause, 2 h, unpause: parked (never failed), then delivered exactly once', async () => {
    const h = harness({ paused: true });
    await h.alerts.save(makeAlert({ expires: '2026-11-10T12:00:00Z' }));
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential); // the API's intent is now in the queue

    // Every 15 minutes for 2 hours the intent comes back while still paused: it is parked with a
    // delay (max 900 s), never reported as a failure, so it can never reach the DLQ.
    for (let i = 1; i <= 8; i++) {
      expect(await pump(h, new Date(START.getTime() + i * 900_000))).toEqual([]);
      expect(h.intents.intents).toHaveLength(2); // push_target + place_added
      expect(h.intents.log.at(-1)?.delaySeconds).toBe(900);
    }
    expect(h.intents.intents[0]?.enqueuedAt).toBe(START.toISOString()); // original time kept
    expect(recipientsFor(h, deviceId)).toHaveLength(0);

    h.control.paused = false;
    expect(await pump(h, new Date(START.getTime() + 9 * 900_000))).toEqual([]);
    expect(recipientsFor(h, deviceId)).toHaveLength(1);
    expect(h.intents.intents).toEqual([]);
    // A duplicate intent afterwards does not send again.
    h.intents.intents.push({ deviceId, reason: 'place_added' });
    await pump(h, new Date(START.getTime() + 10 * 900_000));
    expect(recipientsFor(h, deviceId)).toHaveLength(1);
  });

  it('gives up (and logs an error) only 24 h after the first enqueue', async () => {
    const h = harness({ paused: true });
    await h.alerts.save(makeAlert({ expires: '2026-11-12T12:00:00Z' }));
    const { credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    expect(await pump(h, new Date(START.getTime() + 23 * HOUR))).toEqual([]);
    expect(h.intents.intents).toHaveLength(2); // still parked at 23 h
    expect(await pump(h, new Date(START.getTime() + 25 * HOUR))).toEqual([]);
    expect(h.intents.intents).toHaveLength(0); // dropped
    expect(h.lines.some((l) => l.includes('latejoin.paused_expired'))).toBe(true);
  });

  it('without a park queue a paused record still retries', async () => {
    const h = harness({ paused: true });
    const out = await buildLateJoinHandler(h.deps)({
      Records: [
        {
          messageId: 'm1',
          body: JSON.stringify({ deviceId: randomUUID(), reason: 'place_added' }),
        },
      ],
    } as unknown as SQSEvent);
    expect(out.batchItemFailures).toEqual([{ itemIdentifier: 'm1' }]);
  });

  it('a failed re-enqueue fails the record so SQS redelivers it', async () => {
    const h = harness({ paused: true });
    const out = await buildLateJoinHandler(h.deps, undefined, {
      queue: {
        enqueue: async () => {
          throw new Error('sqs down');
        },
      },
    })({
      Records: [
        {
          messageId: 'm1',
          body: JSON.stringify({ deviceId: randomUUID(), reason: 'push_target' }),
        },
      ],
    } as unknown as SQSEvent);
    expect(out.batchItemFailures).toEqual([{ itemIdentifier: 'm1' }]);
  });
});

describe('erasure racing POST /places', () => {
  it('leaves no cell-index entry for the erased device (API write-through)', async () => {
    const index = new MemoryCellIndex();
    const h = harness({ cellIndex: index });
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    expect((await h.addPlace(credential)).status).toBe(201);
    const workCell = (await h.places.listForDevice(deviceId))[0]?.h3Cell ?? SJ_CELL;
    // Erase right after the next place's entries are written (the request's re-check then sees
    // the device gone).
    const realPut = index.put.bind(index);
    let erased = false;
    index.put = async (entries) => {
      await realPut(entries);
      if (!erased) {
        erased = true;
        await h.places.removeAllForDevice(deviceId);
        await h.devices.deleteDevice(deviceId, h.time.now);
      }
    };
    h.intents.intents.length = 0;
    const res = await h.addPlace(credential, {
      label: 'Work',
      latitude: 37.78,
      longitude: -122.41,
    });
    expect(res.status).toBe(401);
    const left = [...(await index.listForCell(workCell)).entries];
    expect(left.filter((e) => e.deviceId === deviceId)).toEqual([]);
    const sf = await h.call('GET', '/places', undefined, credential);
    expect(sf.status).toBe(401);
    expect(h.intents.intents).toEqual([]); // no late join for an erased device
  });

  it('the drainer removes the named entry of an erased device and asks for no late join', async () => {
    const index = new MemoryCellIndex();
    const h = harness({ cellIndex: index });
    const outbox = new MemoryOutbox();
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    const placeId = (await h.addPlace(credential)).body?.['id'] as string;
    expect((await index.listForCell(SJ_CELL)).entries).toHaveLength(1);
    outbox.rows.push(row(deviceId, 'put', placeId));
    await h.places.removeAllForDevice(deviceId);
    await h.devices.deleteDevice(deviceId, START); // erased; a stale entry is still in the index
    h.intents.intents.length = 0;
    const r = await drainOutbox(drainerDeps(h, outbox, index));
    expect(r).toMatchObject({ applied: 1, failed: 0 });
    expect((await index.listForCell(SJ_CELL)).entries).toEqual([]);
    expect(h.intents.intents).toEqual([]);
    expect(outbox.rows).toEqual([]);
  });
});
