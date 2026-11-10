import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import type {
  CellIndex,
  CellIndexEntry,
  CellIndexPage,
  SendBatch,
  ShardJob,
} from '../../../src/scale/contracts.js';
import { bucketFor, densePartFor } from '../../../src/scale/fanout/hash.js';
import { plannedKeyFor } from '../../../src/scale/fanout/keys.js';
import { MAX_MESSAGE_BYTES } from '../../../src/scale/fanout/pack.js';
import {
  DenseBucketContendedError,
  bucketKey,
  processShard,
} from '../../../src/scale/fanout/shard-worker.js';
import { processBatch } from '../../../src/scale/sender/sender.js';
import { makeDeps } from '../sender/fakes.js';
import { countyCells, entry, setup, signed, target } from './helpers.js';

const CELL = countyCells()[0] as string;

const jobFor = (a: Alert): ShardJob => ({
  alertId: a.id,
  sourceId: a.sourceId,
  relaySeverity: a.relaySeverity,
  shardId: 'dense:0',
  cells: [CELL],
});

/** A distinct UUID-shaped id for n, its first hex digit spread evenly (like a real v4 UUID). */
const uid = (n: number): string =>
  `${(Math.imul(n + 1, 2654435761) >>> 0).toString(16).padStart(8, '0')}-0000-4000-8000-${String(n).padStart(12, '0')}`;

/** Shard worker and sender sharing one idempotency store, as in production. */
function world(alert: Alert, over: Parameters<typeof setup>[1] = {}) {
  const sender = makeDeps();
  void sender.alerts.save(alert);
  const shard = setup(alert, { idempotency: sender.idempotency, ...over });
  const pushes = new Map<string, number>();
  /** Delivers batches through the real sender; counts pushes per device (the token names it). */
  const deliver = async (batches: SendBatch[]) => {
    for (const b of batches) await processBatch(b, sender.deps);
    pushes.clear();
    for (const m of sender.push.calls.flat()) {
      const device = String(m.data['checkInToken']).split(':')[2] as string;
      pushes.set(device, (pushes.get(device) ?? 0) + 1);
    }
  };
  /** Runs one plan and returns the batches it enqueued (only those). */
  const plan = async () => {
    const before = shard.sendQueue.batches('urgent').length;
    const result = await processShard(jobFor(alert), shard.deps);
    return { result, batches: shard.sendQueue.batches('urgent').slice(before) };
  };
  return { sender, shard, pushes, deliver, plan };
}

const add = (w: ReturnType<typeof world>, from: number, to: number) => {
  for (let i = from; i < to; i++) w.shard.cellIndex.add(entry(CELL, uid(i)));
};
const noDuplicates = (pushes: Map<string, number>) =>
  expect([...pushes.values()].filter((n) => n > 1)).toEqual([]);

describe('dense cells: part keys', () => {
  it('a cell over the dense size sends every device once, in `<bucket>.<part>/16` batches', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 3_000);
    const { result, batches } = await w.plan();
    expect(result).toMatchObject({ denseCells: 1, recipients: 3_000, coveredBuckets: 0 });
    for (const b of batches) {
      const first = b.recipients[0]?.deviceId as string;
      const key = `${bucketKey(alert, CELL, bucketFor(first))}.${densePartFor(first)}/16`;
      expect(b.batchKey).toBe(key);
      for (const r of b.recipients) {
        expect(bucketFor(r.deviceId)).toBe(bucketFor(first));
        expect(densePartFor(r.deviceId)).toBe(densePartFor(first));
      }
    }
    await w.deliver(batches);
    expect(w.pushes.size).toBe(3_000);
    noDuplicates(w.pushes);
    // Every bucket's normal key is tombstoned, so late join reads the cell as planned.
    for (let b = 0; b < 64; b++) {
      expect(await w.sender.idempotency.peek(bucketKey(alert, CELL, b))).toBe('sent');
    }
  });

  it('a normal-size cell keeps its normal keys (no change below the dense size)', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 900);
    const { result, batches } = await w.plan();
    expect(result.denseCells).toBe(0);
    for (const b of batches) {
      expect(b.batchKey).toBe(bucketKey(alert, CELL, bucketFor(b.recipients[0]?.deviceId ?? '')));
    }
  });
});

describe('dense cells: a re-plan of the same alert version is exactly-once across a tier change', () => {
  it('normal plan sent, devices added, dense re-plan: nothing new is sent', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 900); // 900 devices: every one of the 64 buckets is non-empty
    const first = await w.plan();
    expect(first.result.denseCells).toBe(0);
    expect(new Set(first.batches.map((b) => b.batchKey)).size).toBe(64);
    await w.deliver(first.batches);
    expect(w.pushes.size).toBe(900);

    add(w, 900, 1_400); // now dense
    const second = await w.plan();
    expect(second.result).toMatchObject({ denseCells: 1, coveredBuckets: 64, batches: 0 });
    await w.deliver(second.batches);
    expect(w.pushes.size).toBe(900);
    noDuplicates(w.pushes);
  });

  it('normal plan still queued, devices added and removed, dense re-plan: everyone once', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 900);
    const first = await w.plan();
    for (let i = 0; i < 50; i++) w.shard.cellIndex.remove(CELL, uid(i));
    add(w, 900, 1_400);
    const second = await w.plan();
    expect(second.result).toMatchObject({ denseCells: 1, coveredBuckets: 0 });

    // The old normal batches arrive first and are dropped (tombstoned); the parts send.
    await w.deliver([...first.batches, ...second.batches]);
    expect(w.pushes.size).toBe(1_350);
    noDuplicates(w.pushes);
    for (let i = 0; i < 50; i++) expect(w.pushes.has(uid(i))).toBe(false);
  });

  it('the same with the dense parts delivered before the old normal batches', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 900);
    const first = await w.plan();
    add(w, 900, 1_400);
    const second = await w.plan();
    await w.deliver([...second.batches, ...first.batches]);
    expect(w.pushes.size).toBe(1_400);
    noDuplicates(w.pushes);
  });

  it('dense plan sent, devices removed below the dense size: the cell stays dense, nothing new is sent', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 1_400);
    const first = await w.plan();
    expect(first.result.denseCells).toBe(1);
    await w.deliver(first.batches);
    expect(w.pushes.size).toBe(1_400);

    for (let i = 0; i < 600; i++) w.shard.cellIndex.remove(CELL, uid(i));
    const second = await w.plan();
    // Sticky: the cell marker keeps it dense for this alert version, so its keys repeat.
    expect(second.result.denseCells).toBe(1);
    const firstKeys = new Set(first.batches.map((b) => b.batchKey));
    for (const b of second.batches) expect(firstKeys.has(b.batchKey)).toBe(true);
    await w.deliver(second.batches);
    expect(w.pushes.size).toBe(1_400);
    noDuplicates(w.pushes);
  });

  it('dense plan queued, devices removed below the dense size, re-plan: every remaining device once', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 1_400);
    const first = await w.plan();
    for (let i = 0; i < 600; i++) w.shard.cellIndex.remove(CELL, uid(i));
    const second = await w.plan();
    expect(second.result.denseCells).toBe(1);
    await w.deliver([...second.batches, ...first.batches]);
    for (let i = 600; i < 1_400; i++) expect(w.pushes.get(uid(i))).toBe(1);
    noDuplicates(w.pushes);
  });

  it('dense, devices added, dense again: identical part keys, nothing sent twice', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 1_400);
    const first = await w.plan();
    await w.deliver(first.batches);
    add(w, 1_400, 1_600);
    const second = await w.plan();
    const firstKeys = new Set(first.batches.map((b) => b.batchKey));
    // Most keys repeat (only (bucket, part) pairs that were empty before are new).
    const repeated = second.batches.filter((b) => firstKeys.has(b.batchKey)).length;
    expect(repeated).toBeGreaterThan(first.batches.length * 0.9);
    const before = w.pushes.size;
    await w.deliver(second.batches);
    noDuplicates(w.pushes);
    // Only devices in brand-new keys could be sent, and those are new devices.
    const newKeyDevices = second.batches
      .filter((b) => !firstKeys.has(b.batchKey))
      .flatMap((b) => b.recipients.map((r) => r.deviceId));
    expect(w.pushes.size).toBe(before + newKeyDevices.length);
  });

  it('a re-run of the same dense plan (crash after enqueue) re-sends the same keys only', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 2_000);
    const first = await w.plan();
    const second = await w.plan();
    expect(second.batches.map((b) => b.batchKey)).toEqual(first.batches.map((b) => b.batchKey));
    await w.deliver([...first.batches, ...second.batches]);
    expect(w.pushes.size).toBe(2_000);
    noDuplicates(w.pushes);
  });

  it('a bucket whose normal batch is being sent right now is retried, not guessed', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 900);
    const first = await w.plan();
    add(w, 900, 1_400);
    // A sender holds bucket 3's normal key (mid-send).
    const held = bucketKey(alert, CELL, 3);
    expect(await w.sender.idempotency.claim(held, new Date('2026-11-10T10:06:00Z'))).toBe(
      'claimed',
    );
    await expect(w.plan()).rejects.toBeInstanceOf(DenseBucketContendedError);
    // That sender finishes; the shard's retry now finds bucket 3 covered.
    await w.sender.idempotency.markSent(held, new Date('2026-11-12T00:00:00Z'));
    const retry = await w.plan();
    expect(retry.result.coveredBuckets).toBe(1);
    const sent3 = first.batches.filter((b) => b.batchKey === held);
    await w.deliver([...first.batches.filter((b) => b.batchKey !== held), ...retry.batches]);
    const pushed = new Set(w.pushes.keys());
    // Bucket 3's normal batch was "sent" by the holder (not replayed here): its devices are not
    // re-sent by the dense parts.
    for (const r of sent3.flatMap((b) => b.recipients)) expect(pushed.has(r.deviceId)).toBe(false);
    noDuplicates(w.pushes);
  });
});

describe('dense cells: a failed dense attempt never strands its tombstones', () => {
  /** A send queue whose `failAt`-th enqueue throws (an SQS error or the worker dying). */
  const flaky = (w: ReturnType<typeof world>, failAt: number) => {
    let n = 0;
    const real = w.shard.deps.sendQueue;
    return {
      enqueue: async (...args: Parameters<typeof real.enqueue>) => {
        if (++n === failAt) throw new Error('SQS unavailable');
        return real.enqueue(...args);
      },
    };
  };

  it("reviewer's probe: 1,050 devices, dense at 1,000, first enqueue throws, 100 cleared, retry: all 950 once", async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 1_050);
    await expect(
      processShard(jobFor(alert), { ...w.shard.deps, sendQueue: flaky(w, 1) }),
    ).rejects.toThrow('SQS unavailable');
    expect(w.shard.sendQueue.batches('urgent')).toHaveLength(0);
    // The tombstones are written: every normal key reads as sent.
    expect(await w.sender.idempotency.peek(bucketKey(alert, CELL, 0))).toBe('sent');

    // Dead-target clears shrink the cell under the dense size before the retry.
    for (let i = 0; i < 100; i++) w.shard.cellIndex.remove(CELL, uid(i));
    const retry = await w.plan();
    expect(retry.result).toMatchObject({ denseCells: 1, recipients: 950 }); // stays dense
    for (const b of retry.batches) expect(b.batchKey).toMatch(/:\d+\.\d+\/16$/);
    await w.deliver(retry.batches);
    expect(w.pushes.size).toBe(950);
    noDuplicates(w.pushes);
  });

  it('the same when the attempt dies partway (some parts enqueued, then a throw)', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000, flushBatches: 1 });
    add(w, 0, 1_050);
    await expect(
      processShard(jobFor(alert), { ...w.shard.deps, sendQueue: flaky(w, 2) }),
    ).rejects.toThrow();
    const partial = w.shard.sendQueue.batches('urgent').slice();
    expect(partial.length).toBeGreaterThan(0);
    for (let i = 0; i < 100; i++) w.shard.cellIndex.remove(CELL, uid(i));
    const retry = await w.plan();
    await w.deliver([...partial, ...retry.batches]);
    for (let i = 100; i < 1_050; i++) expect(w.pushes.get(uid(i))).toBe(1);
    noDuplicates(w.pushes);
  });

  it('late join reads a stranded tombstone as not planned, and the part key once it is sent', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000 });
    add(w, 0, 1_050);
    await expect(
      processShard(jobFor(alert), { ...w.shard.deps, sendQueue: flaky(w, 1) }),
    ).rejects.toThrow();
    const peek = (k: string) => w.sender.idempotency.peek(k);
    const device = uid(500);
    expect(await plannedKeyFor(alert, CELL, device, peek)).toMatchObject({ state: 'absent' });
    const retry = await w.plan();
    await w.deliver(retry.batches);
    const planned = await plannedKeyFor(alert, CELL, device, peek);
    expect(planned).toEqual({
      key: `${bucketKey(alert, CELL, bucketFor(device))}.${densePartFor(device)}/16`,
      state: 'sent',
    });
  });

  it('a contended bucket defers just its cells through retryLater instead of failing the job', async () => {
    const alert = signed();
    const deferred: ShardJob[] = [];
    const w = world(alert, {
      denseCellRecipients: 1_000,
      retryLater: async (j) => void deferred.push(j),
    });
    add(w, 0, 900);
    await w.plan();
    add(w, 900, 1_400);
    await w.sender.idempotency.claim(bucketKey(alert, CELL, 3), new Date('2026-11-10T10:06:00Z'));
    const r = await w.plan(); // does not throw
    expect(r.result.denseCells).toBe(1);
    expect(deferred).toEqual([{ ...jobFor(alert), cells: [CELL] }]);
    expect(w.shard.lines.some((l) => l.msg === 'fanout.dense_bucket_contended')).toBe(true);
  });
});

/**
 * A cell index that makes its entries on demand, in sort order, so the test's own memory does not
 * hide the worker's. It also samples the heap on every page.
 */
class SyntheticCell implements Pick<CellIndex, 'listForCell'> {
  peakHeap = 0;
  private readonly ids: string[];
  constructor(count: number) {
    this.ids = Array.from({ length: count }, (_, i) => uid(i)).sort();
  }
  async listForCell(h3Cell: string, cursor?: string, limit = 1_000): Promise<CellIndexPage> {
    this.peakHeap = Math.max(this.peakHeap, process.memoryUsage().heapUsed);
    const start = cursor ? Number(cursor) : 0;
    const entries: CellIndexEntry[] = this.ids.slice(start, start + limit).map((deviceId) => ({
      h3Cell,
      deviceId,
      placeId: 'home',
      target: target(deviceId),
      language: 'en',
    }));
    const next = start + limit;
    return next < this.ids.length ? { entries, cursor: String(next) } : { entries };
  }
}

describe('dense cells: memory', () => {
  it('streams a 60,000-device cell part by part instead of holding it (CI-sized 150k scenario)', async () => {
    const alert = signed();
    const cell = new SyntheticCell(60_000);
    const sender = makeDeps();
    // Real-sized check-in tokens (about 230 chars), so the byte budget means something.
    const { deps, sendQueue } = setup(alert, {
      cellIndex: cell,
      idempotency: sender.idempotency,
      makeCheckInToken: (a, d, c) => `v2.${a}.${d}.${c}.`.padEnd(230, 'x'),
      denseCellRecipients: 2_000,
    });
    const heapBefore = process.memoryUsage().heapUsed;
    const r = await processShard(jobFor(alert), deps);
    const heapGrowth = cell.peakHeap - heapBefore;
    console.log(
      `dense 60k cell: peak buffered ${r.peakBufferedRecipients} recipients, heap growth ${(heapGrowth / 1e6).toFixed(1)} MB`,
    );
    expect(r).toMatchObject({ denseCells: 1, recipients: 60_000 });
    // About one part (60,000 / 16 = 3,750) plus a page plus the unflushed batches (5,000), never
    // the whole cell; the old worker held all 60,000 (see the next test).
    expect(r.peakBufferedRecipients).toBeLessThan(3_750 + 1_000 + 5_000 + 3_750);
    const all = sendQueue.batches('urgent').flatMap((b) => b.recipients.map((x) => x.deviceId));
    expect(new Set(all).size).toBe(60_000);
    expect(all).toHaveLength(60_000);
    for (const m of sendQueue.messages.urgent) {
      expect(Buffer.byteLength(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
  });

  it('without the dense split, the same cell would have to be held whole (the old behavior)', async () => {
    const alert = signed();
    const cell = new SyntheticCell(30_000);
    const { deps } = setup(alert, {
      cellIndex: cell,
      makeCheckInToken: (a, d, c) => `v2.${a}.${d}.${c}.`.padEnd(230, 'x'),
      denseCellRecipients: 1_000_000,
    });
    const r = await processShard(jobFor(alert), deps);
    expect(r.peakBufferedRecipients).toBeGreaterThanOrEqual(30_000);
  });

  it('a cell under the dense size but over the byte budget per bucket goes dense too', async () => {
    const alert = signed();
    const cell = new SyntheticCell(12_000);
    const sender = makeDeps();
    const { deps, sendQueue } = setup(alert, {
      cellIndex: cell,
      idempotency: sender.idempotency,
      makeCheckInToken: (a, d, c) => `v2.${a}.${d}.${c}.`.padEnd(230, 'x'),
      maxBatchBytes: 40 * 1024, // about 190 recipients x 400 B per normal bucket = 75 KB: over
    });
    const r = await processShard(jobFor(alert), deps);
    expect(r).toMatchObject({ denseCells: 1, recipients: 12_000 });
    for (const b of sendQueue.batches('urgent')) {
      expect(b.batchKey).toMatch(/:\d+\.\d+\/16$/);
      expect(Buffer.byteLength(JSON.stringify(b))).toBeLessThanOrEqual(40 * 1024);
    }
  });
});

describe('dense cells: parts over the byte budget (review 2, P-split)', () => {
  it('late join reads a split part as planned, before and after delivery (no duplicate)', async () => {
    const alert = signed();
    const w = world(alert, { denseCellRecipients: 1_000, maxBatchBytes: 2_500 });
    add(w, 0, 12_000);
    const { batches } = await w.plan();
    const split = batches.filter((b) => /:s[01]+$/.test(b.batchKey));
    expect(split.length).toBeGreaterThan(0);
    const peek = (k: string) => w.sender.idempotency.peek(k);
    const splitDevices = split.flatMap((b) => b.recipients.map((r) => r.deviceId));
    // Planned but not yet sent: already not a late-join candidate.
    for (const d of splitDevices.slice(0, 200)) {
      expect((await plannedKeyFor(alert, CELL, d, peek)).state, d).not.toBe('absent');
    }
    await w.deliver(batches);
    noDuplicates(w.pushes);
    // Before the split marker, every device of a split part read as absent here (and late join
    // pushed it a second time).
    let absent = 0;
    for (let i = 0; i < 12_000; i += 7) {
      if ((await plannedKeyFor(alert, CELL, uid(i), peek)).state === 'absent') absent++;
    }
    expect(absent).toBe(0);
  });
});
