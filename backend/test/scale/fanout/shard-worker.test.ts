import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { alertEndTime } from '../../../src/delivery/eligibility.js';
import { CELL_BUCKETS, type SendBatch, type ShardJob } from '../../../src/scale/contracts.js';
import { bucketFor } from '../../../src/scale/fanout/hash.js';
import { MAX_MESSAGE_BYTES } from '../../../src/scale/fanout/pack.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { countyCells, entry, fakeToken, setup, signed } from './helpers.js';

const CELL_A = countyCells()[0] as string;
const CELL_B = countyCells()[1] as string;

const jobFor = (a: Alert, cells: string[]): ShardJob => ({
  alertId: a.id,
  sourceId: a.sourceId,
  relaySeverity: a.relaySeverity,
  shardId: 'test:0',
  cells,
});

const keyOf = (a: Alert, cell: string, bucket: number) =>
  `${a.id}:${a.relaySeverity}:${cell}:${bucket}`;

describe('processShard', () => {
  it('makes one batch per non-empty (cell, bucket) with the contract batch key', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert);
    for (let i = 0; i < 500; i++) cellIndex.add(entry(CELL_A, `dev-${i}`));
    const r = await processShard(jobFor(alert, [CELL_A]), deps);

    const batches = sendQueue.batches('urgent');
    expect(r).toMatchObject({ cells: 1, entries: 500, recipients: 500, batches: batches.length });
    expect(sendQueue.batches('standard')).toHaveLength(0);
    for (const b of batches) {
      const bucket = bucketFor(b.recipients[0]?.deviceId as string);
      expect(b.batchKey).toBe(keyOf(alert, CELL_A, bucket));
      expect(b.attempt).toBe(0);
      expect(b.sourceId).toBe(alert.sourceId);
      for (const rcp of b.recipients) expect(bucketFor(rcp.deviceId)).toBe(bucket);
    }
    expect(new Set(batches.map((b) => b.batchKey)).size).toBe(batches.length);
    // 500 devices over 64 buckets: nearly every bucket is non-empty.
    expect(batches.length).toBeGreaterThan(CELL_BUCKETS * 0.9);
  });

  it('routes by severity: warning and emergency to urgent, watch and advisory to standard', async () => {
    for (const [sev, queue] of [
      ['emergency', 'urgent'],
      ['warning', 'urgent'],
      ['watch', 'standard'],
      ['advisory', 'standard'],
    ] as const) {
      const alert = signed({ relaySeverity: sev });
      const { deps, cellIndex, sendQueue } = setup(alert);
      cellIndex.add(entry(CELL_A, 'dev-1'));
      await processShard(jobFor(alert, [CELL_A]), deps);
      const other = queue === 'urgent' ? 'standard' : 'urgent';
      expect(sendQueue.batches(queue)).toHaveLength(1);
      expect(sendQueue.batches(other)).toHaveLength(0);
    }
  });

  it('makes tokens bound to alert, device, cell and the alert end time', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert);
    cellIndex.add(entry(CELL_A, 'dev-1'));
    await processShard(jobFor(alert, [CELL_A]), deps);
    const rcp = sendQueue.batches('urgent')[0]?.recipients[0];
    expect(rcp?.checkInToken).toBe(fakeToken(alert.id, 'dev-1', CELL_A, alertEndTime(alert)));
  });

  it('skips entries with no target and dedupes a device with two places in one cell', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert);
    cellIndex.add(entry(CELL_A, 'dev-1', 'home'));
    cellIndex.add(entry(CELL_A, 'dev-1', 'work'));
    cellIndex.add(entry(CELL_A, 'dev-2', 'home', false));
    cellIndex.add(entry(CELL_A, 'dev-3', 'home'));
    const r = await processShard(jobFor(alert, [CELL_A]), deps);
    expect(r).toMatchObject({ entries: 4, skippedNoTarget: 1, duplicates: 1, recipients: 2 });
    const ids = sendQueue
      .batches('urgent')
      .flatMap((b) => b.recipients.map((x) => x.deviceId))
      .sort();
    expect(ids).toEqual(['dev-1', 'dev-3']);
  });

  it('pages the cell index', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert, { pageSize: 100 });
    for (let i = 0; i < 450; i++) cellIndex.add(entry(CELL_A, `dev-${i}`));
    const r = await processShard(jobFor(alert, [CELL_A]), deps);
    expect(cellIndex.pages).toBe(5);
    expect(r.recipients).toBe(450);
    expect(sendQueue.batches('urgent').flatMap((b) => b.recipients)).toHaveLength(450);
  });

  it('processes up to 16 cells at a time, no more', async () => {
    const alert = signed();
    const cells = countyCells().slice(0, 60);
    const { deps, cellIndex } = setup(alert);
    for (const c of cells) cellIndex.add(entry(c, `dev-${c}`));
    await processShard(jobFor(alert, cells), deps);
    expect(cellIndex.maxActive).toBe(16);
  });

  it('gives a device in two different cells two batches (the documented cross-shard gap)', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert);
    cellIndex.add(entry(CELL_A, 'dev-1', 'home'));
    cellIndex.add(entry(CELL_B, 'dev-1', 'work'));
    await processShard(jobFor(alert, [CELL_A, CELL_B]), deps);
    const keys = sendQueue.batches('urgent').map((b) => b.batchKey);
    expect(keys.sort()).toEqual(
      [keyOf(alert, CELL_A, bucketFor('dev-1')), keyOf(alert, CELL_B, bucketFor('dev-1'))].sort(),
    );
  });

  it('keeps batch keys and content of untouched buckets identical when devices change elsewhere', async () => {
    const alert = signed();
    const run = async (extra: string[], removed: string[] = []) => {
      const { deps, cellIndex, sendQueue } = setup(alert);
      for (let i = 0; i < 400; i++) cellIndex.add(entry(CELL_A, `dev-${i}`));
      for (const id of extra) cellIndex.add(entry(CELL_A, id));
      for (const id of removed) cellIndex.remove(CELL_A, id);
      await processShard(jobFor(alert, [CELL_A]), deps);
      return new Map(sendQueue.batches('urgent').map((b) => [b.batchKey, b]));
    };
    const base = await run([]);
    const again = await run([]);
    expect(again).toEqual(base); // stable across runs

    const newcomer = 'newcomer-device';
    const added = await run([newcomer]);
    const removed = await run([], ['dev-7']);
    const newBucket = keyOf(alert, CELL_A, bucketFor(newcomer));
    const goneBucket = keyOf(alert, CELL_A, bucketFor('dev-7'));
    for (const [key, batch] of base) {
      if (key !== newBucket) expect(added.get(key)).toEqual(batch);
      if (key !== goneBucket) expect(removed.get(key)).toEqual(batch);
    }
    expect(new Set(added.keys()).size).toBeGreaterThanOrEqual(base.size);
    expect(
      added
        .get(newBucket)
        ?.recipients.map((r) => r.deviceId)
        .includes(newcomer),
    ).toBe(true);
  });

  it('re-running a shard after a partial failure enqueues the same keys again', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert, { flushBatches: 1 });
    for (const c of countyCells().slice(0, 6)) {
      for (let i = 0; i < 100; i++) cellIndex.add(entry(c, `dev-${c}-${i}`));
    }
    const cells = countyCells().slice(0, 6);
    // First attempt dies on the third send.
    let sends = 0;
    const real = deps.sendQueue;
    const flaky = {
      enqueue: async (q: 'urgent' | 'standard', b: SendBatch[]) => {
        if (++sends === 3) throw new Error('worker died');
        return real.enqueue(q, b);
      },
    };
    await expect(processShard(jobFor(alert, cells), { ...deps, sendQueue: flaky })).rejects.toThrow(
      'worker died',
    );
    await new Promise((r) => setTimeout(r, 50)); // in-flight cells stop after the failure
    const afterFirst = sendQueue.batches('urgent').map((b) => b.batchKey);
    expect(afterFirst.length).toBeGreaterThan(0);

    await processShard(jobFor(alert, cells), deps); // SQS retry
    const all = sendQueue.batches('urgent').map((b) => b.batchKey);
    // Every key from the failed attempt is re-sent with the same key; the sender claims dedupe them.
    for (const k of afterFirst) expect(all.filter((x) => x === k)).toHaveLength(2);
    // And the full set is covered exactly once by the successful run.
    const second = all.slice(afterFirst.length);
    expect(new Set(second).size).toBe(second.length);
  });

  it('keeps every message under 240 KB, even for dense cells', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert);
    for (let i = 0; i < 30_000; i++) cellIndex.add(entry(CELL_A, `dev-${i}`));
    for (let i = 0; i < 40; i++) cellIndex.add(entry(CELL_B, `dev-b-${i}`));
    await processShard(jobFor(alert, [CELL_A, CELL_B]), deps);
    for (const m of sendQueue.messages.urgent) {
      expect(Buffer.byteLength(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
    expect(sendQueue.batches('urgent').flatMap((b) => b.recipients)).toHaveLength(30_040);
  });

  it('splits a single bucket too big for one message, with deterministic keys', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue, lines } = setup(alert);
    // Find ids that all land in bucket 5: one oversized bucket.
    let n = 0;
    const ids: string[] = [];
    while (ids.length < 1_800) {
      const id = `big-${n++}`;
      if (bucketFor(id) === 5) ids.push(id);
    }
    for (const id of ids) cellIndex.add(entry(CELL_A, id));
    await processShard(jobFor(alert, [CELL_A]), deps);
    const batches = sendQueue.batches('urgent');
    expect(batches.length).toBeGreaterThan(1);
    expect(batches.every((b) => b.batchKey.startsWith(`${keyOf(alert, CELL_A, 5)}:s`))).toBe(true);
    expect(batches.flatMap((b) => b.recipients)).toHaveLength(1_800);
    for (const m of sendQueue.messages.urgent) {
      expect(Buffer.byteLength(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
    }
    expect(lines.some((l) => l.msg === 'splitting oversize batch')).toBe(true);
  });

  describe('refusals', () => {
    const cases: Array<[string, Partial<Alert>]> = [
      ['not_actual', { status: 'Test' }],
      ['inactive', { state: 'cancelled' }],
      ['expired', { expires: '2026-11-10T10:00:00Z' }],
    ];
    it.each(cases)('re-checks eligibility: %s', async (reason, over) => {
      const alert = signed(over);
      const { deps, cellIndex, sendQueue } = setup(alert);
      cellIndex.add(entry(CELL_A, 'dev-1'));
      const r = await processShard(jobFor(alert, [CELL_A]), deps);
      expect(r.refused).toBe(reason);
      expect(sendQueue.messages.urgent).toHaveLength(0);
      expect(cellIndex.pages).toBe(0);
    });

    it('refuses an unsigned alert, a paused system and a stale job', async () => {
      const alert = signed();
      const tampered = { ...alert, event: 'Edited' };
      const a = setup(tampered);
      expect((await processShard(jobFor(tampered, [CELL_A]), a.deps)).refused).toBe('unsigned');

      const b = setup(alert, { isDeliveryPaused: async () => true });
      expect((await processShard(jobFor(alert, [CELL_A]), b.deps)).refused).toBe('paused');

      const c = setup(alert);
      const stale = { ...jobFor(alert, [CELL_A]), sourceId: 'urn:oid:old' };
      expect((await processShard(stale, c.deps)).refused).toBe('stale');
      for (const s of [a, b, c]) expect(s.sendQueue.messages.urgent).toHaveLength(0);
    });

    it('throws when the cell index fails, so the shard is retried', async () => {
      const alert = signed();
      const { deps } = setup(alert, {
        cellIndex: {
          listForCell: async () => {
            throw new Error('dynamo throttled');
          },
        },
      });
      await expect(processShard(jobFor(alert, [CELL_A]), deps)).rejects.toThrow('throttled');
    });
  });
});
