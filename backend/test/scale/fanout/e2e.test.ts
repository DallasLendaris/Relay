import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import type { CellIndexEntry, SendBatch } from '../../../src/scale/contracts.js';
import { bucketFor } from '../../../src/scale/fanout/hash.js';
import { MAX_MESSAGE_BYTES } from '../../../src/scale/fanout/pack.js';
import { planShards } from '../../../src/scale/fanout/planner.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { MAX_BATCHES_PER_MESSAGE } from '../../../src/scale/contracts.js';
import { countyCells, entry, fakeToken, setup, signed } from './helpers.js';

describe('end to end at memory scale', () => {
  it('200,000 index entries over a county-sized alert: every device with a target is in exactly one batch per cell', async () => {
    const cells = countyCells();
    const alert = signed({}, cells);
    const { deps, cellIndex, sendQueue, shardDispatcher } = setup(alert);
    expect(deps.alerts).toBeInstanceOf(InMemoryAlertStore);

    // 200,000 entries: ~3% have no target; ~2% are a second place of a device in the same cell;
    // ~1% belong to a device that also has a place in another cell.
    const TOTAL = 200_000;
    const expected = new Map<string, Set<string>>(); // cell -> devices with a target
    let secondPlace = 0; // second places of a device that has a target
    const add = (e: CellIndexEntry) => {
      cellIndex.add(e);
    };
    for (let i = 0; i < TOTAL; i++) {
      const cell = cells[(i * 7919) % cells.length] as string;
      let deviceId = `device-${i}`;
      let placeId = 'home';
      let withTarget = i % 33 !== 0;
      if (i % 50 === 1) {
        // second place of the previous device, same cell
        deviceId = `device-${i - 1}`;
        placeId = 'work';
        withTarget = (i - 1) % 33 !== 0;
        if (withTarget) secondPlace++;
      }
      const prevCell = cells[((i - 1) * 7919) % cells.length] as string;
      const useCell = i % 50 === 1 ? prevCell : cell;
      add(entry(useCell, deviceId, placeId, withTarget));
      if (withTarget) {
        const set = expected.get(useCell) ?? new Set<string>();
        set.add(deviceId);
        expected.set(useCell, set);
      }
    }
    expect(secondPlace).toBeGreaterThan(3_000);

    const started = performance.now();
    const planned = await planShards(
      { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
      deps,
    );
    expect(planned.refused).toBeUndefined();
    const jobs = shardDispatcher.jobs;
    expect(jobs.length).toBeGreaterThanOrEqual(2);
    const totals = { entries: 0, batches: 0, recipients: 0, skipped: 0, dupes: 0 };
    for (const job of jobs) {
      const r = await processShard(job, deps);
      totals.entries += r.entries;
      totals.batches += r.batches;
      totals.recipients += r.recipients;
      totals.skipped += r.skippedNoTarget;
      totals.dupes += r.duplicates;
    }
    const elapsedMs = performance.now() - started;

    // Every entry was read once.
    expect(totals.entries).toBe(TOTAL);
    // Parse what was actually sent, from the wire messages.
    const sent = sendQueue.batches('urgent');
    expect(sent).toHaveLength(totals.batches);
    const keys = new Set(sent.map((b) => b.batchKey));
    expect(keys.size).toBe(sent.length); // no batch key twice

    const perCell = new Map<string, Map<string, number>>();
    for (const b of sent) {
      const cell = b.batchKey.split(':')[2] as string;
      const counts = perCell.get(cell) ?? new Map<string, number>();
      for (const r of b.recipients) {
        counts.set(r.deviceId, (counts.get(r.deviceId) ?? 0) + 1);
        expect(bucketFor(r.deviceId)).toBe(Number(b.batchKey.split(':')[3]));
        expect(r.checkInToken).toBe(
          fakeToken(alert.id, r.deviceId, cell, new Date('2026-11-10T14:00:00Z')),
        );
      }
      perCell.set(cell, counts);
    }
    for (const cell of cells) {
      const want = expected.get(cell) ?? new Set<string>();
      const got = perCell.get(cell) ?? new Map<string, number>();
      expect(new Set(got.keys())).toEqual(want);
      for (const n of got.values()) expect(n).toBe(1); // exactly one batch slot per device per cell
    }

    for (const m of sendQueue.messages.urgent) {
      expect(Buffer.byteLength(m)).toBeLessThanOrEqual(MAX_MESSAGE_BYTES);
      expect((JSON.parse(m) as SendBatch[]).length).toBeLessThanOrEqual(MAX_BATCHES_PER_MESSAGE);
    }
    expect(sendQueue.batches('standard')).toHaveLength(0);
    expect(totals.dupes).toBe(secondPlace);
    expect(totals.recipients + totals.skipped + totals.dupes).toBe(TOTAL);

    console.log(
      JSON.stringify({
        e2e: '200k-county',
        cells: cells.length,
        shards: jobs.length,
        entries: totals.entries,
        recipients: totals.recipients,
        skippedNoTarget: totals.skipped,
        duplicatesDropped: totals.dupes,
        batches: totals.batches,
        messages: sendQueue.messages.urgent.length,
        elapsedMs: Math.round(elapsedMs),
      }),
    );
  }, 60_000);
});
