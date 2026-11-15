import { describe, expect, it } from 'vitest';
import type { CellIndexEntry } from '../../../src/scale/contracts.js';
import { planShards } from '../../../src/scale/fanout/planner.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { countyCells, entry, setup, signed } from './helpers.js';

/**
 * Devices with places in several cells of one alert area. Through the real planner and shard
 * worker, every device must be in exactly ONE batch recipient slot per alert, in its primary
 * cell, with no cross-cell duplicates.
 */

interface Dev {
  id: string;
  cells: string[];
  legacy: boolean;
}

function build(cells: string[], count: number): Dev[] {
  const devs: Dev[] = [];
  for (let i = 0; i < count; i++) {
    const n = i % 10 === 0 ? 2 + (i % 4 === 0 ? 1 : 0) + (i % 20 === 0 ? 2 : 0) : 1; // ~10% multi-cell, up to 5
    const set = new Set<string>();
    for (let k = 0; set.size < n && k < 50; k++)
      set.add(cells[(i * 7919 + k * 104729) % cells.length] as string);
    devs.push({ id: `device-${i}`, cells: [...set].sort(), legacy: false });
  }
  return devs;
}

function entriesFor(dev: Dev): CellIndexEntry[] {
  return dev.cells.map((c, n) => ({
    ...entry(c, dev.id, `place-${n}`),
    ...(dev.legacy ? {} : { deviceCells: dev.cells }),
  }));
}

async function run(alertCells: string[], devs: Dev[]) {
  const alert = signed({}, alertCells);
  const { deps, cellIndex, sendQueue, shardDispatcher } = setup(alert);
  for (const d of devs) for (const e of entriesFor(d)) cellIndex.add(e);
  await planShards(
    { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
    deps,
  );
  let skippedNonPrimary = 0;
  for (const job of shardDispatcher.jobs) {
    skippedNonPrimary += (await processShard(job, deps)).skippedNonPrimary;
  }
  const slots = new Map<string, string[]>(); // deviceId -> cells it was sent in
  for (const b of sendQueue.batches('urgent')) {
    const cell = b.batchKey.split(':')[2] as string;
    for (const r of b.recipients) slots.set(r.deviceId, [...(slots.get(r.deviceId) ?? []), cell]);
  }
  return { slots, skippedNonPrimary };
}

describe('multi-cell devices through plan and shard workers', () => {
  const all = countyCells();

  it('every device is in exactly one batch slot, in its primary cell (alert covers every cell)', async () => {
    const devs = build(all, 20_000);
    const multi = devs.filter((d) => d.cells.length > 1);
    expect(multi.length).toBeGreaterThan(1_500);
    expect(Math.max(...devs.map((d) => d.cells.length))).toBeGreaterThanOrEqual(4);

    const { slots, skippedNonPrimary } = await run(all, devs);
    expect(slots.size).toBe(devs.length);
    for (const d of devs) expect(slots.get(d.id), d.id).toEqual([d.cells[0]]);
    expect(skippedNonPrimary).toBe(multi.reduce((n, d) => n + d.cells.length - 1, 0));
  });

  it('with a partial alert area, the primary is the smallest cell INSIDE the area', async () => {
    const area = all.slice(0, Math.floor(all.length / 2));
    const inArea = new Set(area);
    const devs = build(all, 20_000);
    const { slots } = await run(area, devs);
    let covered = 0;
    for (const d of devs) {
      const shared = d.cells.filter((c) => inArea.has(c));
      if (shared.length === 0) {
        expect(slots.has(d.id), d.id).toBe(false);
        continue;
      }
      covered++;
      expect(slots.get(d.id), d.id).toEqual([shared[0]]);
    }
    expect(covered).toBeGreaterThan(5_000);
  });

  it('old entries without deviceCells are still pushed (once per cell, the old behavior)', async () => {
    const devs = build(all, 4_000).map((d) => ({ ...d, legacy: true }));
    const { slots, skippedNonPrimary } = await run(all, devs);
    expect(skippedNonPrimary).toBe(0);
    for (const d of devs) expect([...(slots.get(d.id) ?? [])].sort(), d.id).toEqual(d.cells);
  });

  it('a mix of old and new entries never misses a device', async () => {
    const devs = build(all, 8_000).map((d, i) => ({ ...d, legacy: i % 3 === 0 }));
    const { slots } = await run(all, devs);
    for (const d of devs) {
      const got = slots.get(d.id) ?? [];
      expect(got.length, d.id).toBeGreaterThanOrEqual(1);
      expect(got.length, d.id).toBeLessThanOrEqual(d.legacy ? d.cells.length : 1);
    }
  });

  it('fails open for an inconsistent entry whose deviceCells name no cell of the alert', async () => {
    const alert = signed({}, all);
    const { deps, cellIndex, sendQueue, shardDispatcher } = setup(alert);
    cellIndex.add({ ...entry(all[0] as string, 'odd'), deviceCells: ['zzzz'] });
    await planShards(
      { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
      deps,
    );
    for (const job of shardDispatcher.jobs) await processShard(job, deps);
    expect(sendQueue.batches('urgent').flatMap((b) => b.recipients.map((r) => r.deviceId))).toEqual(
      ['odd'],
    );
  });
});
