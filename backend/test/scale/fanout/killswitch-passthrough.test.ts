import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import type { ShardJob } from '../../../src/scale/contracts.js';
import { planShards } from '../../../src/scale/fanout/planner.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { countyCells, entry, setup, signed } from './helpers.js';

const CELL_A = countyCells()[0] as string;

const ready = (a: Alert) => ({
  alertId: a.id,
  sourceId: a.sourceId,
  relaySeverity: a.relaySeverity,
});

const jobWith = (a: Alert, killSwitch: unknown): ShardJob =>
  ({
    alertId: a.id,
    sourceId: a.sourceId,
    relaySeverity: a.relaySeverity,
    shardId: 'test:0',
    cells: [CELL_A],
    killSwitch,
  }) as ShardJob;

describe('planShards kill-switch pass-through (PERF-4c)', () => {
  it('reads the switch once and stamps every job with the value and read time', async () => {
    const alert = signed({ relaySeverity: 'emergency' }, countyCells());
    let reads = 0;
    const { deps, shardDispatcher } = setup(alert, {
      isDeliveryPaused: async () => (reads++, false),
    });
    await planShards(ready(alert), deps);
    expect(shardDispatcher.jobs.length).toBeGreaterThan(1);
    expect(reads).toBe(1);
    for (const j of shardDispatcher.jobs) {
      expect(j.killSwitch).toEqual({ paused: false, readAtMs: deps.clock.now().getTime() });
    }
  });

  it('dispatches nothing when paused', async () => {
    const alert = signed({ relaySeverity: 'emergency' }, countyCells());
    const { deps, shardDispatcher } = setup(alert, { isDeliveryPaused: async () => true });
    expect((await planShards(ready(alert), deps)).refused).toBe('paused');
    expect(shardDispatcher.jobs).toHaveLength(0);
  });
});

describe('processShard uses the passed kill switch (PERF-4c)', () => {
  it('uses a fresh passed value and never reads SSM', async () => {
    const alert = signed();
    let reads = 0;
    const { deps, cellIndex, sendQueue } = setup(alert, {
      isDeliveryPaused: async () => (reads++, true),
    });
    cellIndex.add(entry(CELL_A, 'dev-1'));
    const now = deps.clock.now().getTime();
    const r = await processShard(jobWith(alert, { paused: false, readAtMs: now - 5_000 }), deps);
    expect(reads).toBe(0);
    expect(r.refused).toBeUndefined();
    expect(sendQueue.batches('urgent')).toHaveLength(1);
  });

  it('honors a fresh paused value without reading SSM', async () => {
    const alert = signed();
    let reads = 0;
    const { deps } = setup(alert, { isDeliveryPaused: async () => (reads++, false) });
    const now = deps.clock.now().getTime();
    const r = await processShard(jobWith(alert, { paused: true, readAtMs: now }), deps);
    expect(r.refused).toBe('paused');
    expect(reads).toBe(0);
  });

  it.each([
    ['absent', () => undefined],
    ['older than the 30 s cache TTL', (now: number) => ({ paused: false, readAtMs: now - 31_000 })],
    ['malformed', () => ({ paused: 'no', readAtMs: 'x' })],
    ['from the future', (now: number) => ({ paused: false, readAtMs: now + 60_000 })],
  ])('reads the switch itself when the passed value is %s', async (_n, make) => {
    const alert = signed();
    let reads = 0;
    const { deps, cellIndex } = setup(alert, { isDeliveryPaused: async () => (reads++, false) });
    cellIndex.add(entry(CELL_A, 'dev-1'));
    await processShard(jobWith(alert, make(deps.clock.now().getTime())), deps);
    expect(reads).toBe(1);
  });
});
