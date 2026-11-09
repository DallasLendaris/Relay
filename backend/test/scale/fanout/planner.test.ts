import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import {
  buildShardJobs,
  MAX_CELLS_PER_SHARD,
  planShards,
} from '../../../src/scale/fanout/planner.js';
import { SHARD_RESOLUTION } from '../../../src/scale/contracts.js';
import { makeAlert } from '../../delivery/fakes.js';
import {
  cellToChildren,
  cellToParent,
  COUNTY_CENTER,
  countyCells,
  setup,
  signed,
  verifyAlert,
} from './helpers.js';

const ready = (a: Alert) => ({
  alertId: a.id,
  sourceId: a.sourceId,
  relaySeverity: a.relaySeverity,
});

describe('planShards sharding', () => {
  it('groups cells by res-4 parent and is deterministic whatever the cell order', () => {
    const cells = countyCells();
    const a = buildShardJobs({ ...makeAlert({}, cells) });
    const b = buildShardJobs({ ...makeAlert({}, [...cells].reverse()) });
    expect(b).toEqual(a);
    expect(a.length).toBeGreaterThan(1);
    // Every cell appears exactly once, under its own parent.
    expect(a.flatMap((j) => j.cells).sort()).toEqual([...cells].sort());
    for (const job of a) {
      const parents = new Set(job.cells.map((c) => cellToParent(c, SHARD_RESOLUTION)));
      expect(parents.size).toBe(1);
      expect(job.shardId).toBe(`${[...parents][0]}:0`);
      expect([...job.cells]).toEqual([...job.cells].sort());
    }
  });

  it('caps a shard at 2,000 cells and splits by the sorted cell list, with the part in the shardId', () => {
    // A res-4 cell has only 343 res-7 children, so the cap can only bite on finer cells.
    const parent = cellToParent(COUNTY_CENTER, SHARD_RESOLUTION);
    const cells = cellToChildren(parent, 8); // 2,401 cells
    const jobs = buildShardJobs(makeAlert({}, cells));
    expect(jobs).toHaveLength(2);
    expect(jobs.map((j) => j.cells.length)).toEqual([MAX_CELLS_PER_SHARD, 401]);
    expect(jobs.map((j) => j.shardId)).toEqual([`${parent}:0`, `${parent}:1`]);
    const sorted = [...cells].sort();
    expect(jobs[0]?.cells).toEqual(sorted.slice(0, 2000));
    expect(jobs[1]?.cells).toEqual(sorted.slice(2000));
    // Same split a second time, from shuffled input.
    expect(buildShardJobs(makeAlert({}, [...cells].reverse()))).toEqual(jobs);
  });

  it('enqueues the jobs with the alert identity and stored severity', async () => {
    const alert = signed({ relaySeverity: 'emergency' }, countyCells());
    const { deps, shardDispatcher } = setup(alert);
    const r = await planShards(ready(alert), deps);
    expect(r).toEqual({ shards: shardDispatcher.jobs.length, cells: 631 });
    expect(r.refused).toBeUndefined();
    for (const j of shardDispatcher.jobs) {
      expect(j).toMatchObject({
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: 'emergency',
      });
    }
  });
});

describe('planShards refusals', () => {
  it('refuses an unsigned (tampered) alert, logs the reason and enqueues nothing', async () => {
    const alert = { ...signed(), event: 'Tampered' };
    const { deps, shardDispatcher, lines } = setup(alert);
    expect(verifyAlert(alert)).toBe(false);
    const r = await planShards(ready(alert), deps);
    expect(r.refused).toBe('unsigned');
    expect(shardDispatcher.jobs).toHaveLength(0);
    expect(lines.some((l) => l.level === 'error' && l.fields?.reason === 'unsigned')).toBe(true);
  });

  it.each([
    ['not_actual', { status: 'Test' as const }],
    ['inactive', { state: 'cancelled' as const }],
    ['expired', { expires: '2026-11-10T10:00:00Z' }],
  ])('refuses %s', async (reason, over) => {
    const alert = signed(over as Partial<Alert>);
    const { deps, shardDispatcher } = setup(alert);
    const r = await planShards(ready(alert), deps);
    expect(r.refused).toBe(reason);
    expect(shardDispatcher.jobs).toHaveLength(0);
  });

  it('returns paused without enqueuing when the kill switch is on', async () => {
    const alert = signed();
    const { deps, shardDispatcher } = setup(alert, { isDeliveryPaused: async () => true });
    const r = await planShards(ready(alert), deps);
    expect(r.refused).toBe('paused');
    expect(shardDispatcher.jobs).toHaveLength(0);
  });

  it('does not push a statement (informational)', async () => {
    const alert = signed({ relaySeverity: 'statement' });
    const { deps, shardDispatcher } = setup(alert);
    const r = await planShards(ready(alert), deps);
    expect(r.refused).toBe('informational');
    expect(shardDispatcher.jobs).toHaveLength(0);
  });

  it('ignores a stale AlertReady whose sourceId is not the stored alert', async () => {
    const alert = signed({ sourceId: 'urn:oid:2' });
    const { deps, shardDispatcher, lines } = setup(alert);
    const r = await planShards({ ...ready(alert), sourceId: 'urn:oid:1' }, deps);
    expect(r.refused).toBe('stale');
    expect(shardDispatcher.jobs).toHaveLength(0);
    expect(lines.some((l) => l.msg === 'ignoring stale fan-out message')).toBe(true);
  });

  it('throws on infrastructure errors so the message retries', async () => {
    const alert = signed();
    const { deps } = setup(alert, {
      shardDispatcher: {
        dispatch: async () => {
          throw new Error('sqs down');
        },
      },
    });
    await expect(planShards(ready(alert), deps)).rejects.toThrow('sqs down');
    const { deps: d2 } = setup(alert, {
      alerts: {
        get: async () => {
          throw new Error('db down');
        },
      },
    });
    await expect(planShards(ready(alert), d2)).rejects.toThrow('db down');
  });

  it('throws when the alert is missing (not yet visible), so the message retries', async () => {
    const { deps } = setup(signed());
    await expect(
      planShards({ alertId: 'nope', sourceId: 'x', relaySeverity: 'warning' }, deps),
    ).rejects.toThrow('not found');
  });
});
