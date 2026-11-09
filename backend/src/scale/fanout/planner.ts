import { cellToParent } from 'h3-js';
import { errorFields } from '../../delivery/redact.js';
import { SHARD_RESOLUTION, type AlertReady, type ShardJob } from '../contracts.js';
import { checkEligibility, type EligibilityDeps, type FanoutRefusal } from './common.js';
import type { FleetScaler } from './fleet-scaler.js';
import type { ShardDispatcher } from './queues.js';

export interface PlannerDeps extends EligibilityDeps {
  shardDispatcher: ShardDispatcher;
  /** Pre-scales the sender fleet for urgent alerts. Optional; its failures never block fan-out. */
  fleetScaler?: FleetScaler;
}

export interface PlanShardsResult {
  /** Set when nothing was enqueued. 'paused' is not final: the handler retries it. */
  refused?: FanoutRefusal;
  shards: number;
  cells: number;
}

/**
 * A shard never holds more cells than this; a bigger res-4 group is split into parts.
 *
 * The math: the densest res-7 cells hold about 20,000 devices (see CELL_BUCKETS in contracts.ts),
 * so 16 cells is at most about 320,000 devices per shard worker, and typical shards are far
 * smaller. At the roughly 10,000 devices per second one 2 GB Lambda gets through (page the cell
 * index, sign a v2 token, bucket) that is about 32 s worst case inside the 60 s timeout. A whole
 * res-4 parent is up to 343 res-7 cells, which in a dense metro is 1-2M devices: well past 60 s,
 * and a retry restarted from cell 0 so the tail was never sent.
 */
export const MAX_CELLS_PER_SHARD = 16;

/**
 * Splits an alert's cells into ShardJobs: one per res-4 parent, and a res-4 group over
 * MAX_CELLS_PER_SHARD is split by its sorted cell list into parts of at most that size.
 * Pure and deterministic: the same alert always yields the same jobs with the same shardIds
 * (`<parent>:<part>`), whatever order the cells arrive in.
 */
export function buildShardJobs(alert: {
  id: string;
  sourceId: string;
  relaySeverity: ShardJob['relaySeverity'];
  area: { h3Cells: string[] };
}): ShardJob[] {
  const byParent = new Map<string, Set<string>>();
  for (const cell of alert.area.h3Cells) {
    const parent = cellToParent(cell, SHARD_RESOLUTION);
    let group = byParent.get(parent);
    if (!group) byParent.set(parent, (group = new Set()));
    group.add(cell);
  }
  const jobs: ShardJob[] = [];
  for (const parent of [...byParent.keys()].sort()) {
    const cells = [...(byParent.get(parent) ?? [])].sort();
    for (let i = 0, part = 0; i < cells.length; i += MAX_CELLS_PER_SHARD, part++) {
      jobs.push({
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        shardId: `${parent}:${part}`,
        cells: cells.slice(i, i + MAX_CELLS_PER_SHARD),
      });
    }
  }
  return jobs;
}

/**
 * Turns one AlertReady into ShardJobs and dispatches them to shard workers. Refusals (unsigned, not Actual,
 * ended, informational, stale, paused) return a result; infrastructure errors throw so the SQS
 * message is retried. Re-running for the same alert is safe: shard jobs are deterministic, and
 * the batch keys downstream make any duplicate harmless.
 */
export async function planShards(ready: AlertReady, deps: PlannerDeps): Promise<PlanShardsResult> {
  const outcome = await checkEligibility(ready, deps, 'planner');
  if (!outcome.ok) return { refused: outcome.refused, shards: 0, cells: 0 };
  const { alert, queue } = outcome;

  const jobs = buildShardJobs(alert);
  // Start the sender fleet ramping before the shards run: SQS-metric scaling alone needs 3-5
  // minutes. Urgent alerts only, and never allowed to block or fail the fan-out.
  if (queue === 'urgent' && deps.fleetScaler) {
    try {
      await deps.fleetScaler.preScale(jobs.length);
    } catch (err) {
      deps.logger.warn('fanout.prescale_failed', { alertId: alert.id, ...errorFields(err) });
    }
  }
  await deps.shardDispatcher.dispatch(jobs);
  const result = { shards: jobs.length, cells: alert.area.h3Cells.length };
  deps.logger.info('fan-out planned', {
    alertId: alert.id,
    severity: alert.relaySeverity,
    ...result,
  });
  return result;
}
