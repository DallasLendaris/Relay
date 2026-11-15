import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import type { ShardJob } from '../../../src/scale/contracts.js';
import { buildPlannerHandler, buildShardHandler } from '../../../src/scale/fanout/handlers.js';
import { createPausedConsumer } from '../../../src/scale/fanout/paused-consumer.js';
import {
  MemoryPausedQueue,
  PARK_MAX_AGE_MS,
  parseParked,
  type ParkedMessage,
} from '../../../src/scale/fanout/paused-queue.js';
import { MemoryPlanQueue, type ShardDispatcher } from '../../../src/scale/fanout/queues.js';
import { captureLogger, countyCells, entry, latLngToCell, setup, signed } from './helpers.js';

const sqsOf = (...bodies: unknown[]): SQSEvent =>
  ({
    Records: bodies.map((b, i) => ({ messageId: `m${i}`, body: JSON.stringify(b) })),
  }) as SQSEvent;

/** Takes everything parked so far, like SQS delivering the (delayed) messages. */
const deliver = (q: MemoryPausedQueue): ParkedMessage[] => q.messages.splice(0);

describe('REL-2: a paused fan-out recovers by itself', () => {
  // Six shards in six separate res-4 areas, one device each.
  const cells = [
    [37.3, -121.9],
    [34.05, -118.25],
    [32.7, -117.15],
    [38.58, -121.49],
    [36.74, -119.78],
    [40.8, -124.16],
  ].map(([lat, lng]) => latLngToCell(lat as number, lng as number, 7));

  function world() {
    const alert = signed({}, cells);
    let paused = true;
    let now = new Date('2026-11-10T10:05:00Z');
    const s = setup(alert, {
      isDeliveryPaused: async () => paused,
      clock: { now: () => now },
    });
    cells.forEach((c, i) => s.cellIndex.add(entry(c, `dev-${i}`)));
    const jobs: ShardJob[] = cells.map((c, i) => ({
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      shardId: `shard:${i}`,
      cells: [c],
    }));
    return {
      alert,
      s,
      jobs,
      parked: new MemoryPausedQueue(),
      setPaused: (v: boolean) => (paused = v),
      advance: (ms: number) => (now = new Date(now.getTime() + ms)),
      now: () => now,
    };
  }

  it('pause -> unpause: every shard is processed exactly once, with no throw and no redrive', async () => {
    const w = world();
    const shard = buildShardHandler(w.s.deps, undefined, w.parked);

    // Paused: no invoke throws (so Lambda's async retry / DLQ never starts), nothing is sent.
    for (const job of w.jobs) await expect(shard(job)).resolves.toBeUndefined();
    expect(w.s.sendQueue.batches('urgent')).toHaveLength(0);
    expect(w.parked.messages).toHaveLength(6);

    // The consumer's re-dispatch goes through the shard handler, like the real Lambda invoke.
    const redispatched: string[] = [];
    const dispatcher: ShardDispatcher = {
      dispatch: async (jobs) => {
        for (const j of jobs) {
          redispatched.push(j.shardId);
          await shard(j);
        }
      },
    };
    const consumer = createPausedConsumer({
      isDeliveryPaused: w.s.deps.isDeliveryPaused,
      paused: w.parked,
      shards: dispatcher,
      plans: new MemoryPlanQueue(),
      now: w.now,
      logger: w.s.deps.logger,
    });

    // Still paused for a few rounds: the messages are parked again, nothing is dispatched.
    for (let round = 0; round < 3; round++) {
      w.advance(60_000);
      const res = await consumer(sqsOf(...deliver(w.parked)));
      expect(res.batchItemFailures).toEqual([]);
      expect(w.parked.messages).toHaveLength(6);
      expect(redispatched).toEqual([]);
    }
    expect(w.s.sendQueue.batches('urgent')).toHaveLength(0);

    // Unpause: the next round re-dispatches every shard, once.
    w.setPaused(false);
    w.advance(60_000);
    const res = await consumer(sqsOf(...deliver(w.parked)));
    expect(res.batchItemFailures).toEqual([]);
    expect([...redispatched].sort()).toEqual(w.jobs.map((j) => j.shardId).sort());
    expect(w.parked.messages).toHaveLength(0);

    const sent = w.s.sendQueue
      .batches('urgent')
      .flatMap((b) => b.recipients.map((r) => r.deviceId));
    expect(sent.sort()).toEqual(['dev-0', 'dev-1', 'dev-2', 'dev-3', 'dev-4', 'dev-5']);
  });

  it('keeps parkedAt across re-parks and gives up after 24 hours (failure -> DLQ)', async () => {
    const w = world();
    const { logger, lines } = captureLogger();
    const shard = buildShardHandler(w.s.deps, undefined, w.parked);
    await shard(w.jobs[0] as ShardJob);
    const firstParkedAt = w.parked.messages[0]?.parkedAt;

    const consumer = createPausedConsumer({
      isDeliveryPaused: w.s.deps.isDeliveryPaused,
      paused: w.parked,
      shards: { dispatch: async () => {} },
      plans: new MemoryPlanQueue(),
      now: w.now,
      logger,
    });
    w.advance(PARK_MAX_AGE_MS - 1_000);
    expect((await consumer(sqsOf(...deliver(w.parked)))).batchItemFailures).toEqual([]);
    expect(w.parked.messages[0]?.parkedAt).toBe(firstParkedAt);

    w.advance(2_000);
    const res = await consumer(sqsOf(...deliver(w.parked)));
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm0' }]);
    expect(w.parked.messages).toHaveLength(0);
    expect(lines.some((l) => l.level === 'error' && l.msg === 'fanout.paused_gave_up')).toBe(true);
  });

  it('a failed re-dispatch is reported for SQS to retry, not lost', async () => {
    const w = world();
    w.setPaused(false);
    const consumer = createPausedConsumer({
      isDeliveryPaused: async () => false,
      paused: w.parked,
      shards: {
        dispatch: async () => {
          throw new Error('lambda throttled');
        },
      },
      plans: new MemoryPlanQueue(),
      now: w.now,
      logger: w.s.deps.logger,
    });
    const res = await consumer(
      sqsOf({ kind: 'shard', job: w.jobs[1], parkedAt: w.now().toISOString() }),
    );
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm0' }]);
  });

  it('a paused AlertReady is parked by the planner and re-published to the plan queue', async () => {
    const alert = signed({}, countyCells());
    const msg = { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: 'warning' };
    let paused = true;
    const s = setup(alert, { isDeliveryPaused: async () => paused });
    const parked = new MemoryPausedQueue();
    const res = await buildPlannerHandler(s.deps, undefined, parked)(sqsOf(msg));
    expect(res.batchItemFailures).toEqual([]);
    expect(s.shardDispatcher.jobs).toHaveLength(0);
    expect(parked.messages).toHaveLength(1);
    expect(parked.messages[0]?.kind).toBe('plan');

    const plans = new MemoryPlanQueue();
    const consumer = createPausedConsumer({
      isDeliveryPaused: s.deps.isDeliveryPaused,
      paused: parked,
      shards: { dispatch: async () => {} },
      plans,
      now: () => new Date(s.deps.clock.now().getTime() + 120_000),
      logger: s.deps.logger,
    });
    await consumer(sqsOf(...deliver(parked)));
    expect(plans.messages).toHaveLength(0);
    paused = false;
    await consumer(sqsOf(...deliver(parked)));
    expect(plans.messages).toEqual([msg]);
  });

  it('without a parking queue the old behavior is kept (direct invoke throws)', async () => {
    const w = world();
    await expect(buildShardHandler(w.s.deps)(w.jobs[0] as ShardJob)).rejects.toThrow('paused');
  });

  it('a failure to park still throws, so the async retry and DLQ remain the safety net', async () => {
    const w = world();
    const broken = {
      park: async () => {
        throw new Error('sqs down');
      },
    };
    await expect(
      buildShardHandler(w.s.deps, undefined, broken)(w.jobs[0] as ShardJob),
    ).rejects.toThrow('sqs down');
  });

  it('parseParked rejects junk', () => {
    expect(parseParked('nope')).toBeNull();
    expect(parseParked(JSON.stringify({ kind: 'shard', job: {}, parkedAt: 'x' }))).toBeNull();
    expect(
      parseParked(JSON.stringify({ kind: 'other', parkedAt: new Date().toISOString() })),
    ).toBeNull();
  });
});
