import { DescribeServicesCommand, UpdateServiceCommand } from '@aws-sdk/client-ecs';
import { StartMessageMoveTaskCommand } from '@aws-sdk/client-sqs';
import type { Alert } from '@relay/shared';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import type { ShardJob } from '../../../src/scale/contracts.js';
import { CachedAlertReader } from '../../../src/scale/fanout/alert-cache.js';
import {
  EcsFleetScaler,
  fleetScalerFromEnv,
  targetTasks,
} from '../../../src/scale/fanout/fleet-scaler.js';
import { buildPlannerHandler, buildShardHandler } from '../../../src/scale/fanout/handlers.js';
import type { ResumableShardJob } from '../../../src/scale/fanout/messages.js';
import { planShards } from '../../../src/scale/fanout/planner.js';
import { redriveSendDlqs } from '../../../src/scale/fanout/send-redrive.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { MemoryShardDispatcher } from '../../../src/scale/fanout/queues.js';
import { captureLogger, countyCells, entry, setup, signed } from './helpers.js';

const ready = (a: Alert) => ({
  alertId: a.id,
  sourceId: a.sourceId,
  relaySeverity: a.relaySeverity,
});

const cfg = { maxTasks: 100, scalePerShard: 20_000, recipientsPerTaskPerMin: 18_000 };

describe('targetTasks', () => {
  it('is ceil(shards x per-shard / per-task rate)', () => {
    // 30 shards x 20,000 = 600,000 recipients / 18,000 per task per minute = 33.3 -> 34.
    expect(targetTasks(30, 1, cfg)).toBe(34);
    expect(targetTasks(1, 1, cfg)).toBe(2);
  });
  it('clamps to maxTasks', () => {
    expect(targetTasks(1_000, 1, cfg)).toBe(100);
  });
  it('never lowers the count, even above max or when the estimate is smaller', () => {
    expect(targetTasks(1, 40, cfg)).toBe(40);
    expect(targetTasks(1_000, 120, cfg)).toBe(120);
    expect(targetTasks(0, 10, cfg)).toBe(10);
  });
  it('falls back to the current count on a nonsense estimate', () => {
    expect(targetTasks(5, 7, { ...cfg, recipientsPerTaskPerMin: 0 })).toBe(7);
  });
});

function fakeEcs(desired: number | undefined, failUpdate = false) {
  const updates: number[] = [];
  const ecs = {
    send: async (cmd: unknown) => {
      if (cmd instanceof DescribeServicesCommand) {
        return { services: desired === undefined ? [] : [{ desiredCount: desired }] };
      }
      if (cmd instanceof UpdateServiceCommand) {
        if (failUpdate) throw new Error('throttled');
        updates.push(cmd.input.desiredCount as number);
        expect(cmd.input.service).toBe('svc');
        expect(cmd.input.cluster).toBe('cluster-arn');
        return {};
      }
      throw new Error('unexpected command');
    },
  };
  return { ecs, updates };
}
const scalerConfig = { cluster: 'cluster-arn', service: 'svc', ...cfg };

describe('EcsFleetScaler', () => {
  it('raises desiredCount to the target', async () => {
    const { ecs, updates } = fakeEcs(10);
    const { logger } = captureLogger();
    const r = await new EcsFleetScaler(ecs, scalerConfig, logger).preScale(30);
    expect(r).toEqual({ current: 10, target: 34, updated: true });
    expect(updates).toEqual([34]);
  });
  it('does not call UpdateService when the fleet is already big enough', async () => {
    const { ecs, updates } = fakeEcs(50);
    const { logger } = captureLogger();
    const r = await new EcsFleetScaler(ecs, scalerConfig, logger).preScale(30);
    expect(r).toEqual({ current: 50, target: 50, updated: false });
    expect(updates).toEqual([]);
  });
  it('throws when the service is not found', async () => {
    const { ecs } = fakeEcs(undefined);
    await expect(
      new EcsFleetScaler(ecs, scalerConfig, captureLogger().logger).preScale(3),
    ).rejects.toThrow('not found');
  });
  it('is built from env only when fully configured and max > 0', () => {
    const { ecs } = fakeEcs(1);
    const { logger } = captureLogger();
    const env = {
      SENDER_CLUSTER_ARN: 'c',
      SENDER_SERVICE_NAME: 's',
      SENDER_MAX_TASKS: '100',
      SENDER_SCALE_PER_SHARD: '20000',
      SENDER_RECIPIENTS_PER_TASK_PER_MIN: '18000',
    };
    expect(fleetScalerFromEnv(env, ecs, logger)).toBeInstanceOf(EcsFleetScaler);
    expect(fleetScalerFromEnv({ ...env, SENDER_MAX_TASKS: '0' }, ecs, logger)).toBeUndefined();
    expect(fleetScalerFromEnv({ ...env, SENDER_SERVICE_NAME: '' }, ecs, logger)).toBeUndefined();
    expect(fleetScalerFromEnv({}, ecs, logger)).toBeUndefined();
  });
});

describe('planner pre-scale', () => {
  it('pre-scales for an urgent alert from the number of shards, before dispatching', async () => {
    const alert = signed({ relaySeverity: 'warning' }, countyCells());
    const calls: number[] = [];
    const { deps, shardDispatcher } = setup(alert, {
      fleetScaler: {
        preScale: async (n) => {
          calls.push(n);
          expect(shardDispatcher.jobs).toHaveLength(0); // scaling starts first
          return { current: 1, target: 2, updated: true };
        },
      },
    });
    const r = await planShards(ready(alert), deps);
    expect(calls).toEqual([r.shards]);
    expect(r.shards).toBeGreaterThan(0);
  });

  it('does not pre-scale for the standard queue or a refused alert', async () => {
    let calls = 0;
    const scaler = {
      preScale: async () => {
        calls++;
        return { current: 1, target: 1, updated: false };
      },
    };
    const watch = signed({ relaySeverity: 'watch' }, countyCells());
    await planShards(ready(watch), setup(watch, { fleetScaler: scaler }).deps);
    const paused = signed({}, countyCells());
    await planShards(
      ready(paused),
      setup(paused, { fleetScaler: scaler, isDeliveryPaused: async () => true }).deps,
    );
    expect(calls).toBe(0);
  });

  it('logs and carries on when pre-scaling fails: fan-out is never blocked', async () => {
    const alert = signed({ relaySeverity: 'emergency' }, countyCells());
    const { deps, shardDispatcher, lines } = setup(alert, {
      fleetScaler: {
        preScale: async () => {
          throw new Error('ecs down');
        },
      },
    });
    const r = await planShards(ready(alert), deps);
    expect(r.shards).toBe(shardDispatcher.jobs.length);
    expect(shardDispatcher.jobs.length).toBeGreaterThan(0);
    expect(lines.some((l) => l.level === 'warn' && l.msg === 'fanout.prescale_failed')).toBe(true);
  });
});

const CELLS = countyCells().slice(0, 6);
const jobFor = (a: Alert, cells: string[], startAt?: number): ResumableShardJob => ({
  alertId: a.id,
  sourceId: a.sourceId,
  relaySeverity: a.relaySeverity,
  shardId: 'p:0',
  cells,
  ...(startAt === undefined ? {} : { startAt }),
});

describe('shard continuation', () => {
  function world() {
    const alert = signed();
    const s = setup(alert);
    for (const cell of CELLS)
      for (let i = 0; i < 20; i++) s.cellIndex.add(entry(cell, `${cell}-${i}`));
    return { alert, ...s };
  }
  /** remainingMs that reports plenty of time for `ok` calls, then 5 s. */
  const clockRunningOut = (ok: number) => {
    let n = 0;
    return () => (n++ < ok ? 55_000 : 5_000);
  };

  it('dispatches a continuation at the next cell when time runs short, then resumes with no skips or repeats', async () => {
    const { alert, deps, sendQueue } = world();
    const continuation = new MemoryShardDispatcher();
    const first = await processShard(jobFor(alert, CELLS), {
      ...deps,
      cellConcurrency: 1,
      remainingMs: clockRunningOut(2),
      continuation,
    });
    expect(first.continuedAt).toBe(2);
    expect(continuation.jobs).toHaveLength(1);
    const next = continuation.jobs[0] as ResumableShardJob;
    expect(next).toMatchObject({ shardId: 'p:0', cells: CELLS, startAt: 2 });
    const cellsOfFirst = new Set(sendQueue.batches('urgent').map((b) => b.batchKey.split(':')[2]));
    expect([...cellsOfFirst].sort()).toEqual(CELLS.slice(0, 2).sort());

    // The continuation (with plenty of time) finishes the rest.
    const second = await processShard(next, { ...deps, remainingMs: () => 55_000, continuation });
    expect(second.continuedAt).toBeUndefined();
    const keys = sendQueue.batches('urgent').map((b) => b.batchKey);
    expect(new Set(keys).size).toBe(keys.length); // no cell sent twice
    const cellsSent = new Set(keys.map((k) => k.split(':')[2]));
    expect([...cellsSent].sort()).toEqual([...CELLS].sort()); // no cell skipped
    expect(sendQueue.batches('urgent').reduce((n, b) => n + b.recipients.length, 0)).toBe(
      CELLS.length * 20,
    );
    expect(continuation.jobs).toHaveLength(1);
  });

  it('chains: several short runs together cover every cell exactly once', async () => {
    const { alert, deps, sendQueue } = world();
    const continuation = new MemoryShardDispatcher();
    let job: ResumableShardJob | undefined = jobFor(alert, CELLS);
    let runs = 0;
    while (job) {
      const before = continuation.jobs.length;
      await processShard(job, {
        ...deps,
        cellConcurrency: 1,
        remainingMs: clockRunningOut(1), // one cell per run
        continuation,
      });
      job = continuation.jobs.length > before ? continuation.jobs.at(-1) : undefined;
      runs++;
    }
    expect(runs).toBe(CELLS.length);
    const keys = sendQueue.batches('urgent').map((b) => b.batchKey);
    expect(new Set(keys).size).toBe(keys.length);
    expect(new Set(keys.map((k) => k.split(':')[2])).size).toBe(CELLS.length);
  });

  it('with concurrency > 1 every started cell finishes and startAt is exactly the first undone cell', async () => {
    const { alert, deps, sendQueue } = world();
    const continuation = new MemoryShardDispatcher();
    const r = await processShard(jobFor(alert, CELLS), {
      ...deps,
      cellConcurrency: 3,
      remainingMs: clockRunningOut(4),
      continuation,
    });
    const startAt = (continuation.jobs[0] as ResumableShardJob).startAt as number;
    expect(r.continuedAt).toBe(startAt);
    const done = new Set(sendQueue.batches('urgent').map((b) => b.batchKey.split(':')[2]));
    expect([...done].sort()).toEqual(CELLS.slice(0, startAt).sort());
  });

  it('starts at startAt and does nothing extra when time is fine', async () => {
    const { alert, deps, sendQueue } = world();
    const continuation = new MemoryShardDispatcher();
    const r = await processShard(jobFor(alert, CELLS, 4), {
      ...deps,
      remainingMs: () => 55_000,
      continuation,
    });
    expect(r.continuedAt).toBeUndefined();
    expect(continuation.jobs).toHaveLength(0);
    const done = new Set(sendQueue.batches('urgent').map((b) => b.batchKey.split(':')[2]));
    expect([...done].sort()).toEqual(CELLS.slice(4).sort());
  });

  it('throws when the continuation cannot be dispatched, so the job is retried', async () => {
    const { alert, deps } = world();
    await expect(
      processShard(jobFor(alert, CELLS), {
        ...deps,
        cellConcurrency: 1,
        remainingMs: clockRunningOut(1),
        continuation: {
          dispatch: async () => {
            throw new Error('lambda throttled');
          },
        },
      }),
    ).rejects.toThrow('throttled');
  });

  it('uses the 10 s default threshold', async () => {
    const { alert, deps } = world();
    const continuation = new MemoryShardDispatcher();
    // 10,000 ms left is not under the threshold; 9,999 is.
    await processShard(jobFor(alert, CELLS), { ...deps, remainingMs: () => 10_000, continuation });
    expect(continuation.jobs).toHaveLength(0);
    await processShard(jobFor(alert, CELLS), { ...deps, remainingMs: () => 9_999, continuation });
    expect(continuation.jobs).toHaveLength(1);
    expect((continuation.jobs[0] as ResumableShardJob).startAt).toBe(0);
  });
});

const sqsEvent = (...bodies: unknown[]): SQSEvent =>
  ({
    Records: bodies.map((b, i) => ({ messageId: `m${i}`, body: JSON.stringify(b) })),
  }) as SQSEvent;

describe("'unsigned' is retried, not final", () => {
  it('planner: refreshes the keys once; still unsigned -> the message is retried and logged', async () => {
    const alert = signed({}, countyCells());
    const stale = setup(alert, { verifyAlert: () => false });
    let refreshes = 0;
    const res = await buildPlannerHandler(stale.deps, async () => {
      refreshes++;
      return stale.deps;
    })(sqsEvent(ready(alert)));
    expect(refreshes).toBe(1);
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm0' }]);
    expect(stale.shardDispatcher.jobs).toHaveLength(0);
    expect(
      stale.lines.some((l) => l.level === 'error' && l.msg === 'fanout.signature_failed'),
    ).toBe(true);
  });

  it('planner: retries even when no refresh is available, and succeeds once the key is loaded', async () => {
    const alert = signed({}, countyCells());
    const stale = setup(alert, { verifyAlert: () => false });
    const res = await buildPlannerHandler(stale.deps)(sqsEvent(ready(alert)));
    expect(res.batchItemFailures).toHaveLength(1);
    const fresh = setup(alert);
    const ok = await buildPlannerHandler(
      stale.deps,
      async () => fresh.deps,
    )(sqsEvent(ready(alert)));
    expect(ok.batchItemFailures).toEqual([]);
    expect(fresh.shardDispatcher.jobs.length).toBeGreaterThan(0);
  });

  it('shard (async invoke): throws so Lambda retries; from SQS it reports the item', async () => {
    const alert = signed();
    const cell = CELLS[0] as string;
    const stale = setup(alert, { verifyAlert: () => false });
    stale.cellIndex.add(entry(cell, 'dev-1'));
    const job: ShardJob = jobFor(alert, [cell]);
    let refreshes = 0;
    const refresh = async () => {
      refreshes++;
      return stale.deps;
    };
    await expect(buildShardHandler(stale.deps, refresh)(job)).rejects.toThrow('signature');
    expect(refreshes).toBe(1);
    expect(stale.sendQueue.batches('urgent')).toHaveLength(0);
    expect(stale.lines.some((l) => l.msg === 'fanout.signature_failed')).toBe(true);
    const res = await buildShardHandler(stale.deps, refresh)(sqsEvent(job));
    expect(res).toEqual({ batchItemFailures: [{ itemIdentifier: 'm0' }] });
  });
});

describe('CachedAlertReader', () => {
  const makeInner = () => {
    let calls = 0;
    const alert = signed();
    return {
      alert,
      get calls() {
        return calls;
      },
      inner: {
        get: async (id: string): Promise<Alert | null> => {
          calls++;
          return id === alert.id ? alert : null;
        },
      },
    };
  };

  it('serves repeat reads within 10 s from memory and refetches after the TTL', async () => {
    const w = makeInner();
    let t = 1_000;
    const cache = new CachedAlertReader(w.inner, 10_000, () => t);
    await cache.get(w.alert.id, 's1');
    t += 9_999;
    await cache.get(w.alert.id, 's1');
    expect(w.calls).toBe(1);
    t += 2;
    await cache.get(w.alert.id, 's1');
    expect(w.calls).toBe(2);
  });

  it('keys by alertId + sourceId and shares concurrent reads', async () => {
    const w = makeInner();
    const cache = new CachedAlertReader(w.inner, 10_000, () => 0);
    await Promise.all([cache.get(w.alert.id, 'a'), cache.get(w.alert.id, 'a')]);
    expect(w.calls).toBe(1);
    await cache.get(w.alert.id, 'b');
    expect(w.calls).toBe(2);
  });

  it('does not cache misses or failures', async () => {
    const w = makeInner();
    const cache = new CachedAlertReader(w.inner, 10_000, () => 0);
    expect(await cache.get('nope', 's')).toBeNull();
    await cache.get('nope', 's');
    expect(w.calls).toBe(2);
    let fail = true;
    const flaky = new CachedAlertReader(
      {
        get: async () => {
          if (fail) throw new Error('db');
          return w.alert;
        },
      },
      10_000,
      () => 0,
    );
    await expect(flaky.get('x', 's')).rejects.toThrow('db');
    fail = false;
    expect(await flaky.get('x', 's')).toBe(w.alert);
  });

  it('cuts Aurora reads: planner plus many shard workers on one warm container load the alert once', async () => {
    const alert = signed({}, countyCells());
    const w = makeInner();
    const base = setup(alert);
    const cache = new CachedAlertReader({ get: (id) => w.inner.get(id) });
    const deps = { ...base.deps, alerts: cache };
    const cell = CELLS[0] as string;
    base.cellIndex.add(entry(cell, 'dev-1'));
    await planShards(ready(alert), deps);
    for (let i = 0; i < 5; i++) await processShard(jobFor(alert, [cell]), deps);
    expect(w.calls).toBe(1);
  });
});

describe('send DLQ redrive', () => {
  it('starts a native move task per DLQ, back to the source queue', async () => {
    const seen: Array<{ SourceArn?: string; DestinationArn?: string }> = [];
    const sqs = {
      send: async (cmd: unknown) => {
        if (!(cmd instanceof StartMessageMoveTaskCommand)) throw new Error('unexpected');
        seen.push(cmd.input);
        return { TaskHandle: `h-${seen.length}` };
      },
    };
    const dlqArns = { urgent: 'arn:u-dlq', standard: 'arn:s-dlq' };
    expect(await redriveSendDlqs({ sqs, dlqArns }, ['urgent', 'standard'])).toEqual({
      urgent: 'h-1',
      standard: 'h-2',
    });
    expect(seen).toEqual([{ SourceArn: 'arn:u-dlq' }, { SourceArn: 'arn:s-dlq' }]);
    seen.length = 0;
    await redriveSendDlqs({ sqs, dlqArns }, ['urgent']);
    expect(seen).toEqual([{ SourceArn: 'arn:u-dlq' }]);
  });
});
