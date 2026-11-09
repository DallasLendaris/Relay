import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../../src/adapters/memory/place-store.js';
import { noopLogger } from '../../../src/delivery/index.js';
import { createIngestPipeline } from '../../../src/ingest/pipeline.js';
import { DeliveryPausedError } from '../../../src/ingest/poll.js';
import { MemoryPlanQueue } from '../../../src/scale/fanout/queues.js';

const feed = JSON.parse(
  readFileSync(new URL('../../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
) as { features: unknown[] };
const ring = (id: string, secret: string) => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});

async function run(opts: {
  env: Record<string, string | undefined>;
  paused?: boolean;
  planQueue?: MemoryPlanQueue;
}) {
  const direct = new MemoryDeliveryQueue();
  const planQueue = opts.planQueue ?? new MemoryPlanQueue();
  const engine = createIngestPipeline({
    alertStore: new InMemoryAlertStore(),
    places: new MemoryPlaceStore(),
    devices: new MemoryDeviceStore(),
    queue: direct,
    alertKeys: ring('a1', 'alert-secret'),
    tokenKeys: ring('t1', 'token-secret'),
    isDeliveryPaused: async () => opts.paused ?? false,
    fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
    userAgent: 'relay-test',
    clock: { now: () => new Date('2026-11-09T15:00:00Z') },
    logger: noopLogger,
    env: opts.env,
    planQueue,
  });
  const norm = engine.normalize(feed.features[0]);
  if (!norm.ok) throw new Error(norm.reason);
  norm.alert.area.h3Cells = await engine.cellsFor(norm.alert.area);
  const result = await engine.apply(norm.alert);
  return { engine, result, planQueue, direct };
}

describe('pipeline FANOUT_MODE=queue', () => {
  it('publishes AlertReady instead of planning in-line', async () => {
    const { engine, result, planQueue, direct } = await run({ env: { FANOUT_MODE: 'queue' } });
    await engine.onApplied?.(result);
    expect(planQueue.messages).toEqual([
      {
        alertId: result.alert.id,
        sourceId: result.alert.sourceId,
        relaySeverity: result.alert.relaySeverity,
      },
    ]);
    expect(direct.jobs).toHaveLength(0);
  });

  it('throws DeliveryPausedError when paused, publishing nothing (alert stays unseen)', async () => {
    const { engine, result, planQueue } = await run({
      env: { FANOUT_MODE: 'queue' },
      paused: true,
    });
    await expect(engine.onApplied?.(result)).rejects.toBeInstanceOf(DeliveryPausedError);
    expect(planQueue.messages).toHaveLength(0);
  });

  it('re-throws a publish failure so the alert stays unseen', async () => {
    const failing = {
      publish: async () => {
        throw new Error('sqs down');
      },
    } as unknown as MemoryPlanQueue;
    const { engine, result } = await run({ env: { FANOUT_MODE: 'queue' }, planQueue: failing });
    await expect(engine.onApplied?.(result)).rejects.toThrow('sqs down');
  });

  it('requires PLAN_QUEUE_URL when no queue is injected', () => {
    expect(() =>
      createIngestPipeline({
        alertStore: new InMemoryAlertStore(),
        places: new MemoryPlaceStore(),
        devices: new MemoryDeviceStore(),
        queue: new MemoryDeliveryQueue(),
        alertKeys: ring('a1', 'alert-secret'),
        tokenKeys: ring('t1', 'token-secret'),
        isDeliveryPaused: async () => false,
        fetch,
        userAgent: 'x',
        env: { FANOUT_MODE: 'queue' },
      }),
    ).toThrow('PLAN_QUEUE_URL');
  });

  it('keeps the original in-line planning by default (flag unset or not "queue")', async () => {
    for (const env of [{}, { FANOUT_MODE: 'direct' }]) {
      const { engine, result, planQueue } = await run({ env });
      await engine.onApplied?.(result); // no places: plans nothing, but does not publish
      expect(planQueue.messages).toHaveLength(0);
    }
  });
});
