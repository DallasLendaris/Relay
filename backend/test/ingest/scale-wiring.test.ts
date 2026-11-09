import { readFileSync } from 'node:fs';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { encodeZonePack } from '../../src/geo/s3-zone-provider.js';
import { validateScaleEnv } from '../../src/ingest/handler.js';
import { createIngestPipeline, ZONE_PACK_KEY } from '../../src/ingest/pipeline.js';
import { MemoryPlanQueue } from '../../src/scale/fanout/queues.js';
import { activeShardKey } from '../../src/scale/publish/paths.js';

const feed = JSON.parse(
  readFileSync(new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
) as { features: unknown[] };
const ring = (id: string, secret: string) => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});

// A zone around San Jose that only the pack knows.
const PACK_ZONE = {
  type: 'Polygon' as const,
  coordinates: [
    [
      [-121.9, 37.3],
      [-121.85, 37.3],
      [-121.85, 37.35],
      [-121.9, 37.35],
      [-121.9, 37.3],
    ],
  ],
};

const s3Mock = mockClient(S3Client);
afterEach(() => s3Mock.reset());

function build(env: Record<string, string | undefined>) {
  const regions: Array<string | undefined> = [];
  const fetchSpy = vi.fn(async () => new Response('{}', { status: 404 }));
  const alertStore = new InMemoryAlertStore();
  const planQueue = new MemoryPlanQueue();
  const engine = createIngestPipeline({
    alertStore,
    places: new MemoryPlaceStore(),
    devices: new MemoryDeviceStore(),
    queue: new MemoryDeliveryQueue(),
    alertKeys: ring('a1', 'alert-secret'),
    tokenKeys: ring('t1', 'token-secret'),
    isDeliveryPaused: async () => false,
    fetch: fetchSpy as unknown as typeof fetch,
    userAgent: 'relay-test',
    clock: { now: () => new Date('2026-11-09T15:00:00Z') },
    logger: noopLogger,
    env,
    planQueue,
    s3: (region) => {
      regions.push(region);
      return new S3Client({ region: region ?? 'us-west-2' });
    },
  });
  return { engine, regions, fetchSpy, planQueue, alertStore };
}

function ugcArea() {
  return {
    description: 'Zone only',
    geometry: null,
    ugcCodes: ['CAZ999'],
    sameCodes: [],
    h3Cells: [],
  };
}

describe('ingest zone provider wiring', () => {
  it('ZONE_PACK_BUCKET: the pack answers first, so NWS is never asked', async () => {
    const gz = await encodeZonePack({
      version: 1,
      builtAt: '2026-11-09T00:00:00.000Z',
      zones: { CAZ999: PACK_ZONE },
    });
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToByteArray: async () => gz },
    } as never);
    const { engine, fetchSpy } = build({ ZONE_PACK_BUCKET: 'pack-bucket' });
    const cells = await engine.cellsFor(ugcArea());
    expect(cells.length).toBeGreaterThan(0);
    expect(fetchSpy).not.toHaveBeenCalled();
    const [call] = s3Mock.commandCalls(GetObjectCommand);
    expect(call?.args[0].input).toEqual({ Bucket: 'pack-bucket', Key: ZONE_PACK_KEY });
    expect(ZONE_PACK_KEY).toBe('zones/v1/california.json.gz');
  });

  it('ZONE_PACK_BUCKET: a pack miss and a pack outage both fall back to NWS', async () => {
    const gz = await encodeZonePack({
      version: 1,
      builtAt: '2026-11-09T00:00:00.000Z',
      zones: {},
    });
    s3Mock.on(GetObjectCommand).resolves({
      Body: { transformToByteArray: async () => gz },
    } as never);
    const miss = build({ ZONE_PACK_BUCKET: 'pack-bucket' });
    await expect(miss.engine.cellsFor(ugcArea())).rejects.toThrow(); // NWS says 404: no cells
    expect(miss.fetchSpy).toHaveBeenCalled();

    s3Mock.on(GetObjectCommand).rejects(new Error('S3 down'));
    const down = build({ ZONE_PACK_BUCKET: 'pack-bucket' });
    await expect(down.engine.cellsFor(ugcArea())).rejects.toThrow();
    expect(down.fetchSpy).toHaveBeenCalled();
  });

  it('without ZONE_PACK_BUCKET only NWS is used and no S3 client is built', async () => {
    const { engine, fetchSpy, regions } = build({});
    await expect(engine.cellsFor(ugcArea())).rejects.toThrow();
    expect(fetchSpy).toHaveBeenCalled();
    expect(regions).toEqual([]);
    expect(s3Mock.calls()).toHaveLength(0);
  });
});

describe('ingest publisher wiring', () => {
  async function applyOne(env: Record<string, string | undefined>) {
    const built = build(env);
    const norm = built.engine.normalize(feed.features[0]);
    if (!norm.ok) throw new Error(norm.reason);
    norm.alert.area.h3Cells = await built.engine.cellsFor(norm.alert.area);
    const result = await built.engine.apply(norm.alert);
    await built.engine.onApplied?.(result); // planning; the CDN publish starts in the background
    await built.engine.flush?.();
    return { ...built, result };
  }

  it('PUBLIC_ALERTS_BUCKET: every applied alert is published, from a client in PUBLIC_ALERTS_REGION', async () => {
    s3Mock.on(PutObjectCommand).resolves({});
    const { result, regions } = await applyOne({
      PUBLIC_ALERTS_BUCKET: 'public-bucket',
      PUBLIC_ALERTS_REGION: 'us-east-1',
    });
    expect(regions).toEqual(['us-east-1']);
    const puts = s3Mock.commandCalls(PutObjectCommand).map((c) => c.args[0].input);
    expect(puts.every((p) => p.Bucket === 'public-bucket')).toBe(true);
    const keys = puts.map((p) => p.Key);
    expect(keys.some((k) => k?.startsWith(`alerts/v1/doc/${result.alert.id}/`))).toBe(true);
    expect(keys).toContain(`alerts/v1/latest/${result.alert.id}.json`);
    expect(keys.some((k) => k?.startsWith(activeShardKey('').slice(0, -'.json'.length)))).toBe(
      true,
    );
    // The published alert is the SIGNED one (the publisher runs after signing and AlertReady).
    const full = puts.find((p) => p.Key?.startsWith(`alerts/v1/doc/${result.alert.id}/`));
    const body = JSON.parse(String(full?.Body)) as { signature: unknown; area: object };
    expect(body.signature).not.toBeNull();
    expect(body.area).not.toHaveProperty('h3Cells');
  });

  it('a publish failure never fails the save', async () => {
    s3Mock.on(PutObjectCommand).rejects(new Error('S3 down'));
    const { result, alertStore } = await applyOne({
      PUBLIC_ALERTS_BUCKET: 'public-bucket',
      PUBLIC_ALERTS_REGION: 'us-east-1',
    });
    expect(result.action).toBe('created');
    expect(await alertStore.get(result.alert.id)).not.toBeNull();
  });

  it('queue mode: AlertReady is published BEFORE any S3 write, and the alert JSON is the first PUT', async () => {
    const seenAtPut: number[] = [];
    const keys: string[] = [];
    const ref: { queue?: { messages: unknown[] } } = {};
    s3Mock.on(PutObjectCommand).callsFake((input: { Key?: string }) => {
      seenAtPut.push(ref.queue?.messages.length ?? -1);
      keys.push(String(input.Key));
      return {};
    });
    const built = build({
      FANOUT_MODE: 'queue',
      PLAN_QUEUE_URL: 'https://sqs/plan',
      PUBLIC_ALERTS_BUCKET: 'public-bucket',
      PUBLIC_ALERTS_REGION: 'us-east-1',
    });
    ref.queue = built.planQueue;
    const norm = built.engine.normalize(feed.features[0]);
    if (!norm.ok) throw new Error(norm.reason);
    norm.alert.area.h3Cells = await built.engine.cellsFor(norm.alert.area);
    const result = await built.engine.apply(norm.alert);
    await built.engine.onApplied?.(result);
    await built.engine.flush?.();
    expect(built.planQueue.messages).toHaveLength(1);
    expect(keys[0]).toMatch(new RegExp(`^alerts/v1/doc/${result.alert.id}/`));
    expect(keys.length).toBeGreaterThan(1);
    expect(seenAtPut.every((n) => n === 1)).toBe(true);
  });

  it('the CDN publish is capped by publishOptions.deadlineMs and never blocks onApplied', async () => {
    s3Mock.on(PutObjectCommand).callsFake(async (input: { Key?: string }) => {
      if (String(input.Key).includes('/active/')) await new Promise(() => {});
      return {};
    });
    const { engine, planQueue } = build({
      FANOUT_MODE: 'queue',
      PLAN_QUEUE_URL: 'https://sqs/plan',
      PUBLIC_ALERTS_BUCKET: 'public-bucket',
      PUBLIC_ALERTS_REGION: 'us-east-1',
    });
    const norm = engine.normalize(feed.features[0]);
    if (!norm.ok) throw new Error(norm.reason);
    norm.alert.area.h3Cells = await engine.cellsFor(norm.alert.area);
    const result = await engine.apply(norm.alert);
    // Default cap is 15 s; a hung shard write must not hold onApplied past it. Use fake timers.
    vi.useFakeTimers();
    try {
      const done = engine.onApplied?.(result);
      await vi.advanceTimersByTimeAsync(15_001);
      await expect(done).resolves.toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
    expect(planQueue.messages).toHaveLength(1);
  });

  it('without PUBLIC_ALERTS_BUCKET nothing is published', async () => {
    const { regions } = await applyOne({});
    expect(regions).toEqual([]);
    expect(s3Mock.calls()).toHaveLength(0);
  });
});

describe('validateScaleEnv (the handler checks the contract at cold start)', () => {
  it('accepts unset, inline and queue with a URL', () => {
    expect(() => validateScaleEnv({})).not.toThrow();
    expect(() => validateScaleEnv({ FANOUT_MODE: 'inline' })).not.toThrow();
    expect(() =>
      validateScaleEnv({ FANOUT_MODE: 'queue', PLAN_QUEUE_URL: 'https://sqs/plan' }),
    ).not.toThrow();
  });

  it('PLAN_QUEUE_URL is required only when FANOUT_MODE=queue', () => {
    expect(() => validateScaleEnv({ FANOUT_MODE: 'queue' })).toThrow('PLAN_QUEUE_URL');
    expect(() => validateScaleEnv({ FANOUT_MODE: 'inline' })).not.toThrow();
  });

  it('rejects an unknown FANOUT_MODE', () => {
    expect(() => validateScaleEnv({ FANOUT_MODE: 'direct' })).toThrow('FANOUT_MODE');
  });
});
