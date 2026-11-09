import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { noopLogger } from '../../src/delivery/index.js';
import { parseFeed } from '../../src/ingest/feed.js';
import { resolvePollIntervalMs } from '../../src/ingest/handler.js';
import { normalizeFeature } from '../../src/ingest/normalize.js';
import {
  MIN_POLL_INTERVAL_MS,
  RequestGate,
  createNwsClient,
  parseRetryAfter,
  type NwsClient,
  type NwsFetchResult,
} from '../../src/ingest/nws-client.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import {
  groupIntoChains,
  pollLoop,
  pollOnce,
  type PollDeps,
  type NormalizeResult,
} from '../../src/ingest/poll.js';
import { MemoryPlanQueue } from '../../src/scale/fanout/queues.js';
import type { SeenStore } from '../../src/ports.js';

const sleepMs = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- NWS client

describe('NWS client pacing (PERF-6)', () => {
  const okBody = () => new Response('{"type":"FeatureCollection","features":[]}', { status: 200 });

  it('sends a descriptive User-Agent and the ETag from a committed response', async () => {
    const calls: Array<Record<string, string>> = [];
    const fetchFn = (async (_url: string, init: { headers: Record<string, string> }) => {
      calls.push(init.headers);
      return calls.length === 1
        ? new Response('{}', { status: 200, headers: { ETag: '"abc"' } })
        : new Response(null, { status: 304 });
    }) as unknown as typeof fetch;
    const client = createNwsClient({ fetch: fetchFn, userAgent: 'Relay (ops@example.org)' });
    await client.fetchActiveCalifornia();
    client.commit();
    expect((await client.fetchActiveCalifornia()).status).toBe('not_modified');
    expect(calls[0]?.['User-Agent']).toBe('Relay (ops@example.org)');
    expect(calls[0]?.['If-None-Match']).toBeUndefined();
    expect(calls[1]?.['If-None-Match']).toBe('"abc"');
  });

  it('never requests the same URL twice within the minimum interval', async () => {
    let now = 1_000_000;
    const fetchFn = vi.fn(async () => okBody());
    const client = createNwsClient({
      fetch: fetchFn as unknown as typeof fetch,
      userAgent: 'u',
      minIntervalMs: 12_000, // below the floor: raised to 30 s
      gate: new RequestGate(),
      now: () => now,
    });
    expect((await client.fetchActiveCalifornia()).status).toBe('ok');
    now += 12_000; // the old 12 s cadence
    expect(await client.fetchActiveCalifornia()).toMatchObject({
      status: 'throttled',
      reason: 'min_interval',
      retryInMs: MIN_POLL_INTERVAL_MS - 12_000,
    });
    now += MIN_POLL_INTERVAL_MS;
    expect((await client.fetchActiveCalifornia()).status).toBe('ok');
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it('two clients sharing a gate (overlapping runs in one container) cannot double the rate', async () => {
    const fetchFn = vi.fn(async () => okBody());
    const gate = new RequestGate();
    const make = () =>
      createNwsClient({
        fetch: fetchFn as unknown as typeof fetch,
        userAgent: 'u',
        minIntervalMs: MIN_POLL_INTERVAL_MS,
        gate,
        now: () => 5_000_000,
      });
    const results = await Promise.all([
      make().fetchActiveCalifornia(),
      make().fetchActiveCalifornia(),
    ]);
    expect(results.map((r) => r.status).sort()).toEqual(['ok', 'throttled']);
    expect(fetchFn).toHaveBeenCalledTimes(1);
  });

  it('honors Retry-After (seconds) after a 429 and does not request until it passes', async () => {
    let now = 10_000_000;
    const fetchFn = vi.fn(
      async () => new Response('slow down', { status: 429, headers: { 'Retry-After': '120' } }),
    );
    const client = createNwsClient({
      fetch: fetchFn as unknown as typeof fetch,
      userAgent: 'u',
      gate: new RequestGate(),
      now: () => now,
    });
    expect(await client.fetchActiveCalifornia()).toMatchObject({
      status: 'error',
      httpStatus: 429,
      retryAfterMs: 120_000,
    });
    now += 119_000;
    expect(await client.fetchActiveCalifornia()).toMatchObject({
      status: 'throttled',
      reason: 'backoff',
    });
    expect(fetchFn).toHaveBeenCalledTimes(1);
    now += 1_001;
    await client.fetchActiveCalifornia();
    expect(fetchFn).toHaveBeenCalledTimes(2);
  });

  it('backs off exponentially on repeated 503 without Retry-After, and resets on success', async () => {
    let now = 20_000_000;
    let status = 503;
    const fetchFn = vi.fn(async () => (status === 200 ? okBody() : new Response('', { status })));
    const client = createNwsClient({
      fetch: fetchFn as unknown as typeof fetch,
      userAgent: 'u',
      gate: new RequestGate(),
      now: () => now,
    });
    const wait = async () => {
      const r = await client.fetchActiveCalifornia();
      if (r.status !== 'error') throw new Error(`expected error, got ${r.status}`);
      now += r.retryAfterMs ?? 0;
      return r.retryAfterMs;
    };
    expect(await wait()).toBe(30_000);
    expect(await wait()).toBe(60_000);
    expect(await wait()).toBe(120_000);
    status = 200;
    expect((await client.fetchActiveCalifornia()).status).toBe('ok');
    status = 503;
    expect(await wait()).toBe(30_000);
  });

  it('parses Retry-After as seconds or as an HTTP date, and ignores garbage', () => {
    const now = Date.parse('2026-11-09T10:00:00Z');
    expect(parseRetryAfter('45', now)).toBe(45_000);
    expect(parseRetryAfter('Mon, 09 Nov 2026 10:01:00 GMT', now)).toBe(60_000);
    expect(parseRetryAfter('Mon, 09 Nov 2026 09:00:00 GMT', now)).toBe(0);
    expect(parseRetryAfter('soon', now)).toBeNull();
    expect(parseRetryAfter(null, now)).toBeNull();
  });
});

describe('NWS client body limit (SEC-B-10)', () => {
  it('refuses a response whose Content-Length is over the limit', async () => {
    const fetchFn = (async () =>
      new Response('x', { status: 200, headers: { 'Content-Length': '999999' } })) as typeof fetch;
    const client = createNwsClient({ fetch: fetchFn, userAgent: 'u', maxBodyBytes: 1000 });
    expect(await client.fetchActiveCalifornia()).toMatchObject({
      status: 'error',
      message: expect.stringContaining('refused') as unknown,
    });
  });

  it('stops reading a chunked body without Content-Length once it passes the limit', async () => {
    let pulled = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulled += 1;
        controller.enqueue(new Uint8Array(400));
        if (pulled > 1000) controller.close();
      },
    });
    const fetchFn = (async () => new Response(stream, { status: 200 })) as typeof fetch;
    const client = createNwsClient({ fetch: fetchFn, userAgent: 'u', maxBodyBytes: 1000 });
    const res = await client.fetchActiveCalifornia();
    expect(res.status).toBe('error');
    expect(pulled).toBeLessThan(10); // it did not drain an endless stream
  });

  it('still reads a body under the limit', async () => {
    const fetchFn = (async () => new Response('{"a":1}', { status: 200 })) as typeof fetch;
    const client = createNwsClient({ fetch: fetchFn, userAgent: 'u', maxBodyBytes: 1000 });
    expect(await client.fetchActiveCalifornia()).toMatchObject({ status: 'ok', body: '{"a":1}' });
  });
});

describe('poll interval floor', () => {
  afterEach(() => vi.restoreAllMocks());

  it('raises an interval below 30 s to 30 s with a warning, keeps higher values', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    expect(resolvePollIntervalMs('12')).toBe(30_000);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(resolvePollIntervalMs('0.5')).toBe(30_000);
    expect(resolvePollIntervalMs('45')).toBe(45_000);
    expect(resolvePollIntervalMs(undefined)).toBe(30_000);
    expect(warn).toHaveBeenCalledTimes(2);
  });
});

describe('pollLoop pacing', () => {
  it('paces start to start (slow processing does not stretch or shrink the gap) and skips nothing', async () => {
    let now = 0;
    const sleeps: number[] = [];
    const deps: PollDeps = {
      client: {
        fetchActiveCalifornia: async () => {
          now += 5_000; // a poll takes 5 s
          return { status: 'not_modified' };
        },
        commit() {},
      },
      archive: new MemoryRawArchive(),
      seen: { has: async () => false, mark: async () => {} },
      normalize: () => ({ ok: false, reason: 'x', sourceId: null }),
      parseFeed: () => ({ features: [] }),
      apply: async (alert) => ({ action: 'created', alert }),
      cellsFor: async () => [],
      log: () => {},
    };
    const results = await pollLoop(deps, {
      polls: 3,
      intervalMs: 30_000,
      now: () => now,
      sleep: async (ms) => {
        sleeps.push(ms);
        now += ms;
      },
      remainingMs: () => 1_000_000,
    });
    expect(results).toHaveLength(3);
    expect(sleeps).toEqual([25_000, 25_000]); // 30 s between starts, minus the 5 s the poll took
  });
});

// ---------------------------------------------------------------- concurrency

const mkAlert = (sourceId: string, sent: string, refs: string[] = [], sev = 'warning'): Alert =>
  ({
    sourceId,
    sent,
    event: 'Flood Warning',
    relaySeverity: sev,
    references: refs.map((r) => ({ sourceId: r, sent })),
    area: { description: '', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: [] },
  }) as unknown as Alert;

function synthetic(alerts: Alert[], over: Partial<PollDeps> = {}) {
  const alertBySource = new Map(alerts.map((a) => [a.sourceId, a]));
  const body = JSON.stringify({ features: alerts.map((a) => ({ id: a.sourceId })) });
  const commits = { n: 0 };
  const ids = new Set<string>();
  const seen: SeenStore = {
    has: async (id) => ids.has(id),
    mark: async (id) => void ids.add(id),
  };
  const client: NwsClient = {
    fetchActiveCalifornia: async (): Promise<NwsFetchResult> => ({
      status: 'ok',
      body,
      fetchedAt: new Date(),
    }),
    commit: () => void (commits.n += 1),
  };
  const deps: PollDeps = {
    client,
    archive: new MemoryRawArchive(),
    seen,
    normalize: (f): NormalizeResult => {
      const a = alertBySource.get((f as { id: string }).id);
      return a
        ? { ok: true, alert: structuredClone(a) }
        : { ok: false, reason: 'x', sourceId: null };
    },
    parseFeed: (b) => ({ features: (JSON.parse(b) as { features: unknown[] }).features }),
    cellsFor: async () => ['c'],
    apply: async (alert) => ({ action: 'created', alert }),
    log: () => {},
    ...over,
  };
  return { deps, commits, ids };
}

describe('concurrent feature processing (PERF-6 / REL-10)', () => {
  it('processes N independent features on a bounded pool: faster, never above the bound', async () => {
    const N = 64;
    const STAGE_MS = 8; // apply (DB) + onApplied (AlertReady publish)
    const alerts = Array.from({ length: N }, (_, i) =>
      mkAlert(`urn:oid:${i}`, `2026-11-09T10:${String(i % 60).padStart(2, '0')}:00Z`),
    );
    const run = async (concurrency: number) => {
      let inFlight = 0;
      let peak = 0;
      const wrap =
        <A extends unknown[], R>(fn: (...a: A) => Promise<R>) =>
        async (...a: A): Promise<R> => {
          inFlight += 1;
          peak = Math.max(peak, inFlight);
          try {
            return await fn(...a);
          } finally {
            inFlight -= 1;
          }
        };
      const { deps, commits } = synthetic(alerts, {
        concurrency,
        apply: wrap(async (alert: Alert) => {
          await sleepMs(STAGE_MS);
          return { action: 'created' as const, alert };
        }),
        onApplied: wrap(async () => sleepMs(STAGE_MS)),
      });
      const t0 = performance.now();
      const counts = await pollOnce(deps);
      return { ms: performance.now() - t0, peak, counts, commits: commits.n };
    };
    const serial = await run(1);
    const pooled = await run(8);
    process.stderr.write(
      `[perf] ${N} features x 2 stages x ${STAGE_MS} ms: serial ${serial.ms.toFixed(0)} ms, ` +
        `pool of 8 ${pooled.ms.toFixed(0)} ms (${(serial.ms / pooled.ms).toFixed(1)}x), peak ${pooled.peak}\n`,
    );
    expect(serial.counts.applied).toBe(N);
    expect(pooled.counts.applied).toBe(N);
    expect(pooled.commits).toBe(1);
    expect(serial.peak).toBe(1);
    expect(pooled.peak).toBeLessThanOrEqual(8);
    expect(pooled.peak).toBeGreaterThan(1);
    expect(pooled.ms).toBeLessThan(serial.ms / 3);
  });

  it('keeps one chain in order while other chains run beside it', async () => {
    // chain A: a1 <- a2 <- a3 (each references the previous); chain B: b1 <- b2; loners l0..l5
    const alerts = [
      mkAlert('a1', '2026-11-09T10:00:00Z'),
      mkAlert('a2', '2026-11-09T10:05:00Z', ['a1']),
      mkAlert('a3', '2026-11-09T10:10:00Z', ['a2']),
      mkAlert('b1', '2026-11-09T10:01:00Z'),
      mkAlert('b2', '2026-11-09T10:06:00Z', ['b1']),
      ...Array.from({ length: 6 }, (_, i) => mkAlert(`l${i}`, '2026-11-09T10:02:00Z')),
    ];
    const started: string[] = [];
    const finished: string[] = [];
    const { deps } = synthetic(alerts, {
      concurrency: 8,
      apply: async (alert) => {
        started.push(alert.sourceId);
        // Earlier messages of a chain are the SLOW ones: a racy implementation reorders them.
        await sleepMs(alert.sourceId.endsWith('1') ? 25 : 2);
        finished.push(alert.sourceId);
        return { action: 'created', alert };
      },
    });
    await pollOnce(deps);
    for (const chain of [
      ['a1', 'a2', 'a3'],
      ['b1', 'b2'],
    ]) {
      expect(finished.filter((id) => chain.includes(id))).toEqual(chain);
      expect(started.filter((id) => chain.includes(id))).toEqual(chain);
    }
    // Different chains really overlapped: l-alerts started before a1 finished.
    expect(started.indexOf('l0')).toBeLessThan(finished.indexOf('a1'));
  });

  it('groups by chain and puts the most urgent chain first', () => {
    const q = [
      mkAlert('x2', '2026-11-09T10:05:00Z', ['x1'], 'advisory'),
      mkAlert('y1', '2026-11-09T10:00:00Z', [], 'emergency'),
      mkAlert('x1', '2026-11-09T10:00:00Z', [], 'advisory'),
    ];
    const rank = (a: Alert) =>
      ['emergency', 'warning', 'watch', 'advisory'].indexOf(a.relaySeverity);
    const groups = groupIntoChains(q, rank).map((g) => g.map((a) => a.sourceId));
    expect(groups).toEqual([['y1'], ['x1', 'x2']]);
  });

  it('one failing feature does not stop the others, and the ETag is not committed', async () => {
    const alerts = Array.from({ length: 12 }, (_, i) =>
      mkAlert(`urn:oid:${i}`, '2026-11-09T10:00:00Z'),
    );
    const { deps, commits, ids } = synthetic(alerts, {
      apply: async (alert) => {
        if (alert.sourceId === 'urn:oid:3') throw new Error('db down');
        return { action: 'created', alert };
      },
    });
    const counts = await pollOnce(deps);
    expect(counts.errors).toBe(1);
    expect(counts.applied).toBe(11);
    expect(ids.size).toBe(11);
    expect(commits.n).toBe(0);
  });

  it('a throttled poll is not an error and does nothing', async () => {
    const entries: Record<string, unknown>[] = [];
    const base = synthetic([]);
    const deps: PollDeps = {
      ...base.deps,
      client: {
        fetchActiveCalifornia: async () => ({
          status: 'throttled',
          retryInMs: 9000,
          reason: 'backoff',
        }),
        commit: () => void (base.commits.n += 1),
      },
      log: (e) => entries.push(e),
    };
    const counts = await pollOnce(deps);
    expect(counts).toMatchObject({ throttled: 1, errors: 0, fetchFailed: 0, fetched: 0 });
    expect(entries[0]).toMatchObject({ event: 'poll.throttled', reason: 'backoff' });
  });

  it('REL-7: unknown zones are counted and logged per alert, and the alert still goes out', async () => {
    const entries: Record<string, unknown>[] = [];
    const delivered: string[] = [];
    const { deps, ids } = synthetic([mkAlert('urn:oid:z', '2026-11-09T10:00:00Z')], {
      cellsFor: async (_area, ctx) => {
        ctx?.onUnknownZones?.(['CAZ998', 'CAZ999'], 5);
        return ['known-cell'];
      },
      onApplied: (r) => void delivered.push(r.alert.sourceId),
      log: (e) => entries.push(e),
    });
    const counts = await pollOnce(deps);
    expect(counts.unknownZones).toBe(2);
    expect(counts.errors).toBe(0);
    expect(delivered).toEqual(['urn:oid:z']);
    expect(ids.has('urn:oid:z')).toBe(true);
    expect(entries.find((e) => e.event === 'poll.unknown_zones')).toMatchObject({
      level: 'warn',
      sourceId: 'urn:oid:z',
      unknown: 2,
      total: 5,
    });
  });
});

// ---------------------------------------------------------------- geometry in normalize

describe('normalize geometry validation (SEC-B-5)', () => {
  const base = JSON.parse(
    readFileSync(new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
  ) as { features: Array<Record<string, any>> };
  const feature = (geometry: unknown, ugc: string[]) => {
    const f = structuredClone(base.features[0]) as Record<string, any>;
    f['geometry'] = geometry;
    f['properties'].geocode.UGC = ugc;
    return f;
  };
  const collinear = {
    type: 'Polygon',
    coordinates: [
      [
        [-121.9, 37.3],
        [-121.8, 37.3],
        [-121.7, 37.3],
        [-121.9, 37.3],
      ],
    ],
  };

  it('falls back to UGC zones when the polygon is invalid and zones are listed', () => {
    const warn = vi.fn();
    const r = normalizeFeature(feature(collinear, ['CAZ513']), { newId: randomUUID, warn });
    expect(r.ok).toBe(true);
    if (r.ok) {
      expect(r.alert.area.geometry).toBeNull();
      expect(r.alert.area.ugcCodes).toEqual(['CAZ513']);
    }
    expect(warn).toHaveBeenCalledWith(
      'invalid geometry dropped, using UGC zones',
      expect.anything(),
    );
  });

  it('rejects with a permanent reason when the polygon is invalid and no zones are listed', () => {
    const r = normalizeFeature(feature(collinear, []), { newId: randomUUID });
    expect(r).toMatchObject({ ok: false, permanent: true });
    if (!r.ok) expect(r.reason).toMatch(/^permanent: invalid_geometry/);
  });

  it('keeps a valid polygon untouched', () => {
    const r = normalizeFeature(base.features[0], { newId: randomUUID });
    expect(r.ok && r.alert.area.geometry !== null).toBe(true);
  });

  it('a rejected bad feature is logged and counted, the feed continues, and the ETag still commits', async () => {
    const good = base.features[1] as Record<string, any>;
    const bad = feature(collinear, []);
    const body = JSON.stringify({ type: 'FeatureCollection', features: [bad, good] });
    const entries: Record<string, unknown>[] = [];
    let commits = 0;
    const ids = new Set<string>();
    const deps: PollDeps = {
      client: {
        fetchActiveCalifornia: async () => ({ status: 'ok', body, fetchedAt: new Date() }),
        commit: () => void (commits += 1),
      },
      archive: new MemoryRawArchive(),
      seen: { has: async (id) => ids.has(id), mark: async (id) => void ids.add(id) },
      normalize: (f) => normalizeFeature(f, { newId: randomUUID }),
      parseFeed,
      cellsFor: async () => ['c'],
      apply: async (alert) => ({ action: 'created', alert }),
      log: (e) => entries.push(e),
    };
    const counts = await pollOnce(deps);
    expect(counts).toMatchObject({ rejected: 1, accepted: 1, applied: 1, errors: 0 });
    expect(commits).toBe(1);
    expect(entries.find((e) => e.event === 'poll.rejected')).toMatchObject({ level: 'error' });
  });
});

// ---------------------------------------------------------------- publish decoupling

describe('public publish does not gate fan-out (PERF-6)', () => {
  const s3Mock = mockClient(S3Client);
  afterEach(() => s3Mock.reset());
  const feed = JSON.parse(
    readFileSync(new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
  ) as { features: unknown[] };
  const ring = (id: string, secret: string) => ({
    current: { id, secret },
    all: new Map([[id, secret]]),
  });

  it('a failing publish is logged and counted, but the alert is still queued, seen and the ETag commits', async () => {
    s3Mock.on(PutObjectCommand).rejects(new Error('S3 down'));
    const errors: Array<[string, unknown]> = [];
    const planQueue = new MemoryPlanQueue();
    const engine = createIngestPipeline({
      alertStore: new InMemoryAlertStore(),
      places: new MemoryPlaceStore(),
      devices: new MemoryDeviceStore(),
      queue: new MemoryDeliveryQueue(),
      alertKeys: ring('a1', 'alert-secret'),
      tokenKeys: ring('t1', 'token-secret'),
      isDeliveryPaused: async () => false,
      fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
      userAgent: 'relay-test',
      clock: { now: () => new Date('2026-11-09T15:00:00Z') },
      logger: { ...noopLogger, error: (m, f) => errors.push([m, f]) },
      env: {
        FANOUT_MODE: 'queue',
        PLAN_QUEUE_URL: 'https://sqs/plan',
        PUBLIC_ALERTS_BUCKET: 'b',
        PUBLIC_ALERTS_REGION: 'us-east-1',
      },
      planQueue,
      s3: () => new S3Client({ region: 'us-east-1' }),
    });
    const ids = new Set<string>();
    let commits = 0;
    const body = JSON.stringify({
      type: 'FeatureCollection',
      features: [feed.features[0], feed.features[2]],
    });
    const counts = await pollOnce({
      ...engine,
      client: {
        fetchActiveCalifornia: async () => ({ status: 'ok', body, fetchedAt: new Date() }),
        commit: () => void (commits += 1),
      },
      archive: new MemoryRawArchive(),
      seen: { has: async (id) => ids.has(id), mark: async (id) => void ids.add(id) },
      log: () => {},
    });
    expect(counts.applied).toBe(2);
    expect(counts.errors).toBe(0);
    expect(counts.publishFailed).toBeGreaterThanOrEqual(counts.applied);
    expect(ids.size).toBe(counts.applied);
    expect(planQueue.messages.length).toBeGreaterThanOrEqual(1);
    expect(commits).toBe(1);
    expect(errors.filter(([m]) => m === 'publish.failed').length).toBeGreaterThanOrEqual(2);
  });

  it('onApplied returns before a slow publish finishes; flush waits for it', async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => (release = r));
    s3Mock.on(PutObjectCommand).callsFake(async () => {
      await gate;
      return {};
    });
    const planQueue = new MemoryPlanQueue();
    const engine = createIngestPipeline({
      alertStore: new InMemoryAlertStore(),
      places: new MemoryPlaceStore(),
      devices: new MemoryDeviceStore(),
      queue: new MemoryDeliveryQueue(),
      alertKeys: ring('a1', 'alert-secret'),
      tokenKeys: ring('t1', 'token-secret'),
      isDeliveryPaused: async () => false,
      fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
      userAgent: 'relay-test',
      clock: { now: () => new Date('2026-11-09T15:00:00Z') },
      logger: noopLogger,
      env: {
        FANOUT_MODE: 'queue',
        PLAN_QUEUE_URL: 'https://sqs/plan',
        PUBLIC_ALERTS_BUCKET: 'b',
        PUBLIC_ALERTS_REGION: 'us-east-1',
      },
      planQueue,
      s3: () => new S3Client({ region: 'us-east-1' }),
    });
    const norm = engine.normalize(feed.features[0]);
    if (!norm.ok) throw new Error(norm.reason);
    norm.alert.area.h3Cells = await engine.cellsFor(norm.alert.area);
    const result = await engine.apply(norm.alert);
    await engine.onApplied?.(result); // resolves although every S3 PUT is still blocked
    expect(planQueue.messages).toHaveLength(1);
    let flushed = false;
    const flushing = engine.flush?.().then((r) => {
      flushed = true;
      return r;
    });
    await sleepMs(20);
    expect(flushed).toBe(false);
    release();
    expect(await flushing).toEqual({ publishFailed: 0 });
  });
});
