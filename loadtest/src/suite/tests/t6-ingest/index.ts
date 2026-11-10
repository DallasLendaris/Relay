/**
 * t6: ingest storm. A mock NWS server process serves a feed of 200 concurrent California alerts
 * (polygons, zones, degenerate geometries, statements, chains of updates and cancels) to the REAL
 * ingest pipeline (backend/src/ingest: normalize, geometry check and zone lookup over HTTP, H3
 * cells, lifecycle, signing store, pooled processing, AlertReady to the plan queue, reconciliation).
 *
 *   a  storm: feed fetch to the last AlertReady, per-alert latency, every alert planned once,
 *      degenerate ones rejected or recovered by UGC zones, updates applied in chain order, and the
 *      30 s poll gate (on a simulated clock, so no real minutes pass)
 *   b  flapping: alerts vanish from feed reads and return (none ends: reconciliation needs 3 reads
 *      and 5 minutes); one alert stays away for more than 5 minutes (it ends) and, back again, is
 *      restored without a duplicate push (the real planner, shard worker and sender run for it)
 *   c  expiredReferences on one feature cancel that alert only
 *
 * The pipeline runs in this process; only the NWS mock is a child. Time inside the pipeline (poll
 * gate, reconciliation) is a simulated clock that the poll loop's sleep advances.
 */
import type { Alert } from '@relay/shared';
import { MemoryDeliveryQueue } from '../../../../../backend/src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../../../../backend/src/adapters/memory/device-store.js';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { MemoryIdempotencyStore } from '../../../../../backend/src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../../../../backend/src/adapters/memory/place-store.js';
import { MemoryRawArchive } from '../../../../../backend/src/adapters/memory/raw-archive.js';
import { InMemorySeenStore } from '../../../../../backend/src/adapters/memory/seen-store.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import {
  MIN_POLL_INTERVAL_MS,
  RequestGate,
  createNwsClient,
} from '../../../../../backend/src/ingest/nws-client.js';
import { createIngestPipeline } from '../../../../../backend/src/ingest/pipeline.js';
import {
  FEATURE_CONCURRENCY,
  RECONCILE_AFTER,
  RECONCILE_MIN_ABSENT_MS,
  clearReconcileState,
  clearSaveFailures,
  pollLoop,
  pollOnce,
  type PollCounts,
  type PollDeps,
} from '../../../../../backend/src/ingest/poll.js';
import { MemoryCellIndex } from '../../../../../backend/src/scale/cell-index/memory-cell-index.js';
import type { NativePushSender } from '../../../../../backend/src/scale/contracts.js';
import { planShards } from '../../../../../backend/src/scale/fanout/planner.js';
import { processShard } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import { MemorySendQueue } from '../../../../../backend/src/scale/fanout/queues.js';
import { parseBatches } from '../../../../../backend/src/scale/sender/consumer.js';
import { processBatch } from '../../../../../backend/src/scale/sender/sender.js';
import {
  createAlertVerifier,
  createMemoryAlertSigning,
} from '../../../../../backend/src/signing/index.js';
import { percentile } from '../../../report.js';
import { spawnWorker } from '../../lib/child.js';
import { fmt, writeTestReport } from '../../lib/report.js';
import { RssSampler } from '../../lib/rss.js';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import { call, sleep } from '../t5-chaos/common.js';
import { buildFeed, expiredRef, feedBody, makeFeature, square, type FeedModel } from './feed.js';
import { fileURLToPath } from 'node:url';

const file = (name: string): string => fileURLToPath(new URL(`./${name}`, import.meta.url));

type Check = ScenarioResult['checks'][number];

/** A clock the poll loop's sleep advances: no real minutes pass while the gate and reconciliation see them. */
class SimClock {
  constructor(private ms: number) {}
  now = (): number => this.ms;
  advance(ms: number): void {
    this.ms += ms;
  }
}

interface ReadyEvent {
  alertId: string;
  sourceId: string;
  severity: string;
  atMs: number;
  simMs: number;
}

interface LogEvent {
  level: string;
  msg: string;
  fields: Record<string, unknown>;
}

interface Bench {
  nwsBase: string;
  clock: SimClock;
  store: InMemoryAlertStore;
  deps: PollDeps;
  ready: ReadyEvent[];
  logs: LogEvent[];
  pollLog: Array<Record<string, unknown>>;
  /** Simulated and real times at which a feed request left the client. */
  feedRequests: Array<{ simMs: number; realMs: number }>;
  peakInFlight: () => number;
  verify: (a: Alert) => boolean;
  client: ReturnType<typeof createNwsClient>;
  gate: RequestGate;
}

function makeBench(nwsBase: string, clock: SimClock): Bench {
  const signing = createMemoryAlertSigning('t6');
  const verify = createAlertVerifier(signing.publicKeys);
  const store = new InMemoryAlertStore();
  const ready: ReadyEvent[] = [];
  const logs: LogEvent[] = [];
  const pollLog: Array<Record<string, unknown>> = [];
  const feedRequests: Bench['feedRequests'] = [];
  const logger: Logger = {
    info: (msg, fields) => void logs.push({ level: 'info', msg, fields: fields ?? {} }),
    warn: (msg, fields) => void logs.push({ level: 'warn', msg, fields: fields ?? {} }),
    error: (msg, fields) => void logs.push({ level: 'error', msg, fields: fields ?? {} }),
  };
  // Every NWS URL the pipeline builds points at api.weather.gov; send it to the mock instead.
  const toMock = (url: string): string => url.replace('https://api.weather.gov', nwsBase);
  const fetchImpl = ((input: string | URL | Request, init?: RequestInit) => {
    const url = toMock(
      typeof input === 'string' ? input : input instanceof URL ? input.href : input.url,
    );
    if (url.includes('/alerts/active')) {
      feedRequests.push({ simMs: clock.now(), realMs: performance.now() });
    }
    return fetch(url, init);
  }) as typeof fetch;

  const engine = createIngestPipeline({
    alertStore: store,
    places: new MemoryPlaceStore(),
    devices: new MemoryDeviceStore(),
    queue: new MemoryDeliveryQueue(),
    alertSigning: signing,
    tokenKeys: { current: { id: 'k1', secret: 'x'.repeat(32) }, all: new Map() },
    isDeliveryPaused: async () => false,
    fetch: fetchImpl,
    userAgent: 'Relay-t6 (ingest-storm@example.org)',
    clock: { now: () => new Date(clock.now()) },
    logger,
    env: { FANOUT_MODE: 'queue' },
    planQueue: {
      async publish(m) {
        ready.push({
          alertId: m.alertId,
          sourceId: m.sourceId,
          severity: m.relaySeverity,
          atMs: performance.now(),
          simMs: clock.now(),
        });
      },
    },
  });

  // How many features are being resolved at once (zone lookups are the awaited I/O).
  let inFlight = 0;
  let peak = 0;
  const cellsFor: typeof engine.cellsFor = async (area, ctx) => {
    inFlight++;
    peak = Math.max(peak, inFlight);
    try {
      return await engine.cellsFor(area, ctx);
    } finally {
      inFlight--;
    }
  };

  const gate = new RequestGate();
  const client = createNwsClient({
    fetch: fetchImpl,
    userAgent: 'Relay-t6 (ingest-storm@example.org)',
    minIntervalMs: MIN_POLL_INTERVAL_MS,
    gate,
    now: clock.now,
  });
  const deps: PollDeps = {
    ...engine,
    cellsFor,
    client,
    archive: new MemoryRawArchive(),
    seen: new InMemorySeenStore(),
    log: (entry) => void pollLog.push(entry),
  };
  return {
    nwsBase,
    clock,
    store,
    deps,
    ready,
    logs,
    pollLog,
    feedRequests,
    peakInFlight: () => peak,
    verify,
    client,
    gate,
  };
}

const setFeed = (nws: string, features: object[]): Promise<unknown> =>
  call(nws, 'POST', '/control/feed', feedBody(features));

// ---------- Scenario a: the storm ----------

async function scenarioStorm(
  ctx: SuiteContext,
  nws: string,
  b: Bench,
  model: FeedModel,
  n: number,
): Promise<{ result: ScenarioResult; counts: PollCounts }> {
  const t0 = Date.now();
  await call(nws, 'POST', '/control/reset');
  await setFeed(
    nws,
    model.features.map((f) => f.feature),
  );
  ctx.log(`t6 storm: polling a feed of ${n} alerts`);

  const fetchStart = performance.now();
  const counts = await pollOnce(b.deps);
  const lastReady = b.ready.reduce((m, r) => Math.max(m, r.atMs), fetchStart);
  const totalMs = lastReady - fetchStart;
  const latencies = b.ready.map((r) => r.atMs - fetchStart).sort((x, y) => x - y);
  const firstRequest = b.feedRequests[0];

  // The gate: another request at the same simulated instant must be held back, not sent.
  const probe = await b.client.fetchActiveCalifornia();
  const log = await call<{ feedRequests: unknown[] }>(nws, 'GET', '/control/log');

  // ----- correctness -----
  const checks: Check[] = [];
  const byAlert = new Map<string, ReadyEvent[]>();
  for (const r of b.ready) {
    const list = byAlert.get(r.alertId) ?? [];
    list.push(r);
    byAlert.set(r.alertId, list);
  }
  const idOf = async (sourceId: string): Promise<Alert | null> => b.store.findBySourceId(sourceId);
  const problems: string[] = [];
  const note = (p: string): void => {
    if (problems.length < 6) problems.push(p);
  };

  // pushed once: valid polygon, zone and UGC-fallback alerts
  let onceOk = 0;
  for (const sourceId of model.pushedOnce) {
    const a = await idOf(sourceId);
    const rs = a ? (byAlert.get(a.id) ?? []) : [];
    if (
      a &&
      a.state === 'active' &&
      a.area.h3Cells.length > 0 &&
      rs.length === 1 &&
      rs[0]?.sourceId === sourceId
    )
      onceOk++;
    else
      note(
        `${sourceId}: ${a ? `${a.state}, ${a.area.h3Cells.length} cells, ${rs.length} ready` : 'not stored'}`,
      );
  }
  checks.push({
    name: 'every valid alert planned exactly once (polygon, zone, UGC fallback)',
    ok: onceOk === model.pushedOnce.length,
    detail: `${onceOk}/${model.pushedOnce.length}${problems.length ? `; e.g. ${problems[0]}` : ''}`,
  });

  // degenerate geometries without zones: rejected, nothing stored or planned
  let rejectedOk = 0;
  for (const sourceId of model.rejected) {
    if ((await idOf(sourceId)) === null && !b.ready.some((r) => r.sourceId === sourceId))
      rejectedOk++;
  }
  const rejectedLogged = b.pollLog.filter(
    (e) => e['event'] === 'poll.rejected' || e['event'] === 'poll.rejected_permanent',
  ).length;
  checks.push({
    name: 'degenerate geometries rejected (nothing stored, nothing planned)',
    ok: rejectedOk === model.rejected.length && counts.rejected === model.rejected.length,
    detail: `${rejectedOk}/${model.rejected.length} rejected, ${counts.rejected} counted, ${rejectedLogged} logged`,
  });

  // statements: stored, never planned
  let statementsOk = 0;
  for (const sourceId of model.statements) {
    const a = await idOf(sourceId);
    if (a && !byAlert.has(a.id)) statementsOk++;
  }
  checks.push({
    name: 'informational statements stored but never planned',
    ok: statementsOk === model.statements.length,
    detail: `${statementsOk}/${model.statements.length}`,
  });

  // chains of updates: applied in order, one AlertReady per message, in that order
  let chainsOk = 0;
  for (const [key, ids] of model.chains3) {
    const a = await idOf(key);
    const seq = a ? (byAlert.get(a.id) ?? []).map((r) => r.sourceId) : [];
    const inOrder = seq.length === 3 && seq.every((s, i) => s === ids[i]);
    if (a && a.state === 'updated' && a.sourceId === ids[2] && inOrder) chainsOk++;
    else note(`chain ${key}: ${a?.state}/${a?.sourceId}/${seq.join(',')}`);
  }
  checks.push({
    name: 'update chains applied in order (Alert, Update, Update), one plan per message',
    ok: chainsOk === model.chains3.size,
    detail: `${chainsOk}/${model.chains3.size} chains`,
  });
  let cancelsOk = 0;
  for (const [key, ids] of model.chainsCancel) {
    const a = await idOf(key);
    const seq = a ? (byAlert.get(a.id) ?? []).map((r) => r.sourceId) : [];
    if (a && a.state === 'cancelled' && seq.length === 1 && seq[0] === ids[0]) cancelsOk++;
  }
  checks.push({
    name: 'cancel chains end the alert and plan nothing for the cancel',
    ok: cancelsOk === model.chainsCancel.size,
    detail: `${cancelsOk}/${model.chainsCancel.size} chains`,
  });

  const pairs = new Set(b.ready.map((r) => `${r.alertId}:${r.sourceId}`));
  checks.push({
    name: 'AlertReady count equals the expected plans, no duplicates',
    ok: b.ready.length === model.expectedReady && pairs.size === b.ready.length,
    detail: `${b.ready.length} published, ${model.expectedReady} expected, ${pairs.size} distinct`,
  });
  checks.push({
    name: 'no ingest errors, quarantines or failed zone lookups',
    ok:
      counts.errors === 0 &&
      counts.quarantined === 0 &&
      counts.fetchFailed === 0 &&
      counts.unknownZones === 0,
    detail: `errors ${counts.errors}, quarantined ${counts.quarantined}, unknown zones ${counts.unknownZones}`,
  });
  checks.push({
    name: `alerts processed on a bounded pool (at most ${FEATURE_CONCURRENCY} at once)`,
    ok: b.peakInFlight() <= FEATURE_CONCURRENCY && b.peakInFlight() >= 1,
    detail: `peak ${b.peakInFlight()} at once`,
  });
  checks.push({
    name: 'poll gate held a second request at the same instant (>= 30 s between requests)',
    ok:
      probe.status === 'throttled' &&
      probe.reason === 'min_interval' &&
      probe.retryInMs >= MIN_POLL_INTERVAL_MS - 1 &&
      log.feedRequests.length === 1 &&
      firstRequest !== undefined,
    detail: `status ${probe.status}${probe.status === 'throttled' ? `, wait ${probe.retryInMs} ms` : ''}, mock saw ${log.feedRequests.length} request`,
  });

  const metrics: Metric[] = [
    { name: 'alerts in the feed', value: n, unit: 'alerts' },
    { name: 'time from feed fetch to the last AlertReady', value: totalMs / 1000, unit: 's' },
    { name: 'alerts per second', value: totalMs > 0 ? (n / totalMs) * 1000 : 0, unit: 'alerts/s' },
    { name: 'per-alert latency p50', value: percentile(latencies, 0.5), unit: 'ms' },
    { name: 'per-alert latency p99', value: percentile(latencies, 0.99), unit: 'ms' },
    { name: 'per-alert latency max', value: latencies[latencies.length - 1] ?? 0, unit: 'ms' },
    { name: 'AlertReady published', value: b.ready.length, unit: 'messages' },
    { name: 'rejected (degenerate)', value: counts.rejected, unit: 'alerts' },
    { name: 'applied (planned)', value: counts.applied, unit: 'alerts' },
    { name: 'peak alerts resolved at once', value: b.peakInFlight(), unit: 'alerts' },
  ];
  return {
    counts,
    result: {
      name: `ingest storm (${n} alerts)`,
      verdict: checks.every((c) => c.ok) ? 'pass' : 'fail',
      durationMs: Date.now() - t0,
      metrics,
      checks,
      notes: [
        `Latency is measured from the start of the feed request to each AlertReady; the pipeline resolves up to ${FEATURE_CONCURRENCY} alert chains at once and orders each chain oldest first. Zone lookups go over HTTP to the mock NWS.`,
        'The gate check runs on the simulated clock: the poll loop would wait 30 s between requests, and a request at the same instant is answered "throttled" without reaching the server.',
      ],
    },
  };
}

// ---------- Fan-out of one alert with the real planner, shard worker and sender ----------

class MiniFanout {
  private readonly pushes = new Map<string, number>();
  private readonly idem: MemoryIdempotencyStore;
  private index = new MemoryCellIndex();
  constructor(private readonly b: Bench) {
    this.idem = new MemoryIdempotencyStore({ now: () => new Date(b.clock.now()) });
  }

  total(): number {
    let n = 0;
    for (const v of this.pushes.values()) n += v;
    return n;
  }
  maxPerToken(): number {
    return Math.max(0, ...this.pushes.values());
  }

  /** One device per alert cell (at most `devices`). */
  async seedDevices(alert: Alert, devices: number): Promise<number> {
    const cells = alert.area.h3Cells.slice(0, devices);
    await this.index.put(
      cells.map((h3Cell, i) => ({
        h3Cell,
        deviceId: `dev-${alert.id.slice(0, 8)}-${i}`,
        placeId: 'home',
        target: {
          platform: 'ios' as const,
          token: `${i.toString(16).padStart(8, '0')}${'ab'.repeat(28)}`,
        },
        language: 'en' as const,
        deviceCells: [h3Cell],
      })),
    );
    return cells.length;
  }

  /** Plans the alert and sends every batch: what the planner Lambda, shard Lambdas and sender do. */
  async run(ready: { alertId: string; sourceId: string; severity: string }): Promise<void> {
    const { b } = this;
    const clock = { now: () => new Date(b.clock.now()) };
    const logger: Logger = { info: () => undefined, warn: () => undefined, error: () => undefined };
    const queue = new MemorySendQueue();
    const alerts = { get: (id: string) => b.store.get(id) };
    const common = {
      alerts,
      verifyAlert: b.verify,
      isDeliveryPaused: async () => false,
      clock,
      logger,
    };
    const jobs: Parameters<typeof processShard>[0][] = [];
    await planShards(
      {
        alertId: ready.alertId,
        sourceId: ready.sourceId,
        relaySeverity: ready.severity as Alert['relaySeverity'],
      },
      {
        ...common,
        shardDispatcher: {
          async dispatch(js) {
            jobs.push(...js);
          },
        },
      },
    );
    for (const job of jobs) {
      await processShard(job, {
        ...common,
        cellIndex: this.index,
        sendQueue: queue,
        makeCheckInToken: () => 'check-in-token',
      });
    }
    const push: NativePushSender = {
      send: async (messages) =>
        messages.map((m) => {
          this.pushes.set(m.target.token, (this.pushes.get(m.target.token) ?? 0) + 1);
          return { ok: true as const };
        }),
    };
    for (const lane of ['urgent', 'standard'] as const) {
      for (const body of queue.messages[lane]) {
        for (const batch of parseBatches(body) ?? []) {
          await processBatch(batch, {
            ...common,
            idempotency: this.idem,
            push,
            sendQueue: queue,
            onDeadTarget: async () => undefined,
            sleep: async () => undefined,
            writeMetric: () => undefined,
          });
        }
      }
    }
  }
}

// ---------- Scenario b: flapping ----------

async function scenarioFlapping(
  ctx: SuiteContext,
  nws: string,
  b: Bench,
  model: FeedModel,
  n: number,
): Promise<ScenarioResult> {
  const t0 = Date.now();
  const flapCount = Math.max(2, Math.round(n / 20));
  const flap = new Set(model.pushedOnce.slice(0, flapCount));
  const tSource = model.pushedOnce[flapCount] as string;
  const tAlert = (await b.store.findBySourceId(tSource)) as Alert;
  const readyBefore = new Map<string, number>();
  for (const s of flap) {
    const a = (await b.store.findBySourceId(s)) as Alert;
    readyBefore.set(s, b.ready.filter((r) => r.alertId === a.id).length);
  }

  // The first fan-out of T, with the real planner / shard worker / sender.
  const fan = new MiniFanout(b);
  const devices = await fan.seedDevices(tAlert, 25);
  await fan.run({ alertId: tAlert.id, sourceId: tAlert.sourceId, severity: tAlert.relaySeverity });
  const firstPushes = fan.total();

  clearReconcileState(); // absence counters start from this scenario
  const all = model.features.map((f) => ({ id: f.sourceId, feature: f.feature }));
  let tReturned = false;
  let k = 0;
  const simStart = b.clock.now();
  const snapshots: Array<{
    k: number;
    elapsedS: number;
    t: Alert['state'] | 'missing';
    flapCancelled: number;
  }> = [];
  const feedFor = (index: number): object[] =>
    all
      .filter((f) => !(index < 2 && flap.has(f.id)) && !(f.id === tSource && !tReturned))
      .map((f) => f.feature);
  const snapshot = async (index: number): Promise<void> => {
    const t = (await b.store.findBySourceId(tSource))?.state ?? 'missing';
    let flapCancelled = 0;
    for (const s of flap)
      if ((await b.store.findBySourceId(s))?.state === 'cancelled') flapCancelled++;
    snapshots.push({ k: index, elapsedS: (b.clock.now() - simStart) / 1000, t, flapCancelled });
    if (t === 'cancelled') tReturned = true; // NWS lists it again from the next read
  };
  await setFeed(nws, feedFor(0));
  const requestsBefore = b.feedRequests.length;
  const polls = 14;
  await pollLoop(b.deps, {
    polls,
    intervalMs: MIN_POLL_INTERVAL_MS,
    now: b.clock.now,
    remainingMs: () => 1e12,
    sleep: async (ms) => {
      b.clock.advance(ms);
      await snapshot(k);
      k++;
      await setFeed(nws, feedFor(k));
    },
  });
  await snapshot(polls - 1);

  // ----- correctness -----
  const checks: Check[] = [];
  const cancelledAt = snapshots.find((s) => s.t === 'cancelled');
  const absentSinceS = 0; // T is missing from the very first read
  const before5 = snapshots.filter(
    (s) => s.elapsedS - absentSinceS < RECONCILE_MIN_ABSENT_MS / 1000,
  );
  checks.push({
    name: `flapping alerts (absent from 2 reads) never ended (needs ${RECONCILE_AFTER} reads)`,
    ok: snapshots.every((s) => s.flapCancelled === 0),
    detail: `${flapCount} alerts, ${snapshots.length} snapshots`,
  });
  checks.push({
    name: 'flapping alerts not planned again when they returned',
    ok: [...flap].every((s) => {
      const a = tAlertCache.get(s);
      return (
        a === undefined || b.ready.filter((r) => r.alertId === a).length === readyBefore.get(s)
      );
    }),
    detail: 'AlertReady count unchanged',
  });
  checks.push({
    name: 'absent alert kept alive for the whole first 5 minutes',
    ok: before5.every((s) => s.t === 'active' || s.t === 'updated'),
    detail: `states during the first ${before5.length} reads: ${[...new Set(before5.map((s) => s.t))].join(', ')}`,
  });
  checks.push({
    name: 'absent alert ended once absent for more than 5 minutes',
    ok:
      cancelledAt !== undefined &&
      cancelledAt.elapsedS >= RECONCILE_MIN_ABSENT_MS / 1000 &&
      cancelledAt.elapsedS <= RECONCILE_MIN_ABSENT_MS / 1000 + 2 * (MIN_POLL_INTERVAL_MS / 1000),
    detail: cancelledAt
      ? `ended at read ${cancelledAt.k + 1}, ${cancelledAt.elapsedS} s`
      : 'never ended',
  });
  const endedLogs = b.logs.filter(
    (l) => l.msg === 'alert.ended_by_reconciliation' && l.fields['alertId'] === tAlert.id,
  );
  checks.push({
    name: 'only the absent alert was ended by reconciliation',
    ok:
      endedLogs.length === 1 &&
      b.logs.filter((l) => l.msg === 'alert.ended_by_reconciliation').length === 1,
    detail: `${b.logs.filter((l) => l.msg === 'alert.ended_by_reconciliation').length} ended`,
  });
  const final = await b.store.findBySourceId(tSource);
  const restoredLogs = b.logs.filter(
    (l) => l.msg === 'alert.restored_after_reconciliation' && l.fields['alertId'] === tAlert.id,
  );
  checks.push({
    name: 'the alert was restored when it reappeared',
    ok: (final?.state === 'active' || final?.state === 'updated') && restoredLogs.length === 1,
    detail: `state ${final?.state}, restored ${restoredLogs.length}x`,
  });
  const readyT = b.ready.filter((r) => r.alertId === tAlert.id);
  checks.push({
    name: 'restoration re-plans the same alert version (same alert id and source id)',
    ok: readyT.length === 2 && readyT.every((r) => r.sourceId === tAlert.sourceId),
    detail: `${readyT.length} AlertReady for it`,
  });
  await fan.run({
    alertId: tAlert.id,
    sourceId: tAlert.sourceId,
    severity: (final ?? tAlert).relaySeverity,
  });
  const secondPushes = fan.total() - firstPushes;
  checks.push({
    name: 'no duplicate push after restoration (re-plan sends nothing new)',
    ok: firstPushes === devices && secondPushes === 0 && fan.maxPerToken() === 1,
    detail: `${firstPushes}/${devices} pushes the first time, ${secondPushes} the second`,
  });
  // The gate: simulated time between requests that reached the mock.
  const times = b.feedRequests.slice(requestsBefore).map((r) => r.simMs);
  const gaps = times.slice(1).map((t, i) => t - (times[i] as number));
  checks.push({
    name: 'requests to NWS at least 30 s apart (simulated)',
    ok: gaps.length > 0 && gaps.every((g) => g >= MIN_POLL_INTERVAL_MS),
    detail: `${times.length} requests, smallest gap ${Math.min(...gaps) / 1000} s`,
  });

  return {
    name: `feed flapping and reconciliation (${polls} reads, ${flapCount} flapping alerts)`,
    verdict: checks.every((c) => c.ok) ? 'pass' : 'fail',
    durationMs: Date.now() - t0,
    metrics: [
      { name: 'simulated time covered', value: (b.clock.now() - simStart) / 60000, unit: 'min' },
      { name: 'feed reads', value: polls, unit: 'reads' },
      { name: 'flapping alerts', value: flapCount, unit: 'alerts' },
      {
        name: 'absent alert ended after',
        value: cancelledAt?.elapsedS ?? -1,
        unit: 's (simulated)',
      },
      { name: 'pushes at the first fan-out', value: firstPushes, unit: 'pushes' },
      {
        name: 'new pushes after restoration',
        value: secondPushes,
        unit: 'pushes',
        target: { op: '==', value: 0 },
      },
    ],
    checks,
    notes: [
      'Reconciliation ends an alert only after it is missing from 3 consecutive full reads and for 5 minutes; the 5 minutes pass on the simulated clock that the poll loop sleeps on.',
      'The no-duplicate check runs the real planner, shard worker and sender (in-memory index and idempotency store) for the restored alert: its batch keys are unchanged, so every claim answers "sent".',
    ],
  };
}

/** alert id by source id of the flapping alerts, filled lazily by the check above. */
const tAlertCache = new Map<string, string>();

// ---------- Scenario c: expiredReferences ----------

async function scenarioExpired(
  nws: string,
  b: Bench,
  model: FeedModel,
  baseMs: number,
): Promise<ScenarioResult> {
  const t0 = Date.now();
  const expires = baseMs + 6 * 3600_000;
  const sent = baseMs - 120_000;
  const mk = (id: string, extra: Partial<Parameters<typeof makeFeature>[0]> = {}): object =>
    makeFeature({
      sourceId: `urn:oid:2.49.0.1.840.0.t6c.${id}`,
      sentMs: sent,
      expiresMs: expires,
      geometry: {
        type: 'Polygon',
        coordinates: square(-121.5 + Number(id.length) * 0.01, 37.5, 0.03),
      },
      ...extra,
    });
  const idOf = (id: string): string => `urn:oid:2.49.0.1.840.0.t6c.${id}`;
  const geom = (lng: number): object => ({ type: 'Polygon', coordinates: square(lng, 38.2, 0.03) });
  const P = mk('P', { geometry: geom(-121.1) });
  const Q = mk('Q', { geometry: geom(-121.2) });
  const R = mk('R', { geometry: geom(-121.3) });
  // S names Q (and an id nobody has) as expired; P2 updates P and names P (its predecessor) as expired.
  const S = mk('S', {
    geometry: geom(-121.4),
    expiredReferences: [expiredRef(idOf('Q'), sent), expiredRef('urn:oid:does.not.exist', sent)],
  });
  const P2 = mk('P2', {
    geometry: geom(-121.1),
    messageType: 'Update',
    sentMs: sent + 60_000,
    references: [{ sourceId: idOf('P'), sentMs: sent }],
    expiredReferences: [expiredRef(idOf('P'), sent)],
  });
  b.clock.advance(MIN_POLL_INTERVAL_MS);
  await setFeed(nws, [...model.features.map((f) => f.feature), P, Q, R, S, P2]);
  const endedBefore = b.logs.filter((l) => l.msg === 'alert.ended_by_reconciliation').length;
  const counts = await pollOnce(b.deps);

  const state = async (id: string): Promise<Alert | null> => b.store.findBySourceId(idOf(id));
  const [a, q, r, s] = await Promise.all([state('P'), state('Q'), state('R'), state('S')]);
  const ended = b.logs.filter((l) => l.msg === 'alert.ended_by_reconciliation').slice(endedBefore);
  const everyoneElse = (
    await Promise.all(model.pushedOnce.map(async (id) => (await b.store.findBySourceId(id))?.state))
  ).filter((st) => st === 'cancelled').length;
  const chainsLive = (
    await Promise.all(
      [...model.chains3.keys()].map(async (id) => (await b.store.findBySourceId(id))?.state),
    )
  ).every((st) => st === 'updated');
  const checks: Check[] = [
    {
      name: 'the alert named in expiredReferences was cancelled',
      ok: q?.state === 'cancelled',
      detail: `Q is ${q?.state}`,
    },
    {
      name: 'the alerts around it were not touched',
      ok:
        a?.state === 'updated' &&
        a.sourceId === idOf('P2') &&
        r?.state === 'active' &&
        s?.state === 'active',
      detail: `P ${a?.state} (${a?.sourceId.slice(-2)}), R ${r?.state}, S ${s?.state}`,
    },
    {
      name: 'exactly one alert ended, with reason expired_reference',
      ok:
        ended.length === 1 &&
        ended[0]?.fields['reason'] === 'expired_reference' &&
        ended[0]?.fields['alertId'] === q?.id,
      detail: `${ended.length} ended`,
    },
    {
      name: 'a replaced predecessor (Update lists the old message) is not an ending',
      ok: a?.state === 'updated' && chainsLive,
      detail: 'update chains stay updated',
    },
    {
      name: 'no other alert in the store was cancelled',
      ok: everyoneElse === 0,
      detail: `${everyoneElse} cancelled`,
    },
    {
      name: 'no ingest errors',
      ok: counts.errors === 0,
      detail: `${counts.errors} errors`,
    },
  ];
  return {
    name: 'expiredReferences cancels the right alert only',
    verdict: checks.every((c) => c.ok) ? 'pass' : 'fail',
    durationMs: Date.now() - t0,
    metrics: [
      { name: 'features in the read', value: counts.fetched, unit: 'alerts' },
      { name: 'alerts ended', value: ended.length, unit: 'alerts', target: { op: '==', value: 1 } },
    ],
    checks,
    notes: [
      'Alert S lists Q and an unknown id in parameters.expiredReferences; update P2 lists its own predecessor P. Only Q ends.',
    ],
  };
}

// ---------- The test ----------

export const t6Ingest: SuiteTest = {
  id: 't6',
  title: 'Ingest storm',
  description:
    'A mock NWS process serves 200 concurrent California alerts (polygons, zones, degenerate geometries, update and cancel chains) to the real ingest pipeline; then feed flapping, reconciliation, restoration and expiredReferences.',
  estimatedMinutes: 2,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const t0 = Date.now();
    const n = ctx.scale === 'full' ? 200 : 20;
    const scenarios: ScenarioResult[] = [];
    let error: string | undefined;
    const rss = new RssSampler();
    const worker = spawnWorker(file('mock-nws-worker.ts'), [], {
      heapMb: ctx.scale === 'full' ? 512 : 256,
    });
    rss.addSource(() => worker.rssNow);
    try {
      const port = await new Promise<number>((resolve, reject) => {
        worker.onMessage((v) => {
          const m = v as { port?: number };
          if (typeof m.port === 'number') resolve(m.port);
        });
        worker.result.then(
          () => reject(new Error('mock NWS exited early')),
          (e: unknown) => reject(e instanceof Error ? e : new Error(String(e))),
        );
      });
      const nws = `http://127.0.0.1:${port}`;
      const baseMs = Date.now();
      const clock = new SimClock(baseMs);
      clearReconcileState();
      clearSaveFailures();
      const bench = makeBench(nws, clock);
      const model = buildFeed(n, baseMs);

      const storm = await scenarioStorm(ctx, nws, bench, model, n);
      scenarios.push(storm.result);
      // cache the alert ids of the flapping alerts for the "not planned again" check
      for (const s of model.pushedOnce.slice(0, Math.max(2, Math.round(n / 20)))) {
        const a = await bench.store.findBySourceId(s);
        if (a) tAlertCache.set(s, a.id);
      }
      scenarios.push(await scenarioFlapping(ctx, nws, bench, model, n));
      scenarios.push(await scenarioExpired(nws, bench, model, baseMs));
      await sleep(0);
    } catch (e) {
      error = e instanceof Error ? (e.stack ?? e.message) : String(e);
      ctx.log(`t6 failed: ${error}`);
    } finally {
      worker.kill();
    }
    const peak = rss.stop();
    const verdict =
      !error && scenarios.length === 3 && scenarios.every((s) => s.verdict === 'pass')
        ? 'pass'
        : 'fail';
    const result: TestResult = {
      id: 't6',
      title: t6Ingest.title,
      verdict,
      startedAt,
      durationMs: Date.now() - t0,
      scenarios,
      peakRssMb: peak,
      ...(error ? { error } : {}),
    };
    writeTestReport(
      ctx,
      result,
      `A mock NWS server process serves a feed of ${fmt(n)} California alerts to the real ingest pipeline (\`createIngestPipeline\` and \`pollOnce\` / \`pollLoop\` from backend/src/ingest) with an in-memory alert store, the real signing store, the real zone lookup over HTTP and the plan queue replaced by a recorder. The pipeline's clock is simulated: the poll loop's sleep advances it, so the 30 s poll gate and the 3-reads-and-5-minutes reconciliation rule are exercised without waiting.`,
    );
    return result;
  },
};
