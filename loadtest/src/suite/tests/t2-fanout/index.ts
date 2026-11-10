/**
 * Test 2: multi-process fan-out. The real planner, shard worker and sender code, each role in its
 * own OS processes, against mock APNs (HTTP/2 + TLS) and mock FCM (one quota bucket per Firebase
 * project) in their own processes, with SQS and the idempotency table in a broker process. The
 * mocks record every accepted push; the parent checks exactly-once against the on-disk seed.
 *
 *   parent: seed orchestration, REAL planShards, shard-job dispatch (like async Lambda invokes),
 *           completion detection, verification, report
 *   seed-worker x P      generate and sort the on-disk cell index (once, cached in ctx.dataDir)
 *   broker               SQS urgent/standard queues + shared idempotency store (TCP)
 *   shard-worker x S     REAL processShard -> REAL SqsSendQueue -> broker
 *   sender x N           REAL SenderConsumer + processBatch + ApnsSender/FcmProjectSender
 *   mock-apns x K, mock-fcm x M (each hosting some project servers)
 */
import { readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { cellToParent } from 'h3-js';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import { DEFAULT_FCM_PROJECT_RATE } from '../../../../../backend/src/adapters/push/fcm-projects.js';
import {
  DENSE_CELL_RECIPIENTS,
  type ShardJob,
} from '../../../../../backend/src/scale/contracts.js';
import { CachedAlertReader } from '../../../../../backend/src/scale/fanout/alert-cache.js';
import { planShards } from '../../../../../backend/src/scale/fanout/planner.js';
import type { ShardResult } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import { buildAlert, createAlertKeys, verifierFromPem, type AlertKeys } from './alert.js';
import {
  resolveConfig,
  scenarioList,
  type Scenario,
  type T2Config,
  type T2Overrides,
} from './config.js';
import { spawnWorker, type Worker } from './proc.js';
import { writeReport } from './report.js';
import {
  SEED_FORMAT,
  STATE_CHUNK,
  ensureDir,
  readManifest,
  seedDirFor,
  type Region,
  type SeedManifest,
} from './seed-data.js';
import {
  HIST_BUCKETS,
  concat,
  exactlyOnce,
  histMerge,
  histQuantile,
  sortedUnique,
} from './verify.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const file = (name: string) => path.join(HERE, name);
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
/** SQS allows 256 KB per message; the plan is to stay under 240 KB. */
const SQS_BODY_LIMIT_KB = 240;
const round = (n: number, d = 0) => Math.round(n * 10 ** d) / 10 ** d;

export type { T2Overrides } from './config.js';

// ---------- seeds ----------

interface SeedInfo {
  dir: string;
  manifest: SeedManifest;
  /** 0 when the seed was already on disk. */
  seedMs: number;
}

async function ensureSeed(
  ctx: SuiteContext,
  cfg: T2Config,
  region: Region,
  devices: number,
  denseDevices = 0,
): Promise<SeedInfo> {
  const dir = seedDirFor(ctx.dataDir, region, devices, cfg.seed, denseDevices);
  const cached = readManifest(dir);
  if (cached && cached.devices === devices && cached.format === SEED_FORMAT) {
    ctx.log(`t2: seed ${region} ${devices} devices cached`, { dir });
    return { dir, manifest: cached, seedMs: 0 };
  }
  const t0 = performance.now();
  rmSync(dir, { recursive: true, force: true });
  ensureDir(dir);
  const chunks = region !== 'state' ? 1 : Math.ceil(devices / STATE_CHUNK);
  const procs = Math.max(1, Math.min(ctx.workers, 8, region !== 'state' ? 2 : chunks));
  ctx.log(`t2: seeding ${region} ${devices} devices (${chunks} chunk(s), ${procs} process(es))`);
  const workers = await Promise.all(
    Array.from({ length: procs }, (_, i) =>
      spawnWorker(file('seed-worker.ts'), { role: `seed-${i}`, heapMb: cfg.heapMb.seed }),
    ),
  );
  try {
    const totals = { entries: 0, ios: 0, android: 0, multiCell: 0 };
    const shards = new Set<string>();
    let next = 0;
    await Promise.all(
      workers.map(async (w) => {
        while (next < chunks) {
          const chunk = next++;
          const r = await w.request<typeof totals & { shards: string[] }>('gen', {
            dir,
            region,
            devices,
            seed: cfg.seed,
            chunk,
            denseDevices,
          });
          totals.entries += r.entries;
          totals.ios += r.ios;
          totals.android += r.android;
          totals.multiCell += r.multiCell;
          for (const s of r.shards) shards.add(s);
        }
      }),
    );
    const list = [...shards].sort();
    const per = workers.map(() => [] as string[]);
    list.forEach((s, i) => per[i % workers.length]?.push(s));
    await Promise.all(workers.map((w, i) => w.request('sort', { dir, shards: per[i] })));
    await workers[0]?.request('cleanup', { dir });
    const manifest: SeedManifest = {
      format: SEED_FORMAT,
      region,
      devices,
      seed: cfg.seed,
      ...totals,
      shards: list.length,
      generatedMs: Math.round(performance.now() - t0),
    };
    writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify(manifest, null, 2));
    ctx.log(`t2: seeded ${region} in ${round(manifest.generatedMs / 1000, 1)} s`, { ...totals });
    return { dir, manifest, seedMs: manifest.generatedMs };
  } finally {
    await Promise.all(workers.map((w) => w.stop()));
  }
}

// ---------- infrastructure ----------

interface Infra {
  broker: Worker;
  brokerPort: number;
  apns: Array<{ w: Worker; url: string; cert: string }>;
  fcmProcs: Worker[];
  fcmUrls: Map<string, string>;
  projectIds: string[];
  shards: Worker[];
  senders: Worker[];
  all(): Worker[];
}

const projectId = (i: number) => `relay-t2-p${i}`;

async function startInfra(cfg: T2Config, keys: AlertKeys, maxProjects: number): Promise<Infra> {
  const broker = await spawnWorker(file('broker-worker.ts'), {
    role: 'broker',
    heapMb: cfg.heapMb.broker,
  });
  const { port: brokerPort } = await broker.request<{ port: number }>('start', {
    capMb: cfg.brokerCapMb,
  });
  const projectIds = Array.from({ length: maxProjects }, (_, i) => projectId(i));
  const fcmProcCount = Math.min(cfg.fcmMocks, maxProjects);
  const [apns, fcmProcs, shards, senders] = await Promise.all([
    Promise.all(
      Array.from({ length: cfg.apnsMocks }, async (_, i) => {
        const w = await spawnWorker(file('mock-worker.ts'), {
          role: 'mock-apns',
          heapMb: cfg.heapMb.mock,
        });
        const r = await w.request<{ port: number; cert: string }>('startApns', { seed: 100 + i });
        return { w, url: `https://127.0.0.1:${r.port}`, cert: r.cert };
      }),
    ),
    Promise.all(
      Array.from({ length: fcmProcCount }, () =>
        spawnWorker(file('mock-worker.ts'), { role: 'mock-fcm', heapMb: cfg.heapMb.mock }),
      ),
    ),
    Promise.all(
      Array.from({ length: cfg.shardWorkers }, async () => {
        const w = await spawnWorker(file('shard-worker.ts'), {
          role: 'shard-worker',
          heapMb: cfg.heapMb.shard,
        });
        await w.request('init', { brokerPort, publicPem: keys.publicPem });
        return w;
      }),
    ),
    Promise.all(
      Array.from({ length: cfg.senders }, async () => {
        const w = await spawnWorker(file('sender-worker.ts'), {
          role: 'sender',
          heapMb: cfg.heapMb.sender,
        });
        await w.request('init', { brokerPort, publicPem: keys.publicPem });
        return w;
      }),
    ),
  ]);
  const fcmUrls = new Map<string, string>();
  await Promise.all(
    fcmProcs.map(async (w, i) => {
      const mine = projectIds.filter((_, j) => j % fcmProcs.length === i);
      const r = await w.request<{ ports: Record<string, number> }>('startFcm', {
        projects: mine,
        seed: 500 + i * 1000,
        quotaPerSec: cfg.fcmQuotaPerSec,
      });
      for (const [id, port] of Object.entries(r.ports)) fcmUrls.set(id, `http://127.0.0.1:${port}`);
    }),
  );
  return {
    broker,
    brokerPort,
    apns,
    fcmProcs,
    fcmUrls,
    projectIds,
    shards,
    senders,
    all: () => [broker, ...apns.map((a) => a.w), ...fcmProcs, ...shards, ...senders],
  };
}

// ---------- one scenario ----------

interface BrokerStats {
  empty: boolean;
  queues: Record<string, { visible: number; inflight: number; delayed: number }>;
  peakBytesMb: number;
  sentMessages: number;
  sentDelayed: number;
  receives: number;
  redeliveries: number;
  dlq: number;
  producerWaits: number;
  producerWaitMs: number;
  idemKeys: number;
  idemOps: number;
  /** Largest SQS message body the shard workers or senders sent, bytes. */
  maxBodyBytes: number;
  /** Messages whose body was over 240 KB (SQS allows 256 KB). */
  overBudgetMessages: number;
}

interface PlatformStats {
  ok: number;
  retryable: number;
  permanent: number;
  firstOk: number;
  lastOk: number;
}

interface SenderStats {
  ios: PlatformStats;
  android: PlatformStats;
  batchHist: Uint32Array;
  timeline: Array<[number, number]>;
  reasons: Record<string, number>;
  errors: Record<string, number>;
  logEvents: Record<string, number>;
  sent: number;
  retried: number;
  gaveUp: number;
  permanentFailed: number;
  batches: number;
  retryBatches: number;
  deadTargets: number;
}

interface MockStats {
  received: number;
  ok: number;
  unregistered: number;
  tooManyRequests: number;
  serverError: number;
  rejected: number;
  rateLimited: number;
  goaways: number;
}

export interface ScenarioRaw {
  scenario: Scenario;
  seed: SeedInfo;
  alertCells: number;
  shardJobs: number;
  planMs: number;
  shardPhaseMs: number;
  lastPushMs: number;
  firstPushMs: number;
  iosLastMs: number;
  androidLastMs: number;
  totalOk: number;
  iosOk: number;
  androidOk: number;
  perSenderOk: number[];
  senderCpu: number[];
  wallMs: number;
  batchP50: number;
  batchP95: number;
  /** Pushes/s between 5% and 95% of all accepted pushes (no ramp-up, no retry tail). */
  steadyRate: number;
  /** Time from AlertReady to 50% and 99% of all accepted pushes, ms. */
  t50Ms: number;
  t99Ms: number;
  peakRss: Record<string, number>;
  cpuPct: Record<string, number>;
  broker: BrokerStats;
  mocks: MockStats;
  senders: SenderStats[];
  /** peakBufferedRecipients is the largest of any one shard job; the rest are sums. */
  shardTotals: Omit<ShardResult, 'refused' | 'continuedAt'> & {
    failedJobs: number;
    refused: number;
  };
  eo: ReturnType<typeof exactlyOnce>;
  fcmRatePerSender: number;
  fcmRateSource: string;
  error?: string;
}

async function runScenario(
  ctx: SuiteContext,
  cfg: T2Config,
  infra: Infra,
  keys: AlertKeys,
  sc: Scenario,
  seed: SeedInfo,
  n: number,
): Promise<ScenarioRaw> {
  const alert: Alert = await buildAlert(keys, n, sc.areaName, sc.ring);
  const projects = infra.projectIds.slice(0, sc.projects);
  const active = infra.senders.slice(0, sc.senders);
  await Promise.all([
    infra.broker.request('reset'),
    ...infra.apns.map((a) => a.w.request('reset')),
    ...infra.fcmProcs.map((w) => w.request('reset')),
    ...infra.shards.map((w) =>
      w.request('scenario', {
        alert,
        seedDir: seed.dir,
        fcmProjects: sc.projects > 1 ? projects : [],
      }),
    ),
  ]);
  for (const w of infra.all()) w.mark();
  const started = await Promise.all(
    active.map((w, i) => {
      const a = infra.apns[i % infra.apns.length];
      return w.request('start', {
        alert,
        apnsUrl: a?.url,
        apnsCert: a?.cert,
        fcm: projects.map((id) => ({ projectId: id, url: infra.fcmUrls.get(id) })),
        senderCount: sc.senders,
        fcmRateMode: cfg.fcmRateMode,
        concurrency: cfg.senderConcurrency,
      });
    }),
  );
  const fcmRatePerSender = (started[0] as { fcmRate: number }).fcmRate;
  const fcmRateSource = (started[0] as { fcmRateSource: string }).fcmRateSource;
  ctx.log(
    `t2: ${sc.name}: ${sc.devices} devices, ${alert.area.h3Cells.length} alert cells, ${sc.senders} senders, ${sc.projects} FCM project(s)`,
  );

  // ----- AlertReady -> the real planner -> shard jobs (dispatched like async Lambda invokes) -----
  const jobs: ShardJob[] = [];
  const shardTotals = {
    denseCells: 0,
    coveredBuckets: 0,
    peakBufferedRecipients: 0,
    rereadPasses: 0,
    cells: 0,
    entries: 0,
    skippedNoTarget: 0,
    skippedNonPrimary: 0,
    duplicates: 0,
    recipients: 0,
    batches: 0,
    failedJobs: 0,
    refused: 0,
  };
  let nextJob = 0;
  let jobsDone = 0;
  let shardsDoneAt = 0;
  const inflight = new Map<Worker, number>(infra.shards.map((w) => [w, 0]));
  let jobError: string | undefined;
  const runJob = async (w: Worker, job: ShardJob): Promise<void> => {
    for (let attempt = 0; ; attempt++) {
      try {
        const r = await w.request<ShardResult>('shard', { job });
        if (r.refused) shardTotals.refused++;
        shardTotals.cells += r.cells;
        shardTotals.entries += r.entries;
        shardTotals.skippedNoTarget += r.skippedNoTarget;
        shardTotals.skippedNonPrimary += r.skippedNonPrimary;
        shardTotals.duplicates += r.duplicates;
        shardTotals.recipients += r.recipients;
        shardTotals.batches += r.batches;
        shardTotals.denseCells += r.denseCells ?? 0;
        shardTotals.coveredBuckets += r.coveredBuckets ?? 0;
        shardTotals.rereadPasses += r.rereadPasses ?? 0;
        shardTotals.peakBufferedRecipients = Math.max(
          shardTotals.peakBufferedRecipients,
          r.peakBufferedRecipients ?? 0,
        );
        return;
      } catch (err) {
        // Lambda retries a failed async invoke twice.
        if (attempt >= 2 || !w.alive) {
          shardTotals.failedJobs++;
          jobError ??= err instanceof Error ? err.message : String(err);
          return;
        }
      }
    }
  };
  const pump = (): void => {
    for (const w of infra.shards) {
      while (nextJob < jobs.length && (inflight.get(w) ?? 0) < cfg.jobsPerShardWorker) {
        const job = jobs[nextJob++] as ShardJob;
        inflight.set(w, (inflight.get(w) ?? 0) + 1);
        void runJob(w, job).then(() => {
          inflight.set(w, (inflight.get(w) ?? 1) - 1);
          jobsDone++;
          if (jobsDone === jobs.length) shardsDoneAt = Date.now();
          pump();
        });
      }
    }
  };
  const store = new InMemoryAlertStore();
  await store.save(alert);
  const verify = verifierFromPem(keys.publicPem);
  const t0 = Date.now();
  const plan = await planShards(
    { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
    {
      alerts: new CachedAlertReader(store),
      verifyAlert: (a) => verify(a),
      isDeliveryPaused: async () => false,
      clock: { now: () => new Date() },
      logger: { info: () => {}, warn: () => {}, error: () => {} },
      shardDispatcher: {
        dispatch: async (js) => {
          jobs.push(...js);
          pump();
        },
      },
    },
  );
  const planMs = Date.now() - t0;
  if (plan.refused) throw new Error(`planner refused the alert: ${plan.refused}`);
  if (jobs.length === 0) shardsDoneAt = Date.now();

  // ----- wait until every shard job is done and the queues are drained -----
  let last = '';
  let lastChange = Date.now();
  let lastLog = Date.now();
  let error: string | undefined;
  for (;;) {
    await sleep(cfg.scale === 'smoke' ? 100 : 250);
    const b = await infra.broker.request<BrokerStats>('stats');
    if (shardsDoneAt > 0 && b.empty) break;
    const sig = `${jobsDone}/${b.sentMessages}/${b.receives}/${b.dlq}`;
    if (sig !== last) {
      last = sig;
      lastChange = Date.now();
    } else if (Date.now() - lastChange > cfg.stallMs) {
      error = `no progress for ${cfg.stallMs / 1000} s (shards ${jobsDone}/${jobs.length}, queues ${JSON.stringify(b.queues)})`;
      break;
    }
    if (Date.now() - lastLog > 10_000) {
      lastLog = Date.now();
      ctx.log(
        `t2: ${sc.key} t=${round((Date.now() - t0) / 1000)}s shards ${jobsDone}/${jobs.length}`,
        {
          queues: b.queues,
          received: b.receives,
        },
      );
    }
    if (infra.all().some((w) => !w.alive)) {
      error = `a child process died (${infra
        .all()
        .filter((w) => !w.alive)
        .map((w) => w.role)
        .join(', ')})`;
      break;
    }
  }
  const wallMs = Date.now() - t0;
  const senderStats = await Promise.all(active.map((w) => w.request<SenderStats>('stop')));
  const brokerStats = await infra.broker.request<BrokerStats>('stats');
  const mockWorkers = [...infra.apns.map((a) => a.w), ...infra.fcmProcs];
  const mockStatsList = await Promise.all(mockWorkers.map((w) => w.request<MockStats>('stats')));

  // ----- exactly-once against the seed -----
  const runDir = path.join(ctx.dataDir, 't2-run');
  ensureDir(runDir);
  const dumps = await Promise.all(
    mockWorkers.map(async (w, i) => {
      const deliveredFile = path.join(runDir, `delivered-${i}.bin`);
      const deadFile = path.join(runDir, `dead-${i}.bin`);
      await w.request('dump', { deliveredFile, deadFile });
      const load = (f: string) => {
        const b = readFileSync(f);
        return new BigUint64Array(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
      };
      return { delivered: load(deliveredFile), dead: load(deadFile) };
    }),
  );
  rmSync(runDir, { recursive: true, force: true });
  const byShard = new Map<string, string[]>();
  for (const cell of alert.area.h3Cells) {
    const s = cellToParent(cell, 4);
    let l = byShard.get(s);
    if (!l) byShard.set(s, (l = []));
    l.push(cell);
  }
  const groups = infra.shards.map(() => [] as string[]);
  [...byShard.values()].forEach((cells, i) => groups[i % groups.length]?.push(...cells));
  const expectedParts = await Promise.all(
    infra.shards.map((w, i) =>
      w.request<{ hashes: BigUint64Array }>('expected', { seedDir: seed.dir, cells: groups[i] }),
    ),
  );
  const expected = sortedUnique(concat(expectedParts.map((p) => p.hashes)));
  const eo = exactlyOnce(
    expected,
    concat(dumps.map((d) => d.delivered)),
    concat(dumps.map((d) => d.dead)),
  );

  // ----- timings and resources -----
  const lastOf = (k: 'ios' | 'android') => Math.max(0, ...senderStats.map((s) => s[k].lastOk));
  const firstOk = Math.min(
    ...senderStats.flatMap((s) => [s.ios.firstOk, s.android.firstOk]).filter((t) => t > 0),
  );
  const lastOk = Math.max(lastOf('ios'), lastOf('android'));
  const slots = new Map<number, number>();
  for (const s of senderStats)
    for (const [k, v] of s.timeline) slots.set(k, (slots.get(k) ?? 0) + v);
  const timeline = [...slots].sort((a, b) => a[0] - b[0]);
  const totalSlots = timeline.reduce((acc, [, v]) => acc + v, 0);
  const at = (q: number): number => {
    let run = 0;
    for (const [slot, v] of timeline) {
      run += v;
      if (run >= q * totalSlots) return slot * 100;
    }
    return 0;
  };
  const t05 = at(0.05);
  const t95 = at(0.95) + 100;
  const steadyRate = t95 > t05 ? (0.9 * totalSlots) / ((t95 - t05) / 1000) : 0;
  const hist = new Uint32Array(HIST_BUCKETS);
  for (const s of senderStats) histMerge(hist, s.batchHist);
  const peakRss: Record<string, number> = {};
  const cpuPct: Record<string, number> = {};
  for (const w of infra.all()) {
    if (w.role === 'sender' && !active.includes(w)) continue;
    peakRss[w.role] = Math.max(peakRss[w.role] ?? 0, w.peakRssMb);
    const pct = (100 * w.cpuSecondsSinceMark()) / (wallMs / 1000);
    cpuPct[w.role] = Math.max(cpuPct[w.role] ?? 0, round(pct));
  }
  const machineCpu = infra
    .all()
    .filter((w) => w.role !== 'sender' || active.includes(w))
    .reduce((acc, w) => acc + w.cpuSecondsSinceMark(), 0);
  cpuPct['machine (all processes, % of all cores)'] = round(
    (100 * machineCpu) / (wallMs / 1000) / ctx.machine.logicalCpus,
  );
  peakRss['parent'] = Math.round(process.memoryUsage.rss() / 1048576);
  peakRss['total'] =
    infra
      .all()
      .reduce((s, w) => s + (w.role !== 'sender' || active.includes(w) ? w.peakRssMb : 0), 0) +
    (peakRss['parent'] ?? 0);
  const mocks = mockStatsList.reduce<MockStats>(
    (acc, m) => {
      for (const k of Object.keys(acc) as Array<keyof MockStats>) acc[k] += m[k] ?? 0;
      return acc;
    },
    {
      received: 0,
      ok: 0,
      unregistered: 0,
      tooManyRequests: 0,
      serverError: 0,
      rejected: 0,
      rateLimited: 0,
      goaways: 0,
    },
  );
  const rel = (t: number) => (t > 0 ? t - t0 : 0);
  return {
    scenario: sc,
    seed,
    alertCells: alert.area.h3Cells.length,
    shardJobs: jobs.length,
    planMs,
    shardPhaseMs: shardsDoneAt > 0 ? shardsDoneAt - t0 : wallMs,
    lastPushMs: rel(lastOk),
    firstPushMs: Number.isFinite(firstOk) ? rel(firstOk) : 0,
    iosLastMs: rel(lastOf('ios')),
    androidLastMs: rel(lastOf('android')),
    totalOk: senderStats.reduce((s, x) => s + x.ios.ok + x.android.ok, 0),
    iosOk: senderStats.reduce((s, x) => s + x.ios.ok, 0),
    androidOk: senderStats.reduce((s, x) => s + x.android.ok, 0),
    perSenderOk: senderStats.map((x) => x.ios.ok + x.android.ok),
    senderCpu: active.map((w) => round((100 * w.cpuSecondsSinceMark()) / (wallMs / 1000))),
    wallMs,
    batchP50: round(histQuantile(hist, 0.5), 1),
    batchP95: round(histQuantile(hist, 0.95), 1),
    steadyRate,
    t50Ms: totalSlots > 0 ? at(0.5) + 100 - t0 : 0,
    t99Ms: totalSlots > 0 ? at(0.99) + 100 - t0 : 0,
    peakRss,
    cpuPct,
    broker: brokerStats,
    mocks,
    senders: senderStats,
    shardTotals,
    eo,
    fcmRatePerSender,
    fcmRateSource,
    ...(error || jobError ? { error: error ?? `shard job failed: ${jobError}` } : {}),
  };
}

// ---------- results ----------

export function toScenarioResult(r: ScenarioRaw): ScenarioResult {
  const sc = r.scenario;
  const lastS = r.lastPushMs / 1000;
  const rate = lastS > 0 ? r.totalOk / lastS : 0;
  const sendS = (r.lastPushMs - r.firstPushMs) / 1000;
  const gaveUp = r.senders.reduce((s, x) => s + x.gaveUp, 0);
  const retried = r.senders.reduce((s, x) => s + x.retried, 0);
  const breaker = r.senders.reduce(
    (s, x) => s + (x.logEvents['delivery.permanent_breaker_open'] ?? 0),
    0,
  );
  const metrics: Metric[] = [
    { name: 'devices (seed)', value: sc.devices, unit: 'devices' },
    { name: 'devices in alert area', value: r.eo.expected, unit: 'devices' },
    { name: 'alert cells', value: r.alertCells, unit: 'cells' },
    { name: 'shard jobs', value: r.shardJobs, unit: 'jobs' },
    { name: 'senders', value: sc.senders, unit: 'processes' },
    { name: 'FCM projects', value: sc.projects, unit: 'projects' },
    { name: 'seed time', value: round(r.seed.seedMs / 1000, 1), unit: 's' },
    { name: 'AlertReady to last push', value: round(lastS, 1), unit: 's' },
    { name: 'planning time', value: r.planMs, unit: 'ms' },
    {
      name: 'shard phase (AlertReady to last shard done)',
      value: round(r.shardPhaseMs / 1000, 1),
      unit: 's',
    },
    { name: 'first push after AlertReady', value: round(r.firstPushMs / 1000, 2), unit: 's' },
    { name: 'last iOS push', value: round(r.iosLastMs / 1000, 1), unit: 's' },
    { name: 'last Android push', value: round(r.androidLastMs / 1000, 1), unit: 's' },
    { name: 'pushes/s (AlertReady to last push)', value: Math.round(rate), unit: 'pushes/s' },
    { name: 'pushes/s per sender', value: Math.round(rate / sc.senders), unit: 'pushes/s' },
    {
      name: 'pushes/s per sender while sending (first to last push)',
      value: sendS > 0 ? Math.round(r.totalOk / sendS / sc.senders) : 0,
      unit: 'pushes/s',
    },
    {
      name: 'pushes/s steady state (5% to 95% of pushes)',
      value: Math.round(r.steadyRate),
      unit: 'pushes/s',
    },
    {
      name: 'pushes/s per sender, steady state',
      value: Math.round(r.steadyRate / sc.senders),
      unit: 'pushes/s',
    },
    { name: 'AlertReady to 50% of pushes', value: round(r.t50Ms / 1000, 1), unit: 's' },
    { name: 'AlertReady to 99% of pushes', value: round(r.t99Ms / 1000, 1), unit: 's' },
    { name: 'iOS pushes', value: r.iosOk, unit: 'pushes' },
    { name: 'Android pushes', value: r.androidOk, unit: 'pushes' },
    { name: 'batch latency p50', value: r.batchP50, unit: 'ms' },
    { name: 'batch latency p95', value: r.batchP95, unit: 'ms' },
    {
      name: 'FCM bucket per sender per project',
      value: Math.round(r.fcmRatePerSender),
      unit: 'msg/s',
    },
    {
      name: 'docs estimate, last Android push (recipients / (projects x 9,000/s))',
      value: round(r.androidOk / (sc.projects * DEFAULT_FCM_PROJECT_RATE), 1),
      unit: 's',
    },
    { name: 'retried recipients', value: retried, unit: 'recipients' },
    { name: 'FCM quota 429s (mock)', value: r.mocks.rateLimited, unit: 'responses' },
    {
      name: 'injected 429/500 (mock)',
      value: r.mocks.tooManyRequests + r.mocks.serverError,
      unit: 'responses',
    },
    { name: 'dead tokens (unregistered)', value: r.mocks.unregistered, unit: 'tokens' },
    { name: 'SQS messages sent', value: r.broker.sentMessages, unit: 'messages' },
    {
      name: 'largest SQS message body',
      value: round(r.broker.maxBodyBytes / 1024, 1),
      unit: 'KB',
      target: { op: '<=', value: SQS_BODY_LIMIT_KB },
    },
    { name: 'dense cells (sent in parts)', value: r.shardTotals.denseCells, unit: 'cells' },
    {
      name: 'dense buckets already covered by an earlier normal batch',
      value: r.shardTotals.coveredBuckets,
      unit: 'buckets',
    },
    {
      name: 'peak buffered recipients (largest single shard job)',
      value: r.shardTotals.peakBufferedRecipients,
      unit: 'recipients',
    },
    { name: 'SQS redeliveries', value: r.broker.redeliveries, unit: 'messages' },
    { name: 'broker backlog peak', value: r.broker.peakBytesMb, unit: 'MB' },
    {
      name: 'shard back-pressure waits',
      value: round(r.broker.producerWaitMs / 1000, 1),
      unit: 's',
    },
  ];
  for (const [role, mb] of Object.entries(r.peakRss)) {
    metrics.push({
      name: `peak RSS ${role}${role === 'total' ? '' : ' (largest process)'}`,
      value: mb,
      unit: 'MB',
    });
  }
  for (const [role, pct] of Object.entries(r.cpuPct)) {
    metrics.push(
      role.startsWith('machine')
        ? { name: `CPU ${role}`, value: pct, unit: '%' }
        : { name: `CPU ${role} (busiest process)`, value: pct, unit: '% of one core' },
    );
  }
  const checks = [
    { name: 'run completed', ok: r.error === undefined, ...(r.error ? { detail: r.error } : {}) },
    {
      name: 'missed devices == 0',
      ok: r.eo.missed === 0,
      detail: `${r.eo.missed} missed of ${r.eo.expected} (${r.eo.deliveredOnce} delivered once, ${r.eo.excusedDead} dead tokens)`,
    },
    {
      name: 'duplicate pushes == 0',
      ok: r.eo.duplicatePushes === 0,
      detail: `${r.eo.duplicatePushes} extra pushes to ${r.eo.duplicateDevices} devices`,
    },
    {
      name: 'pushes outside the alert area == 0',
      ok: r.eo.outsideArea === 0,
      detail: `${r.eo.outsideArea} tokens`,
    },
    {
      name: 'planned recipients == devices in area',
      ok: r.shardTotals.recipients === r.eo.expected,
      detail: `${r.shardTotals.recipients} planned, ${r.eo.expected} in area`,
    },
    {
      name: 'no shard job failed or refused',
      ok: r.shardTotals.failedJobs === 0 && r.shardTotals.refused === 0,
    },
    { name: 'no recipient given up', ok: gaveUp === 0, detail: `${gaveUp}` },
    { name: 'no message dead-lettered', ok: r.broker.dlq === 0, detail: `${r.broker.dlq}` },
    { name: 'breaker stayed closed', ok: breaker === 0, detail: `${breaker} open events` },
    {
      name: `every SQS message body <= ${SQS_BODY_LIMIT_KB} KB`,
      ok: r.broker.maxBodyBytes <= SQS_BODY_LIMIT_KB * 1024,
      detail: `largest ${round(r.broker.maxBodyBytes / 1024, 1)} KB, ${r.broker.overBudgetMessages} message(s) over`,
    },
  ];
  if (sc.region === 'dense') {
    // Streaming bound: a dense cell never holds more than about DENSE_CELL_RECIPIENTS recipients
    // (plus unflushed batches and the other cells in flight), however many devices it has.
    const bound = 3 * DENSE_CELL_RECIPIENTS;
    checks.push(
      {
        name: 'dense path taken (a cell was sent in parts)',
        ok: r.shardTotals.denseCells >= 1,
        detail: `${r.shardTotals.denseCells} dense cell(s), ${sc.denseDevices} devices in the dense cell`,
      },
      {
        name: `peak buffered recipients per shard job <= ${bound.toLocaleString('en-US')} (3 x DENSE_CELL_RECIPIENTS)`,
        ok: r.shardTotals.peakBufferedRecipients <= bound,
        detail: `${r.shardTotals.peakBufferedRecipients} recipients in the dense cell's shard job`,
      },
    );
  }
  const notes: string[] = [];
  const mockCpu = Math.max(r.cpuPct['mock-apns'] ?? 0, r.cpuPct['mock-fcm'] ?? 0);
  if ((r.cpuPct['broker'] ?? 0) > 85) {
    notes.push(
      `The broker was at ${r.cpuPct['broker']}% of a core: it may have capped throughput.`,
    );
  }
  if (mockCpu > 85) {
    notes.push(
      `A mock process was at ${mockCpu}% of a core: the mocks, not the senders, may have capped throughput.`,
    );
  }
  const errors = new Map<string, number>();
  for (const s of r.senders)
    for (const [k, v] of Object.entries(s.errors)) errors.set(k, (errors.get(k) ?? 0) + v);
  if (errors.size > 0) {
    notes.push(
      `Push errors seen by senders: ${[...errors]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 6)
        .map(([k, v]) => `${k} ${v}`)
        .join(', ')}`,
    );
  }
  if (sc.region === 'dense') {
    notes.push(
      `Dense cell: ${sc.denseDevices} of ${sc.devices} devices in one res-7 cell; shard-worker peak RSS ${r.peakRss['shard-worker'] ?? 0} MB; ` +
        `peak buffered ${r.shardTotals.peakBufferedRecipients} recipients in one shard job (${r.shardTotals.rereadPasses} dense-part re-read passes); largest SQS body ${round(r.broker.maxBodyBytes / 1024, 1)} KB.`,
    );
  }
  notes.push(`FCM rate per sender per project: ${r.fcmRateSource}.`);
  notes.push(
    `Per-sender pushes: ${r.perSenderOk.join(', ')}; sender CPU %: ${r.senderCpu.join(', ')}`,
  );
  return {
    name: sc.name,
    verdict: checks.every((c) => c.ok) ? 'pass' : 'fail',
    durationMs: r.wallMs,
    metrics,
    checks,
    notes,
  };
}

/** The sender-count sweep: per-sender throughput, linearity, and the Fargate task arithmetic. */
export function sweepSummary(raws: ScenarioRaw[]): ScenarioResult | null {
  const pts = raws.filter((r) => r.scenario.key.startsWith('e'));
  if (pts.length === 0) return null;
  const perSender = (r: ScenarioRaw) => r.steadyRate / r.scenario.senders;
  const base = pts[0] ? perSender(pts[0]) : 0;
  const metrics: Metric[] = [];
  for (const r of pts) {
    const ps = perSender(r);
    metrics.push({
      name: `${r.scenario.senders} senders: pushes/s per sender (steady state)`,
      value: Math.round(ps),
      unit: 'pushes/s',
    });
    metrics.push({
      name: `${r.scenario.senders} senders: scaling efficiency vs 1 sender`,
      value: base > 0 ? Math.round((100 * ps) / base) : 0,
      unit: '%',
    });
    metrics.push({
      name: `${r.scenario.senders} senders: AlertReady to last push`,
      value: round(r.lastPushMs / 1000, 1),
      unit: 's',
    });
  }
  const top = pts[pts.length - 1] as ScenarioRaw;
  const rate = perSender(top);
  const need1M = 1_000_000 / 60;
  const need10M = 10_000_000 / 120;
  const tasks1M = rate > 0 ? Math.ceil(need1M / rate) : 0;
  const tasks10M = rate > 0 ? Math.ceil(need10M / rate) : 0;
  metrics.push(
    {
      name: 'per-sender rate used for sizing (largest sweep point, steady state)',
      value: Math.round(rate),
      unit: 'pushes/s',
    },
    { name: 'Fargate tasks for 1M in 60 s (16,667/s), local rate', value: tasks1M, unit: 'tasks' },
    {
      name: 'Fargate tasks for 1M in 60 s, with 2x safety (Fargate vCPU slower)',
      value: tasks1M * 2,
      unit: 'tasks',
    },
    {
      name: 'Fargate tasks for 10M in 120 s (83,333/s), with 2x safety',
      value: tasks10M * 2,
      unit: 'tasks',
    },
  );
  const eff = base > 0 ? rate / base : 0;
  return {
    name: 'e) sweep summary: sender scaling and Fargate sizing',
    verdict: pts.every((p) => toScenarioResult(p).verdict === 'pass') ? 'pass' : 'fail',
    durationMs: pts.reduce((s, p) => s + p.wallMs, 0),
    metrics,
    checks: [
      {
        name: 'every sweep point delivered exactly once',
        ok: pts.every((p) => toScenarioResult(p).verdict === 'pass'),
      },
    ],
    notes: [
      `Scaling from 1 to ${top.scenario.senders} senders is ${Math.round(eff * 100)}% of linear per sender ` +
        `(${eff >= 0.8 ? 'close to linear' : 'sub-linear: shared resources (mocks, broker, CPU cores) limit it'}).`,
      `Sizing: 1M in 60 s needs ${Math.round(need1M)} pushes/s; at ${Math.round(rate)} pushes/s per sender that is ` +
        `ceil(${Math.round(need1M)} / ${Math.round(rate)}) = ${tasks1M} tasks on this CPU; one sender process here is one ` +
        `Fargate task (cpu 1024 = 1 vCPU, Node is single-threaded); assuming a Fargate vCPU does about half the work of a desktop core, ` +
        `so plan ${tasks1M * 2}. Statewide 10M in 120 s: ceil(${Math.round(need10M)} / ${Math.round(rate)}) x 2 = ${tasks10M * 2}.`,
      "FCM quota is not the limit in the sweep (it uses many Firebase projects); the per-sender rate is the sender's own CPU and connection capacity.",
    ],
  };
}

// ---------- the test ----------

export async function runT2(ctx: SuiteContext, overrides: T2Overrides = {}): Promise<TestResult> {
  const startedAt = new Date().toISOString();
  const t0 = performance.now();
  const cfg = resolveConfig(ctx, overrides);
  const scenarios = scenarioList(cfg);
  const raws: ScenarioRaw[] = [];
  const results: ScenarioResult[] = [];
  let infra: Infra | undefined;
  let error: string | undefined;
  try {
    // Seeds first (they use all cores), then the long-lived processes.
    const seeds = new Map<string, SeedInfo>();
    for (const sc of scenarios) {
      const k = `${sc.region}-${sc.devices}-${sc.denseDevices ?? 0}`;
      if (!seeds.has(k)) {
        const info = await ensureSeed(ctx, cfg, sc.region, sc.devices, sc.denseDevices ?? 0);
        seeds.set(k, info);
      }
    }
    const keys = createAlertKeys();
    const maxProjects = Math.max(1, ...scenarios.map((s) => s.projects));
    infra = await startInfra(cfg, keys, maxProjects);
    ctx.log('t2: processes up', {
      senders: cfg.senders,
      shardWorkers: cfg.shardWorkers,
      apnsMocks: infra.apns.length,
      fcmMockProcs: infra.fcmProcs.length,
      heapMb: cfg.heapMb,
    });
    const reported = new Set<string>();
    for (const [i, sc] of scenarios.entries()) {
      const k = `${sc.region}-${sc.devices}-${sc.denseDevices ?? 0}`;
      const s = seeds.get(k) as SeedInfo;
      // Seed time is reported once, on the first scenario that uses the seed.
      const seed = reported.has(k) ? { ...s, seedMs: 0 } : s;
      reported.add(k);
      const raw = await runScenario(ctx, cfg, infra, keys, sc, seed, i + 1);
      raws.push(raw);
      const res = toScenarioResult(raw);
      results.push(res);
      ctx.log(`t2: ${sc.name}: ${res.verdict}, last push ${round(raw.lastPushMs / 1000, 1)} s`, {
        pushes: raw.totalOk,
        missed: raw.eo.missed,
        duplicates: raw.eo.duplicatePushes,
      });
      if (raw.error) break;
    }
    const sweep = sweepSummary(raws);
    if (sweep) results.push(sweep);
  } catch (err) {
    error = err instanceof Error ? (err.stack ?? err.message) : String(err);
    ctx.log('t2: failed', { error });
  } finally {
    if (infra) await Promise.all(infra.all().map((w) => w.stop()));
  }
  const peakRssMb = Math.max(0, ...raws.map((r) => r.peakRss['total'] ?? 0));
  const result: TestResult = {
    id: 't2',
    title: t2.title,
    verdict:
      error || results.length === 0 || results.some((r) => r.verdict === 'fail') ? 'fail' : 'pass',
    startedAt,
    durationMs: Math.round(performance.now() - t0),
    scenarios: results,
    peakRssMb,
    ...(error ? { error } : {}),
  };
  writeReport(ctx, cfg, result, raws);
  return result;
}

export const t2: SuiteTest = {
  id: 't2',
  title: 'Multi-process fan-out (planner, shard workers, senders, mock APNs/FCM)',
  description:
    'Real planner, shard worker and sender code in separate processes against mock APNs/FCM: county 1M and 1.9M, statewide 10M with 1 and 8 FCM projects, a sender-count sweep, and a dense city cell (150,000 devices in one cell).',
  estimatedMinutes: 45,
  run: (ctx) => runT2(ctx),
};
