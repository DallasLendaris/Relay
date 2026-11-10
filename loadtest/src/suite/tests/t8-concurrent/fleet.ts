/**
 * The process fleet t8 and t9 share: broker (queues and idempotency table, t2's), mock APNs and mock
 * FCM (t2's), shard workers, senders and the planner (this folder). The parent only conducts: it
 * starts everything, resets it between runs, tells the planner when each alert arrives, waits for
 * the fan-out to drain, and collects the senders' ledgers and the mocks' token lists.
 */
import { writeFileSync, rmSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Alert } from '@relay/shared';
import type { SuiteContext } from '../../types.js';
import { RssSampler } from '../../lib/rss.js';
import type { AlertKeys } from '../t2-fanout/alert.js';
import { DEFAULT_FCM_PROJECT_RATE } from '../../../../../backend/src/adapters/push/fcm-projects.js';
import { resolveConfig, type T2Config } from '../t2-fanout/config.js';
import { spawnWorker as spawnT2, type Worker as T2Worker } from '../t2-fanout/proc.js';
import {
  SEED_FORMAT,
  ensureDir,
  readManifest,
  seedDirFor,
  type SeedManifest,
} from '../t2-fanout/seed-data.js';
import { spawnCtl, sleep, t2Proc, type CtlProc, type Proc } from './ctl.js';
import type { AlertPlanStatus } from './planner-worker.js';
import type { SenderStartOptions, SenderStopReply } from './sender-worker.js';

const here = (rel: string): string => fileURLToPath(new URL(rel, import.meta.url));
const t2File = (name: string): string => here(`../t2-fanout/${name}`);
const file = (name: string): string => here(`./${name}`);

// ---------- configuration ----------

export interface FleetConfig {
  scale: 'full' | 'smoke';
  seed: number;
  senders: number;
  shardWorkers: number;
  jobsPerShard: number;
  apnsMocks: number;
  fcmMocks: number;
  projects: number;
  fcmQuotaPerSec: number;
  fcmRatePerSender: number;
  senderConcurrency: number;
  brokerCapMb: number;
  slotMs: number;
  stallMs: number;
  heapMb: {
    sender: number;
    shard: number;
    broker: number;
    mock: number;
    seed: number;
    ctl: number;
  };
  t2: T2Config;
}

/** Sizes from t2's config (heaps, process counts), adjusted for several alerts at once. */
export function fleetConfig(ctx: SuiteContext, devices: number): FleetConfig {
  const smoke = ctx.scale === 'smoke';
  const t2 = resolveConfig(ctx, { scenarios: ['a'], devicesA: devices });
  const projects = smoke ? 2 : 8;
  return {
    scale: ctx.scale,
    seed: t2.seed,
    senders: t2.senders,
    shardWorkers: t2.shardWorkers,
    jobsPerShard: smoke ? 2 : 3,
    apnsMocks: t2.apnsMocks,
    fcmMocks: Math.min(t2.fcmMocks, projects),
    projects,
    fcmQuotaPerSec: t2.fcmQuotaPerSec,
    fcmRatePerSender: DEFAULT_FCM_PROJECT_RATE / t2.senders,
    senderConcurrency: smoke ? 24 : t2.senderConcurrency,
    // The cap only models memory of this harness; SQS itself is unbounded. Large enough that
    // producers of an urgent alert are never held back by a standard backlog (a harness artefact).
    brokerCapMb: smoke ? 128 : 3072,
    slotMs: smoke ? 10 : 50,
    stallMs: t2.stallMs,
    heapMb: { ...t2.heapMb, broker: smoke ? 256 : 4096, ctl: smoke ? 384 : 1024 },
    t2,
  };
}

// ---------- seed on disk (t2's format, shared with t2 when the sizes match) ----------

export interface SeedInfo {
  dir: string;
  manifest: SeedManifest;
  /** 0 when the seed was already on disk. */
  seedMs: number;
}

export async function ensureCountySeed(
  ctx: SuiteContext,
  cfg: FleetConfig,
  devices: number,
  tag: string,
): Promise<SeedInfo> {
  const dir = seedDirFor(ctx.dataDir, 'county', devices, cfg.seed);
  const cached = readManifest(dir);
  if (cached && cached.devices === devices && cached.format === SEED_FORMAT) {
    ctx.log(`${tag}: seed of ${devices} devices cached`, { dir });
    return { dir, manifest: cached, seedMs: 0 };
  }
  const t0 = performance.now();
  rmSync(dir, { recursive: true, force: true });
  ensureDir(dir);
  const procs = Math.max(1, Math.min(ctx.workers, 2));
  ctx.log(`${tag}: seeding ${devices} devices with ${procs} process(es) (cached for later runs)`);
  const workers: T2Worker[] = await Promise.all(
    Array.from({ length: procs }, (_, i) =>
      spawnT2(t2File('seed-worker.ts'), { role: `seed-${i}`, heapMb: cfg.heapMb.seed }),
    ),
  );
  try {
    const totals = await workers[0]!.request<{
      entries: number;
      ios: number;
      android: number;
      multiCell: number;
      shards: string[];
    }>('gen', { dir, region: 'county', devices, seed: cfg.seed, chunk: 0 });
    const list = [...totals.shards].sort();
    const per = workers.map(() => [] as string[]);
    list.forEach((s, i) => per[i % workers.length]?.push(s));
    await Promise.all(workers.map((w, i) => w.request('sort', { dir, shards: per[i] })));
    await workers[0]!.request('cleanup', { dir });
    const manifest: SeedManifest = {
      format: SEED_FORMAT,
      region: 'county',
      devices,
      seed: cfg.seed,
      entries: totals.entries,
      ios: totals.ios,
      android: totals.android,
      multiCell: totals.multiCell,
      shards: list.length,
      generatedMs: Math.round(performance.now() - t0),
    };
    writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify(manifest, null, 2));
    return { dir, manifest, seedMs: manifest.generatedMs };
  } finally {
    await Promise.all(workers.map((w) => w.stop()));
  }
}

// ---------- the fleet ----------

export interface BrokerStats {
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
}

export interface MockStats {
  received: number;
  ok: number;
  unregistered: number;
  tooManyRequests: number;
  serverError: number;
  rejected: number;
  rateLimited: number;
  goaways: number;
}

export interface Fleet {
  cfg: FleetConfig;
  broker: T2Worker;
  brokerPort: number;
  apns: Array<{ w: T2Worker; url: string; cert: string }>;
  fcmProcs: T2Worker[];
  fcmUrls: Map<string, string>;
  projectIds: string[];
  shards: CtlProc[];
  senders: CtlProc[];
  planner: CtlProc;
  /** Every process, for the memory sampler and the liveness check. */
  procs(): Proc[];
  /** Extra processes (t9's API, late-join, load driver) to watch with the rest. */
  extra: Proc[];
  stop(): Promise<void>;
}

export async function startFleet(cfg: FleetConfig, publicPem: string): Promise<Fleet> {
  const projectIds = Array.from({ length: cfg.projects }, (_, i) => `relay-t8-p${i}`);
  const started: Array<{ stop(): Promise<void> }> = [];
  const t2Started: T2Worker[] = [];
  const track2 = async (p: Promise<T2Worker>): Promise<T2Worker> => {
    const w = await p;
    t2Started.push(w);
    return w;
  };
  const trackC = async (p: Promise<CtlProc>): Promise<CtlProc> => {
    const w = await p;
    started.push(w);
    return w;
  };
  try {
    const broker = await track2(
      spawnT2(t2File('broker-worker.ts'), { role: 'broker', heapMb: cfg.heapMb.broker }),
    );
    const { port: brokerPort } = await broker.request<{ port: number }>('start', {
      capMb: cfg.brokerCapMb,
    });
    const [apns, fcmProcs, shards, senders, planner] = await Promise.all([
      Promise.all(
        Array.from({ length: cfg.apnsMocks }, async (_, i) => {
          const w = await track2(
            spawnT2(t2File('mock-worker.ts'), { role: 'mock-apns', heapMb: cfg.heapMb.mock }),
          );
          const r = await w.request<{ port: number; cert: string }>('startApns', { seed: 100 + i });
          return { w, url: `https://127.0.0.1:${r.port}`, cert: r.cert };
        }),
      ),
      Promise.all(
        Array.from({ length: cfg.fcmMocks }, () =>
          track2(spawnT2(t2File('mock-worker.ts'), { role: 'mock-fcm', heapMb: cfg.heapMb.mock })),
        ),
      ),
      Promise.all(
        Array.from({ length: cfg.shardWorkers }, async (_, i) => {
          const w = await trackC(
            spawnCtl(file('shard-worker.ts'), `shard-${i}`, { heapMb: cfg.heapMb.shard }),
          );
          await w.req('init', { brokerPort, publicPem });
          return w;
        }),
      ),
      Promise.all(
        Array.from({ length: cfg.senders }, (_, i) =>
          trackC(spawnCtl(file('sender-worker.ts'), `sender-${i}`, { heapMb: cfg.heapMb.sender })),
        ),
      ),
      trackC(spawnCtl(file('planner-worker.ts'), 'planner', { heapMb: cfg.heapMb.ctl })),
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
        for (const [id, port] of Object.entries(r.ports)) {
          fcmUrls.set(id, `http://127.0.0.1:${port}`);
        }
      }),
    );
    const extra: Proc[] = [];
    const fleet: Fleet = {
      cfg,
      broker,
      brokerPort,
      apns,
      fcmProcs,
      fcmUrls,
      projectIds,
      shards,
      senders,
      planner,
      extra,
      procs: () => [
        t2Proc(broker),
        ...apns.map((a) => t2Proc(a.w)),
        ...fcmProcs.map(t2Proc),
        ...shards,
        ...senders,
        planner,
        ...extra,
      ],
      stop: async () => {
        await Promise.allSettled([
          ...started.map((s) => s.stop()),
          ...t2Started.map((w) => w.stop()),
        ]);
      },
    };
    return fleet;
  } catch (err) {
    await Promise.allSettled([...started.map((s) => s.stop()), ...t2Started.map((w) => w.stop())]);
    throw err;
  }
}

export function watchMemory(fleet: Fleet): RssSampler {
  const rss = new RssSampler();
  // Processes can come and go (t9 adds some), so read the live list on every sample.
  rss.addSource(() => fleet.procs().reduce((s, p) => s + (p.alive ? p.rssNow : 0), 0));
  return rss;
}

// ---------- one run ----------

export interface PreparedRun {
  alertsFile: string;
  outDir: string;
}

/** Resets broker, mocks, shard workers and planner for a fresh run of the alerts in `alertsFile`. */
export async function prepareRun(
  fleet: Fleet,
  seed: SeedInfo,
  alertsFile: string,
  publicPem: string,
  opts: { overlay?: string } = {},
): Promise<void> {
  const cfg = fleet.cfg;
  await Promise.all([
    fleet.broker.request('reset'),
    ...fleet.apns.map((a) => a.w.request('reset')),
    ...fleet.fcmProcs.map((w) => w.request('reset')),
    ...fleet.shards.map((w) =>
      w.req('scenario', {
        alertsFile,
        seedDir: seed.dir,
        fcmProjects: cfg.projects > 1 ? fleet.projectIds : [],
        overlay: opts.overlay,
      }),
    ),
  ]);
  await fleet.planner.req('init', {
    alertsFile,
    publicPem,
    shards: fleet.shards.map((s) => s.base),
    jobsPerShard: cfg.jobsPerShard,
  });
  await fleet.planner.req('reset');
}

export async function startSenders(
  fleet: Fleet,
  publicPem: string,
  alertsFile: string,
  outDir: string,
  opts: { maxPushesPerSec?: number } = {},
): Promise<void> {
  const cfg = fleet.cfg;
  await Promise.all(
    fleet.senders.map((w, i) => {
      const a = fleet.apns[i % fleet.apns.length];
      const body: SenderStartOptions = {
        id: `s${i}`,
        brokerPort: fleet.brokerPort,
        publicPem,
        alertsFile,
        apnsUrl: a?.url ?? '',
        apnsCert: a?.cert ?? '',
        fcm: fleet.projectIds.map((projectId) => ({
          projectId,
          url: fleet.fcmUrls.get(projectId) ?? '',
        })),
        fcmRatePerSec: cfg.fcmRatePerSender,
        concurrency: cfg.senderConcurrency,
        slotMs: cfg.slotMs,
        outDir,
        ...(opts.maxPushesPerSec ? { maxPushesPerSec: opts.maxPushesPerSec } : {}),
      };
      return w.req('start', body);
    }),
  );
}

export interface DrainState {
  t0: number;
  /** When each alert was handed to the planner (epoch ms), in script order. */
  arrivals: number[];
  planner: {
    idle: boolean;
    arrived: number;
    alerts: AlertPlanStatus[];
    error: string | null;
    log: Record<string, number>;
  };
  broker: BrokerStats;
  wallMs: number;
  error?: string;
}

export interface RunHooks {
  /** Called once, right after the first alert is handed to the planner. */
  onStart?: (t0: number) => Promise<void> | void;
  /** An extra completion condition (t9: the surge and the late-join queue are done). */
  extraDone?: () => Promise<boolean>;
  /** Part of the "is it still moving" signature. */
  extraProgress?: () => Promise<string>;
  log?: (msg: string) => void;
}

/** Runs the script: arrivals on time, then waits until planner, queues and extras are all idle. */
export async function conduct(
  fleet: Fleet,
  script: Array<{ atS: number }>,
  hooks: RunHooks = {},
): Promise<DrainState> {
  const cfg = fleet.cfg;
  const t0 = Date.now();
  const arrivals: number[] = new Array<number>(script.length).fill(0);
  let arrivalsDone = false;
  let arrivalError: string | undefined;
  const arrivalTask = (async () => {
    for (const [i, spec] of script.entries()) {
      const due = t0 + spec.atS * 1000;
      while (Date.now() < due) await sleep(Math.min(20, due - Date.now()));
      arrivals[i] = Date.now();
      await fleet.planner.req('arrive', { idx: i });
    }
    arrivalsDone = true;
  })().catch((e: unknown) => {
    arrivalError = e instanceof Error ? e.message : String(e);
  });
  await hooks.onStart?.(t0);

  let last = '';
  let lastChange = Date.now();
  let lastLog = Date.now();
  let error: string | undefined;
  let planner: DrainState['planner'];
  let broker: BrokerStats;
  const poll = cfg.scale === 'smoke' ? 100 : 250;
  let stable = 0;
  for (;;) {
    await sleep(poll);
    if (arrivalError) {
      error = `arrival failed: ${arrivalError}`;
    }
    planner = await fleet.planner.req<DrainState['planner']>('status');
    broker = await fleet.broker.request<BrokerStats>('stats');
    const extraOk = hooks.extraDone ? await hooks.extraDone() : true;
    if (arrivalsDone && planner.idle && broker.empty && extraOk) {
      // Retry messages (delayed) appear in `empty` too; two clean polls in a row end the run.
      if (++stable >= 2) break;
    } else stable = 0;
    const sig = `${planner.alerts.reduce((s, a) => s + a.jobsDone, 0)}/${broker.sentMessages}/${broker.receives}/${broker.dlq}/${(await hooks.extraProgress?.()) ?? ''}`;
    if (sig !== last) {
      last = sig;
      lastChange = Date.now();
    } else if (Date.now() - lastChange > cfg.stallMs) {
      error = `no progress for ${cfg.stallMs / 1000} s (queues ${JSON.stringify(broker.queues)})`;
    }
    if (planner.error && !error) error = `planner: ${planner.error}`;
    const dead = fleet.procs().filter((p) => !p.alive);
    if (dead.length > 0 && !error)
      error = `a child process died (${dead.map((p) => p.role).join(', ')})`;
    if (error) break;
    if (Date.now() - lastLog > 10_000) {
      lastLog = Date.now();
      hooks.log?.(
        `t=${Math.round((Date.now() - t0) / 1000)} s, jobs ${planner.alerts.reduce((s, a) => s + a.jobsDone, 0)}/${planner.alerts.reduce((s, a) => s + a.jobsTotal, 0)}, queues ${JSON.stringify(broker.queues)}`,
      );
    }
  }
  await arrivalTask;
  return { t0, arrivals, planner, broker, wallMs: Date.now() - t0, ...(error ? { error } : {}) };
}

export interface Collected {
  senders: SenderStopReply[];
  brokerFinal: BrokerStats;
  mocks: MockStats;
  /** Per mock process: files of accepted-push and unregistered token hashes. */
  mockFiles: Array<{ delivered: string; dead: string }>;
}

/** Stops the senders (their ledgers go to files), dumps the mocks' token lists, reads the final stats. */
export async function collect(fleet: Fleet, outDir: string): Promise<Collected> {
  const senders = await Promise.all(
    fleet.senders.map((w) => w.req<SenderStopReply>('finish', {}, 120_000)),
  );
  const brokerFinal = await fleet.broker.request<BrokerStats>('stats');
  const mockWorkers = [...fleet.apns.map((a) => a.w), ...fleet.fcmProcs];
  const stats = await Promise.all(mockWorkers.map((w) => w.request<MockStats>('stats')));
  const mockFiles = await Promise.all(
    mockWorkers.map(async (w, i) => {
      const delivered = path.join(outDir, `mock-${i}-delivered.bin`);
      const dead = path.join(outDir, `mock-${i}-dead.bin`);
      await w.request('dump', { deliveredFile: delivered, deadFile: dead });
      return { delivered, dead };
    }),
  );
  const mocks = stats.reduce<MockStats>(
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
  return { senders, brokerFinal, mocks, mockFiles };
}

export function writeAlertsFile(dir: string, alerts: Alert[]): string {
  const f = path.join(dir, 'alerts.json');
  writeFileSync(f, JSON.stringify(alerts));
  return f;
}

export type { AlertKeys };
