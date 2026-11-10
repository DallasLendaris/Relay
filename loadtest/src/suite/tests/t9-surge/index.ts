/**
 * t9: signup surge. One county-wide warning is active and fanning out to the seeded devices (1,000,000
 * at full scale) while new devices register through the REAL API handler at 50,000 per minute for five
 * minutes (smoke: 2,000 over 30 s): POST /devices, PUT /devices/me/push-token, POST /places inside the
 * alert area. The API writes the cell index and asks the late-join queue; the REAL late-join handler
 * claims, mints check-in tokens and enqueues sends; the REAL senders push them to the mock APNs/FCM.
 *
 * It runs the same fan-out twice, first without the surge (baseline), then with it, so the cost of the
 * surge to the main alert is a measured difference. Besides the exactly-once and latency numbers it
 * checks the push-target move limits (2% of new devices re-register an already used token, 1% try to
 * move a token five times in a row and must get 409 after the second move).
 *
 * Known trade-off the test reports rather than hides: a device whose place is saved before the planned
 * fan-out has read its cell can be sent the alert by both paths (the planned fan-out does not take the
 * late-join per-target claim; late-join "fails toward a duplicate" for a place newer than the alert).
 * Such "racers" may get two pushes; a device registered after the read must get exactly one.
 */
import { mkdirSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Alert } from '@relay/shared';
import { fmt, writeTestReport } from '../../lib/report.js';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import { createAlertKeys } from '../t2-fanout/alert.js';
import { sortedUnique } from '../t2-fanout/verify.js';
import { buildScript, type AlertSpec } from '../t8-concurrent/alerts.js';
import { spawnCtl, sleep, type CtlProc } from '../t8-concurrent/ctl.js';
import type { ExpectedReply } from '../t8-concurrent/expected-worker.js';
import {
  collect,
  conduct,
  ensureCountySeed,
  fleetConfig,
  prepareRun,
  startFleet,
  startSenders,
  watchMemory,
  writeAlertsFile,
  type Collected,
  type DrainState,
  type FleetConfig,
  type SeedInfo,
} from '../t8-concurrent/fleet.js';
import {
  concatWords,
  exactlyOnceDetailed,
  mergeTimelines,
  quantile,
  readDoubles,
  readWords,
  round,
  timeAtFraction,
  lastOf,
  type Detailed,
} from '../t8-concurrent/ledger.js';
import type { LoadFinal, LoadRunOptions } from './load-worker.js';
import type { LateJoinFinal } from './latejoin-worker.js';

const here = (name: string): string => fileURLToPath(new URL(`./${name}`, import.meta.url));
const t8File = (name: string): string =>
  fileURLToPath(new URL(`../t8-concurrent/${name}`, import.meta.url));

/** The agreed bound on what the surge may add to the main alert's time to last push. */
export const SLOWDOWN_LIMIT = {
  full: { factor: 1.25, slackS: 5 },
  smoke: { factor: 1.6, slackS: 3 },
} as const;

/** Registration must stay this fast for the 99th percentile of a whole device flow (3 requests), ms. */
export const REGISTER_P99_LIMIT_MS = 2000;
/** A late-join push for a place saved after the planned fan-out ended must arrive within this, ms (p99). */
export const LATE_AFTER_DRAIN_P99_LIMIT_MS = 10_000;

export interface SurgeSizes {
  seedDevices: number;
  newDevices: number;
  perMin: number;
  /** Cap on pushes per second over all senders (0 = unlimited). Smoke only. */
  smokePushesPerSec: number;
}

export function sizesFor(scale: 'full' | 'smoke', env = process.env): SurgeSizes {
  if (scale === 'smoke') {
    return { seedDevices: 6_000, newDevices: 2_000, perMin: 4_000, smokePushesPerSec: 600 };
  }
  const num = (name: string, dflt: number, min: number): number => {
    const n = Number(env[name]);
    return Number.isFinite(n) && n >= min ? Math.floor(n) : dflt;
  };
  const perMin = num('T9_PER_MIN', 50_000, 60);
  const minutes = num('T9_MINUTES', 5, 1);
  return {
    seedDevices: num('T9_DEVICES', 1_000_000, 1000),
    newDevices: perMin * minutes,
    perMin,
    smokePushesPerSec: 0,
  };
}

const MAIN: AlertSpec = { name: 'county warning', severity: 'warning', area: 'county', atS: 0 };

// ---------- helpers on sorted 64-bit arrays ----------

function lowerBound(a: BigUint64Array, v: bigint): number {
  let lo = 0;
  let hi = a.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if ((a[mid] as bigint) < v) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** Removes one occurrence of each element of `sub` from `all` (both sorted). */
export function minusMultiset(all: BigUint64Array, sub: BigUint64Array): BigUint64Array {
  const out = new BigUint64Array(all.length);
  let w = 0;
  let j = 0;
  for (let i = 0; i < all.length; i++) {
    const v = all[i] as bigint;
    while (j < sub.length && (sub[j] as bigint) < v) j++;
    if (j < sub.length && sub[j] === v) {
      j++;
      continue;
    }
    out[w++] = v;
  }
  return out.slice(0, w);
}

/** Sorts `tokens` ascending and applies the same order to `values`. */
export function sortPairs(
  tokens: BigUint64Array,
  values: Float64Array,
): { tokens: BigUint64Array; values: Float64Array } {
  const idx = Uint32Array.from({ length: tokens.length }, (_, i) => i);
  idx.sort((a, b) => {
    const x = tokens[a] as bigint;
    const y = tokens[b] as bigint;
    return x < y ? -1 : x > y ? 1 : 0;
  });
  return {
    tokens: BigUint64Array.from(idx, (i) => tokens[i] as bigint),
    values: Float64Array.from(idx, (i) => values[i] as number),
  };
}

// ---------- one run ----------

interface MainOutcome {
  arrivedAt: number;
  shardDoneAt: number;
  accepted: number;
  t90Ms: number;
  lastMs: number;
  lastAt: number;
  recipients: number;
  /** Planned pushes (not late-join) against the seed devices. */
  seedCheck: Detailed;
  /** Tokens in the planned fan-out that are not seed devices (new devices the shards read). */
  plannedExtra: bigint[];
  plannedAll: BigUint64Array;
}

interface ApiFinal {
  overlayWithTarget: number;
  /** Cell names; the cells file holds an index into this list per token. */
  cells: string[];
  files: { tokens: string; cells: string };
}

interface SurgeOutcome {
  load: LoadFinal;
  lateJoin: LateJoinFinal;
  api: ApiStats;
  apiFinal: ApiFinal;
  lateCount: number;
  /** New push targets whose final holder saved a place. */
  expected: number;
  /** Expected tokens with no push from either path (and not answered "unregistered"). */
  missed: number;
  excusedDead: number;
  /** Tokens pushed by BOTH the planned fan-out and late-join. */
  both: number;
  /** Same, for a push token that moved between two new devices (planned for the old holder, late-join for the new one). */
  bothMoved: number;
  /** Tokens late-join pushed twice. */
  lateTwice: number;
  /** Tokens the planned fan-out pushed twice because they moved between devices mid-read. */
  plannedTwiceMoved: number;
  plannedTwiceOther: number;
  plannedOnly: number;
  lateOnly: number;
  movedTokens: number;
  /** Place saved before, during or after the read of its cell (fanout.cell_planned); moved tokens left out. */
  phases: Record<'before' | 'during' | 'after' | 'unknown', PhaseCounts>;
  lateOutside: number;
  strayPlanned: number;
  /** Planned pushes to non-seed tokens (new devices the shards read). */
  plannedNew: number;
  latency: { all: number[]; during: number[]; after: number[]; waited: number[] };
  cellsTimed: number;
}

interface ApiStats {
  requests: number;
  serverErrors: number;
  byRoute: Record<string, number>;
  devices: number;
  devicesHolding: number;
  places: number;
  overlayEntries: number;
  intents: { enqueued: number; polled: number; waiting: number; byReason: Record<string, number> };
  geocoder: { calls: number; outside: number };
}

interface RunOutput {
  surge: boolean;
  drain: DrainState;
  collected: Collected;
  main: MainOutcome;
  surgeOut?: SurgeOutcome;
  peakRssMb: number;
  error?: string;
}

async function runOnce(
  ctx: SuiteContext,
  cfg: FleetConfig,
  sizes: SurgeSizes,
  seed: SeedInfo,
  alertsFile: string,
  publicPem: string,
  expectedSeed: BigUint64Array,
  dir: string,
  withSurge: boolean,
): Promise<RunOutput> {
  mkdirSync(dir, { recursive: true });
  const tag = withSurge ? 'surge' : 'baseline';
  const fleet = await startFleet(cfg, publicPem);
  let api: CtlProc | undefined;
  let lj: CtlProc | undefined;
  let load: CtlProc | undefined;
  let peakRssMb = 0;
  let error: string | undefined;
  const rss = watchMemory(fleet);
  try {
    let apiPub = 0;
    if (withSurge) {
      api = await spawnCtl(here('api-worker.ts'), 'api', {
        heapMb: cfg.scale === 'smoke' ? 384 : 2048,
      });
      fleet.extra.push(api);
      const initReply = await api.req<{ pub: number }>('init', {
        alertsFile,
        fcmProjects: cfg.projects > 1 ? fleet.projectIds : [],
      });
      apiPub = initReply.pub;
      lj = await spawnCtl(here('latejoin-worker.ts'), 'latejoin', {
        heapMb: cfg.scale === 'smoke' ? 384 : 1024,
      });
      fleet.extra.push(lj);
      await lj.req('init', {
        apiCtl: api.base,
        brokerPort: fleet.brokerPort,
        publicPem,
        alertsFile,
        // Production waits 15 s between decisions for a device that may be in the planned fan-out.
        planWaitDelayS: cfg.scale === 'smoke' ? 1 : 15,
        outDir: dir,
      });
      load = await spawnCtl(here('load-worker.ts'), 'load', {
        heapMb: cfg.scale === 'smoke' ? 384 : 1024,
      });
      fleet.extra.push(load);
    }
    await prepareRun(fleet, seed, alertsFile, publicPem, api ? { overlay: api.base } : {});
    await startSenders(fleet, publicPem, alertsFile, dir, {
      ...(sizes.smokePushesPerSec > 0
        ? { maxPushesPerSec: Math.ceil(sizes.smokePushesPerSec / cfg.senders) }
        : {}),
    });
    ctx.log(
      `t9 ${tag}: fleet up (${cfg.senders} senders${api ? ', API, late-join, load driver' : ''}); running`,
    );

    const loadOpts: LoadRunOptions = {
      apiPort: apiPub,
      devices: sizes.newDevices,
      perMin: sizes.perMin,
      seed: cfg.seed,
      outDir: dir,
    };
    let loadDone = !withSurge;
    const drain = await conduct(fleet, [MAIN], {
      log: (m) => ctx.log(`t9 ${tag}: ${m}`),
      onStart: async () => {
        if (!api || !lj || !load) return;
        await lj.req('start', { concurrency: cfg.scale === 'smoke' ? 4 : 16, batchSize: 10 });
        await load.req('run', loadOpts);
      },
      extraDone: async () => {
        if (!api || !lj || !load) return true;
        if (!loadDone) {
          const s = await load.req<{ done: boolean }>('status');
          if (!s.done) return false;
          loadDone = true;
        }
        const [a, l] = await Promise.all([
          api.req<ApiStats>('stats'),
          lj.req<{ polled: number; processed: number; inflight: number; retryWaiting: number }>(
            'status',
          ),
        ]);
        return (
          a.intents.waiting === 0 &&
          l.polled === a.intents.enqueued &&
          l.inflight === 0 &&
          l.retryWaiting === 0 &&
          l.processed === l.polled
        );
      },
      extraProgress: async () => {
        if (!load || !lj) return '';
        const [s, l] = await Promise.all([
          load.req<{ finishedFlows: number }>('status'),
          lj.req<{ processed: number }>('status'),
        ]);
        return `${s.finishedFlows}/${l.processed}`;
      },
    });
    if (drain.error) error = drain.error;

    const loadFinal = load && !error ? await load.req<LoadFinal>('finish', {}, 120_000) : undefined;
    const apiStats = api && !error ? await api.req<ApiStats>('stats') : undefined;
    const apiFinal = api && !error ? await api.req<ApiFinal>('final', { outDir: dir }) : undefined;
    // When each cell was read by the planned fan-out (fanout.cell_planned), merged over shard workers.
    const cellPlans = new Map<string, { start: number; end: number }>();
    if (api && !error) {
      const parts = await Promise.all(
        fleet.shards.map((w) =>
          w.req<{ cells: Array<{ cell: string; start: number; end: number }> }>('cellPlans'),
        ),
      );
      for (const part of parts) {
        for (const c of part.cells) {
          const old = cellPlans.get(c.cell);
          cellPlans.set(c.cell, {
            start: old ? Math.min(old.start, c.start) : c.start,
            end: old ? Math.max(old.end, c.end) : c.end,
          });
        }
      }
    }
    const ljFinal = lj && !error ? await lj.req<LateJoinFinal>('finish', {}, 120_000) : undefined;
    const collected = await collect(fleet, dir);
    peakRssMb = rss.stop();

    // ----- the main alert -----
    const senders = collected.senders;
    const tl = mergeTimelines(
      senders.map((s) => s.timeline),
      1,
      cfg.slotMs,
    )[0] as ReturnType<typeof mergeTimelines>[number];
    const st = drain.planner.alerts[0];
    const arrivedAt = st?.arrivedAt ?? drain.arrivals[0] ?? 0;
    const lastAt = lastOf(tl);
    const t90At = timeAtFraction(tl, 0.9);
    const okAll = concatWords(senders.map((s) => readWords(s.files.ok[0] as string)));
    okAll.sort();
    const lateTokens = concatWords(senders.map((s) => readWords(s.files.lateTokens)));
    lateTokens.sort();
    const dead = concatWords(senders.map((s) => readWords(s.files.dead[0] as string)));
    const planned = minusMultiset(okAll, lateTokens);
    const seedCheck = exactlyOnceDetailed(expectedSeed, planned.slice(), dead);
    const main: MainOutcome = {
      arrivedAt,
      shardDoneAt: st?.shardDoneAt ?? 0,
      accepted: tl.total,
      t90Ms: t90At > 0 ? t90At - arrivedAt : 0,
      lastMs: lastAt > 0 ? lastAt - arrivedAt : 0,
      lastAt,
      recipients: st?.recipients ?? 0,
      seedCheck,
      plannedExtra: seedCheck.outsideHashes,
      plannedAll: planned,
    };
    const out: RunOutput = {
      surge: withSurge,
      drain,
      collected,
      main,
      peakRssMb,
      ...(error ? { error } : {}),
    };
    if (loadFinal && apiStats && apiFinal && ljFinal) {
      out.surgeOut = analyseSurge(
        main,
        expectedSeed,
        senders.map((s) => s.files),
        loadFinal,
        ljFinal,
        apiStats,
        apiFinal,
        cellPlans,
        dead,
      );
    }
    return out;
  } finally {
    peakRssMb = Math.max(peakRssMb, rss.stop());
    await Promise.allSettled([api?.stop(), lj?.stop(), load?.stop()]);
    await fleet.stop();
  }
}

function upperBound(a: BigUint64Array, v: bigint): number {
  let lo = 0;
  let hi = a.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if ((a[mid] as bigint) <= v) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

const countOf = (a: BigUint64Array, v: bigint): number => upperBound(a, v) - lowerBound(a, v);
const hasToken = (a: BigUint64Array, v: bigint): boolean => countOf(a, v) > 0;

export interface PhaseCounts {
  /** Devices whose place was saved in this phase relative to the read of their cell. */
  n: number;
  planned: number;
  late: number;
}

function readU32(file: string): Uint32Array {
  const b = readFileSync(file);
  return new Uint32Array(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
}

function analyseSurge(
  main: MainOutcome,
  expectedSeed: BigUint64Array,
  senderFiles: Array<{ lateTokens: string; lateTimes: string; lost: string }>,
  load: LoadFinal,
  lateJoin: LateJoinFinal,
  api: ApiStats,
  apiFinal: ApiFinal,
  cellPlans: Map<string, { start: number; end: number }>,
  dead: BigUint64Array,
): SurgeOutcome {
  // Expected tokens (the final holder saved a place) with the time their first place was saved.
  const e = sortPairs(readWords(load.files.tokens), readDoubles(load.files.firstPlaceAt));
  const late0 = concatWords(senderFiles.map((f) => readWords(f.lateTokens)));
  const lateTimes0 = new Float64Array(late0.length);
  let at = 0;
  for (const f of senderFiles) {
    const t = readDoubles(f.lateTimes);
    lateTimes0.set(t, at);
    at += t.length;
  }
  const late = sortPairs(late0, lateTimes0);
  const lost = sortedUnique(concatWords(senderFiles.map((f) => readWords(f.lost))));
  const moved = sortedUnique(readWords(load.files.moved));
  const waited = sortedUnique(readWords(lateJoin.deferredTokensFile));
  // Planned pushes to tokens that are not seed devices: the new devices the shards read.
  const extras = minusMultiset(main.plannedAll, expectedSeed);
  // Cell of each expected token (its final holder's place), from the API's index.
  const cellOf = sortPairs(
    readWords(apiFinal.files.tokens),
    Float64Array.from(readU32(apiFinal.files.cells)),
  );

  const out = {
    missed: 0,
    excusedDead: 0,
    both: 0,
    bothMoved: 0,
    lateTwice: 0,
    plannedTwiceMoved: 0,
    plannedTwiceOther: 0,
    plannedOnly: 0,
    lateOnly: 0,
    movedTokens: 0,
  };
  const phases = {
    before: { n: 0, planned: 0, late: 0 } as PhaseCounts,
    during: { n: 0, planned: 0, late: 0 } as PhaseCounts,
    after: { n: 0, planned: 0, late: 0 } as PhaseCounts,
    unknown: { n: 0, planned: 0, late: 0 } as PhaseCounts,
  };
  const deadSet = sortedUnique(dead.slice());
  const lostSet = new Set(lost);
  for (let i = 0; i < e.tokens.length; i++) {
    const v = e.tokens[i] as bigint;
    const cp = countOf(extras, v);
    const cl = countOf(late.tokens, v);
    if (cp + cl === 0) {
      if (hasToken(deadSet, v)) out.excusedDead++;
      else out.missed++;
      continue;
    }
    const isMoved = hasToken(moved, v);
    if (cp > 0 && cl > 0) {
      if (isMoved) out.bothMoved++;
      else out.both++;
    } else if (cl > 1 && !lostSet.has(v)) out.lateTwice++;
    else if (cp > 1) {
      if (hasToken(moved, v)) out.plannedTwiceMoved++;
      else out.plannedTwiceOther++;
    }
    if (cp > 0 && cl === 0) out.plannedOnly++;
    if (cl > 0 && cp === 0) out.lateOnly++;
    // Classify by when the place was saved against the read of its cell. Moved tokens have two
    // holders in two cells, so they are left out.
    if (hasToken(moved, v)) {
      out.movedTokens++;
      continue;
    }
    const ci = lowerBound(cellOf.tokens, v);
    const cellName =
      ci < cellOf.tokens.length && cellOf.tokens[ci] === v
        ? apiFinal.cells[cellOf.values[ci] as number]
        : undefined;
    const plan = cellName ? cellPlans.get(cellName) : undefined;
    const placeAt = e.values[i] as number;
    const phase = !plan
      ? phases.unknown
      : placeAt < plan.start
        ? phases.before
        : placeAt <= plan.end
          ? phases.during
          : phases.after;
    phase.n++;
    if (cp > 0) phase.planned++;
    if (cl > 0) phase.late++;
  }
  // Pushes to tokens that are not expected new devices.
  let lateOutside = 0;
  for (let i = 0; i < late.tokens.length; i++) {
    if (!hasToken(e.tokens, late.tokens[i] as bigint)) lateOutside++;
  }
  let strayPlanned = 0;
  for (let i = 0; i < extras.length; i++) {
    if (!hasToken(e.tokens, extras[i] as bigint)) strayPlanned++;
  }

  // Latency: late-join push accepted minus the first place saved for that token.
  const lat = {
    all: [] as number[],
    during: [] as number[],
    after: [] as number[],
    waited: [] as number[],
  };
  for (let i = 0; i < late.tokens.length; i++) {
    const v = late.tokens[i] as bigint;
    const p = lowerBound(e.tokens, v);
    if (!(p < e.tokens.length && e.tokens[p] === v)) continue;
    const placeAt = e.values[p] as number;
    const ms = Math.max(0, (late.values[i] as number) - placeAt);
    lat.all.push(ms);
    (placeAt <= main.lastAt ? lat.during : lat.after).push(ms);
    if (hasToken(waited, v)) lat.waited.push(ms);
  }
  return {
    load,
    lateJoin,
    api,
    apiFinal,
    lateCount: late.tokens.length,
    expected: e.tokens.length,
    ...out,
    phases,
    lateOutside,
    strayPlanned,
    plannedNew: extras.length,
    latency: lat,
    cellsTimed: cellPlans.size,
  };
}

// ---------- the test ----------

export const t9: SuiteTest = {
  id: 't9',
  title: 'Signup surge',
  description:
    'While one county alert fans out to 1,000,000 devices, new devices register at 50,000 per minute for 5 minutes through the real API; they get the active alert exactly once through the real late-join path; the main fan-out is compared with a baseline without the surge.',
  estimatedMinutes: 15,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const wall0 = Date.now();
    const smoke = ctx.scale === 'smoke';
    const sizes = sizesFor(ctx.scale);
    const cfg = fleetConfig(ctx, sizes.seedDevices);
    const runDir = join(ctx.dataDir, 't9-run');
    rmSync(runDir, { recursive: true, force: true });
    mkdirSync(runDir, { recursive: true });
    let error: string | undefined;
    let base: RunOutput | undefined;
    let surge: RunOutput | undefined;
    let expected: ExpectedReply | undefined;
    let seed: SeedInfo | undefined;
    let alert: Alert | undefined;
    let peakRssMb = 0;
    const scenarioStart = Date.now();
    try {
      ctx.log(
        `t9: ${fmt(sizes.seedDevices)} seed devices, ${fmt(sizes.newDevices)} new devices at ${fmt(sizes.perMin)}/min (${ctx.scale})`,
      );
      const keys = createAlertKeys();
      [alert] = await buildScript(keys, [MAIN], ctx.scale);
      const alertsFile = writeAlertsFile(runDir, [alert as Alert]);
      seed = await ensureCountySeed(ctx, cfg, sizes.seedDevices, 't9');
      const ex = await spawnCtl(t8File('expected-worker.ts'), 'expected', {
        heapMb: cfg.heapMb.ctl,
      });
      try {
        expected = await ex.req<ExpectedReply>('run', {
          alertsFile,
          devices: sizes.seedDevices,
          seed: cfg.seed,
          outDir: runDir,
        });
      } finally {
        await ex.stop();
      }
      const expectedSeed = sortedUnique(readWords(expected.files.perAlert[0] as string));
      ctx.log(`t9: the alert must reach ${fmt(expectedSeed.length)} seed devices`);
      base = await runOnce(
        ctx,
        cfg,
        sizes,
        seed,
        alertsFile,
        keys.publicPem,
        expectedSeed,
        join(runDir, 'base'),
        false,
      );
      peakRssMb = base.peakRssMb;
      if (base.error) error = `baseline: ${base.error}`;
      else {
        await sleep(500);
        surge = await runOnce(
          ctx,
          cfg,
          sizes,
          seed,
          alertsFile,
          keys.publicPem,
          expectedSeed,
          join(runDir, 'surge'),
          true,
        );
        peakRssMb = Math.max(peakRssMb, surge.peakRssMb);
        if (surge.error) error = `surge run: ${surge.error}`;
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      ctx.log(`t9 failed: ${error}`);
    } finally {
      rmSync(runDir, { recursive: true, force: true });
    }

    const scenario = buildScenario(
      ctx.scale,
      sizes,
      base,
      surge,
      error,
      Date.now() - scenarioStart,
      seed,
    );
    const result: TestResult = {
      id: 't9',
      title: t9.title,
      verdict: scenario.verdict,
      startedAt,
      durationMs: Date.now() - wall0,
      scenarios: [scenario],
      peakRssMb,
      ...(error ? { error } : {}),
    };
    writeTestReport(ctx, result, intro(cfg, sizes, smoke));
    return result;
  },
};

// ---------- analysis ----------

export function slowdownBoundS(scale: 'full' | 'smoke', baselineS: number): number {
  const b = SLOWDOWN_LIMIT[scale];
  return baselineS * b.factor + b.slackS;
}

function buildScenario(
  scale: 'full' | 'smoke',
  sizes: SurgeSizes,
  base: RunOutput | undefined,
  surge: RunOutput | undefined,
  error: string | undefined,
  durationMs: number,
  seed: SeedInfo | undefined,
): ScenarioResult {
  const checks: ScenarioResult['checks'] = [];
  const metrics: Metric[] = [];
  const notes: string[] = [];
  const add = (name: string, ok: boolean, detail?: string): void => {
    checks.push({ name, ok, ...(detail ? { detail } : {}) });
  };
  add('both runs completed without an error', !error, error);
  const so = surge?.surgeOut;
  if (!base || !surge || !so) {
    return { name: 'signup surge', verdict: 'fail', durationMs, metrics, checks, notes };
  }
  const s = so;
  const lf = s.load;
  const q = (xs: number[], p: number): number => round(quantile(xs, p) / 1000, 2);

  // ----- the main alert (both runs): every seed device exactly once -----
  for (const [label, run] of [
    ['baseline', base],
    ['with surge', surge],
  ] as const) {
    const c = run.main.seedCheck;
    add(
      `${label}: every seed device got the alert exactly once (missed ${c.missed}, duplicates ${c.duplicatePushes})`,
      c.missed === 0 && c.duplicatePushes === 0,
      `expected ${c.expected}`,
    );
  }
  add(
    'baseline: planned fan-out reached no device outside the seed',
    base.main.seedCheck.outside === 0,
    `${base.main.seedCheck.outside}`,
  );

  // ----- the surge: registration -----
  const errRate = lf.requests > 0 ? lf.errors / lf.requests : 1;
  add(
    'every new device was created (load driver == API)',
    lf.devicesCreated === s.api.devices && lf.devicesCreated >= sizes.newDevices * 0.99,
    `driver ${lf.devicesCreated}, API ${s.api.devices}, planned ${sizes.newDevices}`,
  );
  add(
    `registration error rate <= 0.5% (${lf.errors} errors in ${fmt(lf.requests)} requests, ${lf.networkErrors} network errors, ${s.api.serverErrors} API 5xx)`,
    errRate <= 0.005 && s.api.serverErrors === 0,
    `${round(errRate * 100, 3)}%`,
  );
  add(
    `registration p99 (device flow of three requests) <= ${REGISTER_P99_LIMIT_MS} ms`,
    lf.latency.flow.p99 <= REGISTER_P99_LIMIT_MS,
    `p99 ${lf.latency.flow.p99} ms`,
  );
  add(
    `the load driver held the arrival rate (>= 90% of ${fmt(sizes.perMin)}/min)`,
    lf.achievedPerMin >= sizes.perMin * 0.9,
    `${fmt(lf.achievedPerMin)}/min, launch lag p99 ${lf.launchLagP99} ms`,
  );
  add(
    'the 1% of addresses outside California were refused and retried',
    lf.outsideRetries > 0 && lf.outsideRetries === s.api.geocoder.outside && lf.placeGaveUp === 0,
    `${lf.outsideRetries} retries, geocoder gave ${s.api.geocoder.outside}, gave up ${lf.placeGaveUp}`,
  );

  // ----- push-target move limits -----
  add(
    `token moves: every re-registered token moved once and was allowed (${lf.reuse.moved} of ${lf.reuse.attempted})`,
    lf.reuse.attempted > 0 && lf.reuse.rejected === 0,
    `${lf.reuse.rejected} rejected`,
  );
  const patterns = Object.entries(lf.rapid.patterns)
    .map(([k, v]) => `${k} x${v}`)
    .join('; ');
  add(
    `rapid token moves: answers were 200, 200, 200, 409, 409 in all ${lf.rapid.groups} groups`,
    lf.rapid.groups > 0 && lf.rapid.mismatched === 0,
    patterns,
  );
  const api409 = Object.entries(s.api.byRoute)
    .filter(([k]) => k.startsWith('PUT /devices/me/push-token') && k.endsWith(' 409'))
    .reduce((n, [, v]) => n + v, 0);
  add(
    'the API counted the same 409s as the driver saw',
    api409 === lf.rapid.rejected409,
    `API ${api409}, driver ${lf.rapid.rejected409}`,
  );
  add(
    'the cell index the API wrote matches the devices that hold a token and saved a place',
    s.apiFinal.overlayWithTarget === lf.expectedTokens,
    `index ${s.apiFinal.overlayWithTarget}, expected ${lf.expectedTokens}`,
  );

  // ----- every new device got the alert exactly once (planned or late-join, never both) -----
  const ev = s.latency;
  add(
    `new devices missed == 0 (${fmt(s.expected)} new push targets expected)`,
    s.missed === 0,
    `${s.missed} missed${s.excusedDead ? `, ${s.excusedDead} excused (unregistered)` : ''}`,
  );
  add(
    'devices sent by both paths (planned fan-out and late-join) == 0 (tokens that moved between devices are counted separately)',
    s.both === 0,
    `${s.both} devices`,
  );
  add(
    'late-join duplicates == 0 (no device got its late-join push twice)',
    s.lateTwice === 0,
    `${s.lateTwice}`,
  );
  add(
    'pushes only to devices the API registered with a place in the area (late-join and planned)',
    s.lateOutside === 0 && s.strayPlanned === 0,
    `${s.lateOutside} late-join, ${s.strayPlanned} planned`,
  );
  add(
    'the planned fan-out repeated no token except one that moved between two devices mid-read',
    s.plannedTwiceOther === 0,
    `${s.plannedTwiceOther} unexplained, ${s.plannedTwiceMoved} from moved tokens`,
  );
  add(
    'no late-join intent was dead-lettered, and every intent the API queued was processed',
    s.lateJoin.deadLettered === 0 &&
      s.lateJoin.polled === s.api.intents.enqueued &&
      s.lateJoin.processed === s.lateJoin.polled,
    `queued ${s.api.intents.enqueued}, processed ${s.lateJoin.processed}, dead ${s.lateJoin.deadLettered}`,
  );
  const gaveUp = s.lateJoin.logEvents['latejoin.plan_wait_gave_up'] ?? 0;
  add(
    'late-join never gave up waiting for the plan (latejoin.plan_wait_gave_up == 0)',
    gaveUp === 0,
    `${gaveUp}; deferred ${s.lateJoin.logEvents['latejoin.waiting_for_plan'] ?? 0} times`,
  );
  // Placed against the read window of the device's cell (fanout.cell_planned).
  const ph = s.phases;
  add(
    'cell read windows were recorded (fanout.cell_planned) for the cells new devices registered in',
    s.cellsTimed > 0 && ph.unknown.n === 0,
    `${s.cellsTimed} cells timed, ${ph.unknown.n} devices in an untimed cell`,
  );
  add(
    `devices that registered before their cell was read got only the planned push (${ph.before.n} devices)`,
    ph.before.late === 0 && ph.before.planned === ph.before.n,
    `${ph.before.planned} planned, ${ph.before.late} late-join`,
  );
  add(
    `devices that registered after their cell was read got only the late-join push (${ph.after.n} devices)`,
    ph.after.planned === 0 && ph.after.late === ph.after.n,
    `${ph.after.planned} planned, ${ph.after.late} late-join`,
  );
  add(
    `devices that registered during the read got exactly one push (${ph.during.n} devices)`,
    ph.during.planned + ph.during.late === ph.during.n,
    `${ph.during.planned} planned, ${ph.during.late} late-join`,
  );
  add(
    'senders and mocks agree on accepted pushes (with surge)',
    surge.collected.mocks.ok >=
      surge.collected.senders.reduce((n, x) => n + x.perAlert.reduce((m, p) => m + p.ok, 0), 0),
    `${surge.collected.mocks.ok}`,
  );
  add(
    'queues drained and nothing went to the dead-letter queue (with surge)',
    surge.collected.brokerFinal.empty && surge.collected.brokerFinal.dlq === 0,
    `dlq ${surge.collected.brokerFinal.dlq}`,
  );

  // ----- late-join latency after the planned fan-out ended -----
  const afterP99 = quantile(s.latency.after, 0.99);
  add(
    `late-join p99 <= ${LATE_AFTER_DRAIN_P99_LIMIT_MS / 1000} s for places saved after the main fan-out ended (${s.latency.after.length} devices)`,
    s.latency.after.length < 50 || afterP99 <= LATE_AFTER_DRAIN_P99_LIMIT_MS,
    `p99 ${round(afterP99 / 1000, 2)} s`,
  );

  // ----- the cost to the main alert -----
  const baseLastS = base.main.lastMs / 1000;
  const surgeLastS = surge.main.lastMs / 1000;
  const bound = slowdownBoundS(scale, baseLastS);
  add(
    `the surge slowed the main alert's time to last push by no more than the agreed bound (baseline ${round(baseLastS, 1)} s, limit ${round(bound, 1)} s = x${SLOWDOWN_LIMIT[scale].factor} + ${SLOWDOWN_LIMIT[scale].slackS} s)`,
    surgeLastS <= bound,
    `with surge ${round(surgeLastS, 1)} s`,
  );

  // ----- numbers -----
  metrics.push(
    { name: 'seed devices', value: sizes.seedDevices, unit: 'devices' },
    { name: 'new devices registered', value: lf.devicesCreated, unit: 'devices' },
    { name: 'registration rate achieved', value: lf.achievedPerMin, unit: 'devices/min' },
    { name: 'surge duration', value: round((lf.finishedAt - lf.startedAt) / 1000, 1), unit: 's' },
    { name: 'requests (create, push token, place)', value: lf.requests, unit: 'requests' },
    {
      name: 'registration errors',
      value: lf.errors,
      unit: 'requests',
      target: { op: '==', value: 0 },
    },
    { name: 'registration p50, device flow (3 requests)', value: lf.latency.flow.p50, unit: 'ms' },
    {
      name: 'registration p99, device flow (3 requests)',
      value: lf.latency.flow.p99,
      unit: 'ms',
      target: { op: '<=', value: REGISTER_P99_LIMIT_MS },
    },
    { name: 'POST /devices p50', value: lf.latency.create.p50, unit: 'ms' },
    { name: 'POST /devices p99', value: lf.latency.create.p99, unit: 'ms' },
    { name: 'PUT push-token p50', value: lf.latency.token.p50, unit: 'ms' },
    { name: 'PUT push-token p99', value: lf.latency.token.p99, unit: 'ms' },
    { name: 'POST /places p50', value: lf.latency.place.p50, unit: 'ms' },
    { name: 'POST /places p99', value: lf.latency.place.p99, unit: 'ms' },
    { name: 'places retried after "outside California"', value: lf.outsideRetries, unit: 'places' },
    { name: 'tokens moved (reinstall)', value: lf.reuse.moved, unit: 'moves' },
    { name: 'rapid-move groups (200,200,200,409,409)', value: lf.rapid.ok, unit: 'groups' },
    { name: 'moves refused with 409', value: lf.rapid.rejected409, unit: 'requests' },
    { name: 'late-join pushes (accepted)', value: s.lateCount, unit: 'pushes' },
    {
      name: 'new devices missed',
      value: s.missed,
      unit: 'devices',
      target: { op: '==', value: 0 },
    },
    {
      name: 'late-join duplicates',
      value: s.lateTwice,
      unit: 'pushes',
      target: { op: '==', value: 0 },
    },
    {
      name: 'late-join latency p50 (place saved to push accepted)',
      value: q(s.latency.all, 0.5),
      unit: 's',
    },
    { name: 'late-join latency p99', value: q(s.latency.all, 0.99), unit: 's' },
    {
      name: `late-join latency p50, place saved during the main fan-out (${s.latency.during.length})`,
      value: q(s.latency.during, 0.5),
      unit: 's',
    },
    {
      name: 'late-join latency p99, place saved during the main fan-out',
      value: q(s.latency.during, 0.99),
      unit: 's',
    },
    {
      name: `late-join latency p50, place saved after it (${s.latency.after.length})`,
      value: q(s.latency.after, 0.5),
      unit: 's',
    },
    {
      name: 'late-join latency p99, place saved after it',
      value: q(s.latency.after, 0.99),
      unit: 's',
    },
    {
      name: 'intent wait in the late-join queue p50',
      value: round(s.lateJoin.waitP50, 0),
      unit: 'ms',
    },
    {
      name: 'intent wait in the late-join queue p99',
      value: round(s.lateJoin.waitP99, 0),
      unit: 'ms',
    },
    {
      name: 'devices sent by both paths (planned and late-join)',
      value: s.both,
      unit: 'devices',
      target: { op: '==', value: 0 },
    },
    {
      name: 'moved push tokens pushed by both paths (old holder planned, new holder late-join)',
      value: s.bothMoved,
      unit: 'tokens',
    },
    {
      name: 'new devices pushed by the planned fan-out only',
      value: s.plannedOnly,
      unit: 'devices',
    },
    { name: 'new devices pushed by late-join only', value: s.lateOnly, unit: 'devices' },
    {
      name: 'tokens the planned fan-out sent twice because they moved mid-read',
      value: s.plannedTwiceMoved,
      unit: 'tokens',
    },
    { name: 'devices registered before their cell was read', value: ph.before.n, unit: 'devices' },
    {
      name: 'devices registered during the read of their cell',
      value: ph.during.n,
      unit: 'devices',
    },
    { name: 'devices registered after their cell was read', value: ph.after.n, unit: 'devices' },
    {
      name: 'late-join deferrals (latejoin.waiting_for_plan)',
      value: s.lateJoin.logEvents['latejoin.waiting_for_plan'] ?? 0,
      unit: 'intents',
    },
    {
      name: 'late-join gave up waiting (latejoin.plan_wait_gave_up)',
      value: gaveUp,
      unit: 'intents',
      target: { op: '==', value: 0 },
    },
    {
      name: `late-join latency p50, devices that waited for the plan (${ev.waited.length})`,
      value: q(ev.waited, 0.5),
      unit: 's',
    },
    {
      name: 'late-join latency p99, devices that waited for the plan',
      value: q(ev.waited, 0.99),
      unit: 's',
    },
    {
      name: 'main alert: time to 90%, baseline',
      value: round(base.main.t90Ms / 1000, 2),
      unit: 's',
    },
    {
      name: 'main alert: time to 90%, with surge',
      value: round(surge.main.t90Ms / 1000, 2),
      unit: 's',
    },
    { name: 'main alert: time to last push, baseline', value: round(baseLastS, 2), unit: 's' },
    {
      name: 'main alert: time to last push, with surge',
      value: round(surgeLastS, 2),
      unit: 's',
      target: { op: '<=', value: round(bound, 2) },
    },
    {
      name: 'main alert: slowdown from the surge',
      value: baseLastS > 0 ? round(surgeLastS / baseLastS, 3) : 0,
      unit: 'x',
    },
    { name: 'main alert: pushes accepted (planned)', value: surge.main.accepted, unit: 'pushes' },
    {
      name: 'main alert: shards done after arrival, baseline',
      value: round((base.main.shardDoneAt - base.main.arrivedAt) / 1000, 1),
      unit: 's',
    },
    {
      name: 'main alert: shards done after arrival, with surge',
      value: round((surge.main.shardDoneAt - surge.main.arrivedAt) / 1000, 1),
      unit: 's',
    },
  );
  if (seed) metrics.push({ name: 'seed time', value: round(seed.seedMs / 1000, 1), unit: 's' });
  notes.push(
    'Late-join sends go onto the same urgent queue as the main alert, behind the messages already queued, so a place saved during the fan-out waits for that backlog (see the "during" and "after" latencies). The queue is first in, first out within a lane.',
    'Late-join (G5) checks the planned fan-out first: the shard workers write a plan record per cell and the senders store the recipient set of each batch they send. A device in the plan is skipped; one that is not is sent at once; where the plan is not known yet the intent waits (latejoin.waiting_for_plan, 15 s in production, 1 s in smoke) and is decided again. Devices are classified by when their place was saved against the read window of their cell (fanout.cell_planned).',
    `The slowdown bound is a limit of x${SLOWDOWN_LIMIT[scale].factor} plus ${SLOWDOWN_LIMIT[scale].slackS} s over the baseline's time to last push.`,
  );
  const verdict = checks.every((c) => c.ok) ? 'pass' : 'fail';
  return { name: 'signup surge', verdict, durationMs, metrics, checks, notes };
}

function intro(cfg: FleetConfig, sizes: SurgeSizes, smoke: boolean): string {
  return [
    `One county-wide warning fans out to ${fmt(sizes.seedDevices)} seeded devices (real planner, shard workers, senders in separate processes, mock APNs and FCM). While it does, ${fmt(sizes.newDevices)} new devices register at ${fmt(sizes.perMin)} per minute through the real API handler behind node:http (POST /devices, PUT /devices/me/push-token, POST /places), served by a separate process with indexed in-memory stores. The API writes the cell index (the shard workers read those entries too) and queues late-join intents; the real late-join handler sends the active alert to each new device through the same queues and senders.`,
    '',
    `The same fan-out is run first without the surge (baseline) and then with it. ${smoke ? 'Smoke scale caps the senders at ' + fmt(sizes.smokePushesPerSec) + ' pushes per second so the fan-out overlaps the surge. ' : ''}${cfg.senders} senders, ${cfg.shardWorkers} shard workers, ${cfg.projects} FCM projects.`,
  ].join('\n');
}
