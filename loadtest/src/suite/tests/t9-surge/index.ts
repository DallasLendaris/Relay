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
import { mkdirSync, rmSync } from 'node:fs';
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

interface SurgeOutcome {
  load: LoadFinal;
  lateJoin: LateJoinFinal;
  api: ApiStats;
  apiFinal: { overlayWithTarget: number };
  /** Late-join pushes against the expected tokens of the new devices. */
  lateCheck: Detailed;
  lateCount: number;
  lateOutsideLost: number;
  racers: number;
  racersAfterRead: number;
  /** Late-join push time minus first place saved, ms, split by phase. */
  latency: { all: number[]; during: number[]; after: number[] };
  lateTimes: Float64Array;
  expectedTokens: BigUint64Array;
  strayPlanned: number;
  /** Tokens the planned fan-out sent more than once that no moved token explains. */
  unexplainedRepeats: number;
  /** Tokens sent twice by the planned fan-out because they moved between two devices it read at different times. */
  plannedRepeatsFromMoves: number;
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
    const apiFinal =
      api && !error ? await api.req<{ overlayWithTarget: number }>('final') : undefined;
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
        senders.map((s) => s.files),
        loadFinal,
        ljFinal,
        apiStats,
        apiFinal,
        sortedUnique(dead.slice()),
      );
    }
    return out;
  } finally {
    peakRssMb = Math.max(peakRssMb, rss.stop());
    await Promise.allSettled([api?.stop(), lj?.stop(), load?.stop()]);
    await fleet.stop();
  }
}

function analyseSurge(
  main: MainOutcome,
  senderFiles: Array<{ lateTokens: string; lateTimes: string; lost: string }>,
  load: LoadFinal,
  lateJoin: LateJoinFinal,
  api: ApiStats,
  apiFinal: { overlayWithTarget: number },
  dead: BigUint64Array,
): SurgeOutcome {
  // Expected tokens with the time their first place was saved, sorted by token.
  const e = sortPairs(readWords(load.files.tokens), readDoubles(load.files.firstPlaceAt));
  // Late-join accepted pushes with their times.
  const lateTokens = concatWords(senderFiles.map((f) => readWords(f.lateTokens)));
  const lateTimes = new Float64Array(lateTokens.length);
  let at = 0;
  for (const f of senderFiles) {
    const t = readDoubles(f.lateTimes);
    lateTimes.set(t, at);
    at += t.length;
  }
  const lost = sortedUnique(concatWords(senderFiles.map((f) => readWords(f.lost))));
  const late = sortPairs(lateTokens, lateTimes);
  const lateCheck = exactlyOnceDetailed(e.tokens, late.tokens.slice(), dead);
  // A duplicate is excused when the token's answer was lost (the sender retried an accepted push).
  const lostSet = new Set(lost);
  const dupNotExcused = lateCheck.duplicateHashes.filter((h) => !lostSet.has(h)).length;
  if (dupNotExcused !== lateCheck.duplicateHashes.length) {
    const excused = lateCheck.duplicateHashes.length - dupNotExcused;
    lateCheck.duplicatePushes = Math.max(0, lateCheck.duplicatePushes - excused);
    lateCheck.duplicateDevices = dupNotExcused;
  }

  // Racers: tokens that were in the planned fan-out and also got a late-join push.
  const plannedSorted = main.plannedAll; // sorted
  let racers = 0;
  let racersAfterRead = 0;
  for (let i = 0; i < e.tokens.length; i++) {
    const v = e.tokens[i] as bigint;
    const p = lowerBound(plannedSorted, v);
    if (p < plannedSorted.length && plannedSorted[p] === v) {
      racers++;
      // Its place was saved after the planner had finished reading cells: it cannot have been planned.
      if ((e.values[i] as number) > main.shardDoneAt) racersAfterRead++;
    }
  }
  // Planned pushes to tokens that are neither seed devices nor expected new devices.
  let strayPlanned = 0;
  for (const h of main.plannedExtra) {
    const p = lowerBound(e.tokens, h);
    if (!(p < e.tokens.length && e.tokens[p] === h)) strayPlanned++;
  }

  // Tokens the planned fan-out sent more than once. Only a moved token can explain it: the shards read
  // the old holder's cell before the move and the new holder's cell after it, and the planned path
  // has no per-target claim.
  const moved = sortedUnique(readWords(load.files.moved));
  let unexplainedRepeats = 0;
  let plannedRepeatsFromMoves = 0;
  for (let i = 1; i < plannedSorted.length; i++) {
    if (plannedSorted[i] !== plannedSorted[i - 1]) continue;
    const v = plannedSorted[i] as bigint;
    if (i >= 2 && plannedSorted[i - 2] === v) continue; // count each token once
    const p = lowerBound(moved, v);
    if (p < moved.length && moved[p] === v) plannedRepeatsFromMoves++;
    else unexplainedRepeats++;
  }

  // Latency: late-join push accepted minus the first place saved for that token.
  const all: number[] = [];
  const during: number[] = [];
  const after: number[] = [];
  for (let i = 0; i < late.tokens.length; i++) {
    const v = late.tokens[i] as bigint;
    const p = lowerBound(e.tokens, v);
    if (!(p < e.tokens.length && e.tokens[p] === v)) continue;
    const placeAt = e.values[p] as number;
    const ms = Math.max(0, (late.values[i] as number) - placeAt);
    all.push(ms);
    (placeAt <= main.lastAt ? during : after).push(ms);
  }
  return {
    load,
    lateJoin,
    api,
    apiFinal,
    lateCheck,
    lateCount: late.tokens.length,
    lateOutsideLost: lost.length,
    racers,
    racersAfterRead,
    latency: { all, during, after },
    lateTimes: late.values,
    expectedTokens: e.tokens,
    strayPlanned,
    unexplainedRepeats,
    plannedRepeatsFromMoves,
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

  // ----- late join: exactly once -----
  const lc = s.lateCheck;
  add(
    `late-join missed == 0 (${fmt(lc.expected)} new push targets expected)`,
    lc.missed === 0,
    `${lc.missed} missed${lc.excusedDead ? `, ${lc.excusedDead} excused (unregistered)` : ''}`,
  );
  add(
    'late-join duplicates == 0 (no device got its late-join push twice)',
    lc.duplicatePushes === 0,
    `${lc.duplicatePushes}`,
  );
  add(
    'late-join pushes only to devices the API registered with a place in the area',
    lc.outside === 0,
    `${lc.outside}`,
  );
  add(
    'no late-join intent was dead-lettered, and every intent the API queued was processed',
    s.lateJoin.deadLettered === 0 &&
      s.lateJoin.polled === s.api.intents.enqueued &&
      s.lateJoin.processed === s.lateJoin.polled,
    `queued ${s.api.intents.enqueued}, processed ${s.lateJoin.processed}, dead ${s.lateJoin.deadLettered}`,
  );

  // ----- planned vs late-join overlap (the documented trade-off) -----
  add(
    'every device that got both a planned and a late-join push registered before the planned read finished',
    s.racersAfterRead === 0,
    `${s.racers} such devices, ${s.racersAfterRead} registered after the read`,
  );
  add(
    'the planned fan-out sent only to seed devices and to devices the API had registered',
    s.strayPlanned === 0 && s.unexplainedRepeats === 0,
    `${s.strayPlanned} stray, ${s.unexplainedRepeats} repeated without a token move, ${s.plannedRepeatsFromMoves} repeated because a token moved while the shards were reading`,
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
    { name: 'late-join missed', value: lc.missed, unit: 'devices', target: { op: '==', value: 0 } },
    {
      name: 'late-join duplicates',
      value: lc.duplicatePushes,
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
      value: s.racers,
      unit: 'devices',
    },
    {
      name: 'tokens the planned fan-out sent twice because they moved mid-read',
      value: s.plannedRepeatsFromMoves,
      unit: 'tokens',
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
    'A device whose place was saved before the planned fan-out read its cell can be pushed by both paths: the planned sender does not take the late-join per-target claim, and late-join fails toward a duplicate for a place newer than the alert. The count is reported as "devices sent by both paths"; the check requires each of them to have registered before the read finished.',
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
