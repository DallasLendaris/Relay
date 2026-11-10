/**
 * Test 3: check-in burst. After an alert, up to 1,000,000 phones tap "I'm okay" within minutes.
 * Measures the real check-in route (createApi: stateless device credential, v2 token, input
 * bounds, tombstone check), the conditional idempotent store write, and the real stream
 * aggregator, with open-loop load generators in their own processes.
 *
 * Topology (all separate processes, started per scenario so memory numbers are clean):
 *   L generators --HTTP keep-alive--> K API processes --TCP change stream--> A aggregators
 * Store choice: devices are partitioned over the K API processes (device number % K, like a sticky
 * load balancer), and each process owns the in-memory store for its devices. That keeps the
 * conditional write local (no IPC on the request path) while idempotency still holds, because all
 * requests of one device reach the same store. The real DynamoDB variant is test 4.
 */
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import {
  cellOf,
  classOf,
  isDuplicate,
  nowMs,
  percentileFromHist,
  HIST_BUCKETS,
  type AggDone,
  type ApiDone,
  type GenResult,
  type Segment,
} from './common.js';
import { spawnChild, type Child } from './proc.js';

const DIR = path.dirname(fileURLToPath(import.meta.url));
const P99_LIMIT_MS = 500;

interface ScenarioDef {
  name: string;
  n: number;
  segments: Segment[];
  /** Nominal first-tap rate (devices/s); for the curve, its peak. */
  targetRate: number;
  /** Constant-rate scenarios take part in the saturation search. */
  constant: boolean;
  dupDelaySec: number;
}

function constantScenario(n: number, rate: number, label: string): ScenarioDef {
  return {
    name: label,
    n,
    segments: [{ startSec: 0, endSec: n / rate, share: 1 }],
    targetRate: rate,
    constant: true,
    dupDelaySec: 1,
  };
}

/** 1M taps over 5 minutes, 40% in the first minute (a quarter of all taps in the first 10 s). */
function curveScenario(n: number, durationSec: number): ScenarioDef {
  const k = durationSec / 300;
  const shape: Array<[number, number, number]> = [
    [0, 10, 0.25],
    [10, 60, 0.15],
    [60, 120, 0.25],
    [120, 180, 0.15],
    [180, 240, 0.12],
    [240, 300, 0.08],
  ];
  const segments = shape.map(([a, b, share]) => ({ startSec: a * k, endSec: b * k, share }));
  const peak = Math.max(...segments.map((s) => (s.share * n) / (s.endSec - s.startSec)));
  return {
    name: `realistic curve, ${n.toLocaleString('en-US')} taps over ${Math.round(durationSec)} s (40% in minute 1)`,
    n,
    segments,
    targetRate: Math.round(peak),
    constant: false,
    dupDelaySec: Math.max(0.3, Math.min(1, k * 1)),
  };
}

interface Sizing {
  K: number;
  L: number;
  A: number;
  apiHeapMb: number;
  genHeapMb: number;
  aggHeapMb: number;
  maxSockets: number;
}

function sizing(ctx: SuiteContext): Sizing {
  if (ctx.scale === 'smoke') {
    return { K: 1, L: 1, A: 1, apiHeapMb: 384, genHeapMb: 384, aggHeapMb: 384, maxSockets: 64 };
  }
  const K = Math.max(1, Math.floor(ctx.workers / 2));
  const L = Math.max(1, Math.floor(ctx.workers / 2));
  return {
    K,
    L,
    A: Math.max(1, Math.round(K / 3)),
    apiHeapMb: 3072,
    genHeapMb: 1536,
    aggHeapMb: 3072,
    maxSockets: 128,
  };
}

interface Outcome {
  result: ScenarioResult;
  /** p99 < 500 ms, send rate kept up, and nothing unexpected. */
  sustained: boolean;
  p99: number;
  peakRssMb: number;
}

const round = (x: number, d = 1): number => Math.round(x * 10 ** d) / 10 ** d;

async function runScenario(ctx: SuiteContext, sz: Sizing, def: ScenarioDef): Promise<Outcome> {
  const t00 = Date.now();
  const children: Child[] = [];
  const spawn = (file: string, heap: number, name: string): Child => {
    const c = spawnChild(path.join(DIR, file), [], heap, name);
    children.push(c);
    return c;
  };
  try {
    const READY = 90_000;
    const aggs = Array.from({ length: sz.A }, (_, a) =>
      spawn('agg-worker.ts', sz.aggHeapMb, `agg${a}`),
    );
    const apis = Array.from({ length: sz.K }, (_, k) =>
      spawn('api-worker.ts', sz.apiHeapMb, `api${k}`),
    );
    const gens = Array.from({ length: sz.L }, (_, g) =>
      spawn('gen-worker.ts', sz.genHeapMb, `gen${g}`),
    );
    await Promise.all([...aggs, ...apis, ...gens].map((c) => c.waitFor('loaded', READY)));

    const producersOf = (a: number): number => apis.filter((_, k) => k % sz.A === a).length;
    const aggPorts: number[] = [];
    for (let a = 0; a < sz.A; a++) {
      aggs[a]?.send({ type: 'init', producers: producersOf(a), index: a });
      aggPorts.push(
        (await aggs[a]?.waitFor<{ type: string; port: number }>('ready', READY))?.port ?? 0,
      );
    }
    const apiPorts: number[] = [];
    for (let k = 0; k < sz.K; k++) {
      apis[k]?.send({ type: 'init', n: def.n, aggPort: aggPorts[k % sz.A] });
    }
    for (const api of apis) {
      apiPorts.push((await api.waitFor<{ type: string; port: number }>('ready', READY)).port);
    }

    // ----- load -----
    const lastSec = Math.max(...def.segments.map((s) => s.endSec));
    const t0Ms = nowMs() + 1500;
    ctx.log(`${def.name}: sending`, {
      devices: def.n,
      K: sz.K,
      L: sz.L,
      A: sz.A,
      seconds: round(lastSec),
    });
    gens.forEach((g, i) =>
      g.send({
        type: 'run',
        g: i,
        L: sz.L,
        n: def.n,
        ports: apiPorts,
        t0Ms,
        segments: def.segments,
        dupDelaySec: def.dupDelaySec,
        maxSockets: sz.maxSockets,
      }),
    );
    const genRes = await Promise.all(
      gens.map((g) => g.waitFor<GenResult>('result', (lastSec + 300) * 1000)),
    );
    apis.forEach((a) => a.send({ type: 'finish' }));
    const apiDone = await Promise.all(apis.map((a) => a.waitFor<ApiDone>('done', 120_000)));
    const aggDone = await Promise.all(aggs.map((a) => a.waitFor<AggDone>('done', 600_000)));

    // ----- expected values from the device classes -----
    const expectedByCell = new Map<string, number>();
    let valid = 0;
    let tampered = 0;
    let tombstoned = 0;
    let dupTaps = 0;
    for (let i = 0; i < def.n; i++) {
      const c = classOf(i);
      if (c === 'tampered') tampered++;
      else if (c === 'tombstoned') tombstoned++;
      else {
        valid++;
        const cell = cellOf(i);
        expectedByCell.set(cell, (expectedByCell.get(cell) ?? 0) + 1);
        if (isDuplicate(i)) dupTaps++;
      }
    }
    const requestsExpected = def.n + dupTaps;

    // ----- merge -----
    const hist = new Array<number>(HIST_BUCKETS).fill(0);
    const sum = (f: (g: GenResult) => number): number => genRes.reduce((s, g) => s + f(g), 0);
    for (const g of genRes) g.hist.forEach((c, b) => (hist[b] = (hist[b] ?? 0) + c));
    const maxMs = Math.max(...genRes.map((g) => g.maxMs));
    const sent = sum((g) => g.sent);
    const responses = sum((g) => g.responses);
    const s5xx = sum((g) => g.s5xx);
    const connErrors = sum((g) => g.connErrors);
    const unexpected = sum((g) => g.unexpected);
    const firstSched = Math.min(...genRes.map((g) => g.firstSchedMs));
    const lastPrimary = Math.max(...genRes.map((g) => g.lastIssuedMs));
    const lastDone = Math.max(...genRes.map((g) => g.lastDoneMs));
    const sendSeconds = Math.max(0.001, (lastPrimary - firstSched) / 1000);
    const achieved = def.n / sendSeconds;

    const totals = new Map<string, number>();
    for (const a of aggDone)
      for (const [cell, n] of a.totals) totals.set(cell, (totals.get(cell) ?? 0) + n);
    const accepted = new Map<string, number>();
    for (const a of apiDone)
      for (const [cell, n] of a.perCell) accepted.set(cell, (accepted.get(cell) ?? 0) + n);
    const acceptedTotal = apiDone.reduce((s, a) => s + a.accepted, 0);
    const duplicateSaves = apiDone.reduce((s, a) => s + a.duplicateSaves, 0);
    const totalsSum = [...totals.values()].reduce((s, n) => s + n, 0);
    let aggVsStore = 0;
    let vsExpected = 0;
    for (const cell of new Set([...totals.keys(), ...accepted.keys(), ...expectedByCell.keys()])) {
      if ((totals.get(cell) ?? 0) !== (accepted.get(cell) ?? 0)) aggVsStore++;
      if ((totals.get(cell) ?? 0) !== (expectedByCell.get(cell) ?? 0)) vsExpected++;
    }
    const lastAccepted = Math.max(...apiDone.map((a) => a.lastAcceptedMs));
    const lastAgg = Math.max(...aggDone.map((a) => a.lastBatchDoneMs));
    const lagS = Math.max(0, lastAgg - lastAccepted) / 1000;
    const p50 = percentileFromHist(hist, 0.5);
    const p95 = percentileFromHist(hist, 0.95);
    const p99 = percentileFromHist(hist, 0.99);
    const apiRss = apiDone.reduce((s, a) => s + a.maxRssKb, 0);
    const peakRssMb =
      (apiRss +
        aggDone.reduce((s, a) => s + a.maxRssKb, 0) +
        genRes.reduce((s, g) => s + g.maxRssKb, 0)) /
      1024;
    const clean = unexpected === 0 && s5xx === 0 && connErrors === 0;
    const sustained = p99 < P99_LIMIT_MS && clean && achieved >= 0.95 * def.targetRate;
    const aggBusy = Math.max(...aggDone.map((a) => a.busyMs));

    const metrics: Metric[] = [
      { name: 'devices', value: def.n, unit: 'devices' },
      { name: 'requests sent', value: sent, unit: 'requests' },
      {
        name: def.constant ? 'target rate' : 'target peak rate',
        value: def.targetRate,
        unit: 'taps/s',
      },
      { name: 'achieved rate', value: Math.round(achieved), unit: 'taps/s' },
      { name: 'latency p50', value: round(p50, 2), unit: 'ms' },
      { name: 'latency p95', value: round(p95, 2), unit: 'ms' },
      {
        name: 'latency p99',
        value: round(p99, 2),
        unit: 'ms',
        target: { op: '<=', value: P99_LIMIT_MS },
      },
      { name: 'latency max', value: round(maxMs, 1), unit: 'ms' },
      { name: '403 (tampered token)', value: sum((g) => g.s403), unit: 'responses' },
      { name: '401 (erased device)', value: sum((g) => g.s401), unit: 'responses' },
      { name: '5xx + connection errors', value: s5xx + connErrors, unit: 'responses' },
      { name: 'aggregator lag after last accepted check-in', value: round(lagS, 2), unit: 's' },
      { name: 'busiest aggregator busy time', value: round(aggBusy / 1000, 1), unit: 's' },
      {
        name: 'API event-loop delay p99 (worst process)',
        value: round(Math.max(...apiDone.map((a) => a.loopDelayP99Ms)), 1),
        unit: 'ms',
      },
      { name: 'peak RSS, all processes', value: Math.round(peakRssMb), unit: 'MB' },
    ];
    const checks = [
      {
        name: 'aggregator totals == accepted check-ins, per cell',
        ok: aggVsStore === 0,
        detail: `${aggVsStore} cells differ`,
      },
      {
        name: 'accepted check-ins == unique valid devices sent',
        ok: acceptedTotal === valid,
        detail: `accepted ${acceptedTotal}, valid devices ${valid}`,
      },
      {
        name: 'totals per cell == distinct devices per cell (exact)',
        ok: vsExpected === 0 && totalsSum === valid,
        detail: `${vsExpected} cells differ, sum ${totalsSum} vs ${valid}`,
      },
      {
        name: 'repeated taps not double counted',
        ok: duplicateSaves === dupTaps,
        detail: `${dupTaps} second taps sent, ${duplicateSaves} store rejections`,
      },
      {
        name: 'tampered tokens all 403 and not counted',
        ok: sum((g) => g.s403) === tampered,
        detail: `${sum((g) => g.s403)} of ${tampered}`,
      },
      {
        name: 'erased devices all 401 and not counted',
        ok: sum((g) => g.s401) === tombstoned,
        detail: `${sum((g) => g.s401)} of ${tombstoned}`,
      },
      {
        name: 'every request answered, none unexpected',
        ok: responses === requestsExpected && sent === requestsExpected && unexpected === 0,
        detail: `sent ${sent}, answered ${responses}, expected ${requestsExpected}, unexpected ${unexpected}`,
      },
      {
        name: 'no 5xx or connection errors while p99 < 500 ms',
        ok: p99 >= P99_LIMIT_MS || (s5xx === 0 && connErrors === 0),
        detail: `5xx ${s5xx}, connection errors ${connErrors}, p99 ${round(p99, 1)} ms`,
      },
      {
        name: 'aggregator reported no failed items; redelivered batches kept totals exact',
        ok: aggDone.every((a) => a.failedItems === 0),
        detail: `${aggDone.reduce((s, a) => s + a.redeliveredBatches, 0)} batches delivered twice`,
      },
    ];
    const notes = [
      `${sz.L} generators -> ${sz.K} API processes -> ${sz.A} aggregators; ${aggDone.reduce((s, a) => s + a.batches, 0)} stream batches`,
      `requests = first taps + ${dupTaps} second taps, so the load is ${round((requestsExpected / def.n) * 100 - 100, 1)}% above the nominal rate`,
      sustained
        ? 'sustained: p99 under 500 ms, send rate kept up, no errors'
        : 'not sustained at this rate (p99 >= 500 ms, errors, or generators behind schedule)',
      `last response ${round((lastDone - firstSched) / 1000, 1)} s after the first scheduled tap`,
    ];
    const ok = checks.every((c) => c.ok);
    return {
      result: {
        name: def.name,
        verdict: ok ? 'pass' : 'fail',
        durationMs: Date.now() - t00,
        metrics,
        checks,
        notes,
      },
      sustained,
      p99,
      peakRssMb,
    };
  } finally {
    for (const c of children) c.kill();
  }
}

function report(ctx: SuiteContext, scenarios: ScenarioResult[], saturation: number): string {
  const m = ctx.machine;
  const lines = [
    '# T3: check-in burst',
    '',
    `Scale: ${ctx.scale}. Machine: ${m.cpuModel}, ${m.logicalCpus} logical CPUs, ${m.memoryTotalMb} MB RAM, ${m.platform}, node ${m.node}.`,
    '',
    'Real code under test: the check-in API handler (device credential, v2 token, bounds, tombstones), the conditional check-in store write, and the stream aggregator (batches of up to 1,000, every 20th batch delivered twice). Open-loop generators, latency measured from the scheduled send time. Store: partitioned by device over the API processes.',
    '',
    `Highest constant rate with p99 < ${P99_LIMIT_MS} ms (CPU-bound saturation point): **${saturation.toLocaleString('en-US')} taps/s**`,
    '',
  ];
  for (const s of scenarios) {
    lines.push(`## ${s.name}: ${s.verdict}`, '', '| metric | value |', '|---|---|');
    for (const x of s.metrics)
      lines.push(`| ${x.name} | ${x.value.toLocaleString('en-US')} ${x.unit} |`);
    lines.push('', '| check | result |', '|---|---|');
    for (const c of s.checks)
      lines.push(`| ${c.name} | ${c.ok ? 'ok' : 'FAIL'} ${c.detail ?? ''} |`);
    lines.push('', ...(s.notes ?? []).map((n) => `- ${n}`), '');
  }
  return lines.join('\n');
}

export const t3: SuiteTest = {
  id: 't3',
  title: 'Check-in burst',
  description:
    'Up to 1M devices tap "I\'m okay" at 10k-100k requests/s through the real check-in API, store and aggregator.',
  estimatedMinutes: 14,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date();
    const smoke = ctx.scale === 'smoke';
    const n = smoke ? 5_000 : 1_000_000;
    const sz = sizing(ctx);
    const scenarios: ScenarioResult[] = [];
    let peakRssMb = 0;
    let saturation = 0;
    let error: string | undefined;
    const run = async (def: ScenarioDef): Promise<Outcome> => {
      const o = await runScenario(ctx, sz, def);
      scenarios.push(o.result);
      peakRssMb = Math.max(peakRssMb, o.peakRssMb);
      if (o.sustained && def.constant) saturation = Math.max(saturation, def.targetRate);
      ctx.log(`${def.name}: ${o.result.verdict}`, { p99Ms: round(o.p99, 1) });
      return o;
    };
    try {
      const rates = smoke ? [1_000, 2_000] : [10_000, 25_000, 50_000];
      let last: Outcome | undefined;
      for (const r of rates)
        last = await run(constantScenario(n, r, `constant ${r.toLocaleString('en-US')} taps/s`));
      if (!smoke) {
        // Find the knee: keep raising the rate while the previous one was sustained.
        for (const r of [75_000, 100_000]) {
          if (!last?.sustained) break;
          last = await run(constantScenario(n, r, `constant ${r.toLocaleString('en-US')} taps/s`));
        }
      }
      await run(curveScenario(n, smoke ? 4 : 300));
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
    const failed = scenarios.some((s) => s.verdict === 'fail') || error !== undefined;
    scenarios.push({
      name: 'saturation point',
      verdict: saturation > 0 || smoke ? 'pass' : 'fail',
      durationMs: 0,
      metrics: [
        {
          name: `highest constant rate with p99 < ${P99_LIMIT_MS} ms`,
          value: saturation,
          unit: 'taps/s',
        },
      ],
      checks: [{ name: 'at least the lowest rate sustained', ok: saturation > 0 }],
      notes: [
        'Includes the 5% second taps; generators run in separate processes, so a client-bound limit shows as "achieved rate" below target.',
      ],
    });
    await writeFile(path.join(ctx.outDir, 't3.md'), report(ctx, scenarios, saturation), 'utf8');
    const verdict = failed || scenarios.some((s) => s.verdict === 'fail') ? 'fail' : 'pass';
    return {
      id: 't3',
      title: t3.title,
      verdict,
      startedAt: startedAt.toISOString(),
      durationMs: Date.now() - startedAt.getTime(),
      scenarios,
      peakRssMb: Math.round(peakRssMb + process.resourceUsage().maxRSS / 1024),
      ...(error ? { error } : {}),
    };
  },
};
