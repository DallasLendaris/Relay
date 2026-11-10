/**
 * t1: single-process baseline. Wraps the existing local simulation (`runFanoutSim` + the check-in
 * burst) exactly as the recorded 1M run did (loadtest/results/fanout-1m-local-v2.md: 2 CPUs, 2 sender
 * consumers, one Node process), in a child process, and compares the time to last push with that
 * recorded baseline. Full: 1,000,000 devices, 6 GB heap. Smoke: 5,000 devices, 512 MB heap.
 */
import { fileURLToPath } from 'node:url';
import { spawnWorker } from '../lib/child.js';
import { fmt, writeTestReport } from '../lib/report.js';
import { RssSampler } from '../lib/rss.js';
import type { BurstResult } from '../../sim/checkin-burst.js';
import type { FanoutSimResult } from '../../sim/fanout-sim.js';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../types.js';

/** Time to last successful push in the recorded 2-CPU run (loadtest/results/fanout-1m-local-v2.md). */
export const BASELINE_2CPU_LAST_PUSH_S = 217.0;

type Worker = { fanout: Omit<FanoutSimResult, 'tokens'>; burst: BurstResult | null };

export const t1Baseline: SuiteTest = {
  id: 't1',
  title: 'Single-process baseline (1M fan-out + check-in burst)',
  description:
    'The existing local simulation: 1,000,000 devices, one alert, real planner/workers/senders against the mock APNs/FCM, then a check-in burst; compared with the recorded 2-CPU run.',
  estimatedMinutes: 5,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const t0 = Date.now();
    const full = ctx.scale === 'full';
    const devices = full ? 1_000_000 : 5_000;
    const heapMb = full ? 6144 : 512;
    ctx.log(
      `running the single-process simulation with ${fmt(devices)} devices, ${heapMb} MB heap`,
    );

    const rss = new RssSampler();
    const worker = spawnWorker(
      fileURLToPath(new URL('./t1-worker.ts', import.meta.url)),
      ['--devices', String(devices), '--senders', '2'],
      { heapMb },
    );
    rss.addSource(() => worker.rssNow);
    let out: Worker;
    try {
      out = (await worker.result) as Worker;
    } finally {
      worker.kill();
    }
    const peakRssMb = Math.max(rss.stop(), Math.round(worker.rssPeak));
    const { fanout: r, burst: b } = out;

    const lastPushS = (r.timing.lastPushMs ?? 0) / 1000;
    const firstPushS = (r.timing.firstPushMs ?? 0) / 1000;
    const metrics: Metric[] = [
      { name: 'time to last push', value: lastPushS, unit: 's' },
      { name: 'sends/s mean', value: r.throughput.meanOkPerSec, unit: 'pushes/s' },
      { name: 'devices', value: r.counts.devices, unit: 'devices' },
      { name: 'time to first push', value: firstPushS, unit: 's' },
      { name: 'sends/s best 5 s window', value: r.throughput.peakWindowOkPerSec, unit: 'pushes/s' },
      {
        name: 'missed devices',
        value: r.exactlyOnce.missing,
        unit: 'devices',
        target: { op: '==', value: 0 },
      },
      {
        name: 'duplicates (not cross-cell)',
        value: r.exactlyOnce.unexpectedDuplicates,
        unit: 'devices',
        target: { op: '==', value: 0 },
      },
      {
        name: 'cross-cell duplicates',
        value: r.exactlyOnce.knownTwoCellDuplicates,
        unit: 'devices',
        target: { op: '==', value: 0 },
      },
    ];
    if (b) {
      metrics.push(
        { name: 'check-ins/s (API)', value: b.api.perSec, unit: 'check-ins/s' },
        { name: 'check-ins/s (aggregator)', value: b.aggregator.perSec, unit: 'check-ins/s' },
      );
    }
    metrics.push({ name: 'peak RSS', value: peakRssMb, unit: 'MB' });
    if (full && lastPushS > 0) {
      metrics.push({
        name: 'speedup_vs_2cpu_baseline',
        value: Math.round((BASELINE_2CPU_LAST_PUSH_S / lastPushS) * 100) / 100,
        unit: 'x',
      });
    }

    const checks: ScenarioResult['checks'] = [
      { name: 'missed devices == 0', ok: r.exactlyOnce.missing === 0 },
      {
        name: 'no duplicates other than cross-cell',
        ok: r.exactlyOnce.unexpectedDuplicates === 0,
        detail: `${r.exactlyOnce.duplicatesUnexplained} unexplained, ${r.exactlyOnce.duplicatesAfterLostResponse} after a lost response`,
      },
      { name: 'cross-cell duplicates == 0', ok: r.exactlyOnce.knownTwoCellDuplicates === 0 },
      { name: 'exactly-once verdict', ok: r.exactlyOnce.ok },
      {
        name: 're-plan of the same alert sends nothing new',
        ok: r.replan.newPushRequests === 0,
        detail: `${r.replan.newPushRequests} new push requests`,
      },
      {
        name: 'check-in burst: per-cell totals match',
        ok: b?.verification.ok === true,
        detail: b ? `${b.verification.perCellMismatches} mismatching cells` : 'no burst ran',
      },
      { name: 'check-in API errors == 0', ok: b?.api.errors === 0 },
      { name: 'check-in aggregator failed items == 0', ok: b?.aggregator.failedItems === 0 },
    ];
    const notes = [
      full
        ? `Baseline: ${BASELINE_2CPU_LAST_PUSH_S} s to last push on 2 CPUs (loadtest/results/fanout-1m-local-v2.md); this run: ${lastPushS.toFixed(1)} s. The simulation is one Node process (one JS thread) with the mocks in-process, so extra cores help little here; t2 measures the multi-process case.`
        : 'Smoke scale: sizes are tiny, so no comparison with the 217 s baseline is made.',
    ];
    const scenario: ScenarioResult = {
      name: `fan-out ${fmt(devices)} devices, single process`,
      verdict: checks.every((c) => c.ok) ? 'pass' : 'fail',
      durationMs: Date.now() - t0,
      metrics,
      checks,
      notes,
    };
    const result: TestResult = {
      id: 't1',
      title: t1Baseline.title,
      verdict: scenario.verdict,
      startedAt,
      durationMs: Date.now() - t0,
      scenarios: [scenario],
      peakRssMb,
    };
    writeTestReport(
      ctx,
      result,
      `The existing local simulation, unchanged: ${fmt(devices)} seeded devices, one signed alert, the real planner, shard workers, \`processBatch\`, \`ApnsSender\` (h2c) and \`FcmSender\` against the mock APNs and FCM (default error mix, FCM quota 10,000/s, 2 sender consumers), then one check-in per delivered device through the check-in API handler and the real aggregator. Single child process, ${heapMb} MB heap. Re-plan, retry and breaker details are in \`t1.json\`.`,
    );
    return result;
  },
};
