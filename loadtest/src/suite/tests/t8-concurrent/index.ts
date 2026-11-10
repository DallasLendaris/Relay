/**
 * t8: concurrent alerts. Twenty alerts arrive within about 10 s over a seeded Santa Clara County
 * (1,000,000 devices): a county-wide warning, city-sized emergencies inside it, a small zone inside
 * one of those, advisories and watches on top, a statewide-like watch and two statements (which
 * must push nothing). The REAL planner, shard workers and senders run in separate processes against
 * mock APNs and FCM; the queues and the idempotency table live in a broker process.
 *
 * Measured: per-alert time to last push and to 90%, per severity; that urgent-queue alerts
 * (emergency, warning) finish before standard ones (watch, advisory) whenever both were queued;
 * exactly one push per (alert, device), checked from the senders' ledgers and again from what the
 * mocks received; statements push nothing.
 *
 * Smoke: 5,000 devices and 6 alerts over about 2 s.
 */
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import type { Alert } from '@relay/shared';
import { fmt, writeTestReport } from '../../lib/report.js';
import type { Metric, ScenarioResult, SuiteContext, SuiteTest, TestResult } from '../../types.js';
import { createAlertKeys } from '../t2-fanout/alert.js';
import { sortedUnique } from '../t2-fanout/verify.js';
import { spawnCtl } from './ctl.js';
import {
  FULL_SCRIPT,
  SMOKE_SCRIPT,
  buildScript,
  isStandard,
  isUrgent,
  type AlertSpec,
} from './alerts.js';
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
  type Fleet,
  type FleetConfig,
  type SeedInfo,
} from './fleet.js';
import {
  concatWords,
  exactlyOnceDetailed,
  fractionAt,
  lastOf,
  mergeTimelines,
  quantile,
  readWords,
  round,
  timeAtFraction,
  tokenLevelCheck,
  type AlertTimeline,
  type Detailed,
} from './ledger.js';
import type { ExpectedReply } from './expected-worker.js';
import { fileURLToPath } from 'node:url';

const here = (name: string): string => fileURLToPath(new URL(`./${name}`, import.meta.url));

export interface AlertOutcome {
  idx: number;
  spec: AlertSpec;
  alert: Alert;
  /** Epoch ms the planner received the alert. */
  arrivedAt: number;
  expectedDevices: number;
  accepted: number;
  eo: Detailed;
  t90Ms: number;
  lastMs: number;
  /** Epoch ms of 90% and of the last accepted push (absolute). */
  t90At: number;
  lastAt: number;
  shardMs: number;
  planMs: number;
  recipients: number;
  refused: string | null;
}

export function devicesFor(scale: 'full' | 'smoke', env = process.env): number {
  if (scale === 'smoke') return 5_000;
  const n = Number(env['T8_DEVICES']);
  return Number.isFinite(n) && n >= 1000 ? Math.floor(n) : 1_000_000;
}

export const t8: SuiteTest = {
  id: 't8',
  title: 'Concurrent alerts',
  description:
    '20 alerts (emergency, warning, watch, advisory, statements) arrive within 10 s over 1,000,000 devices with overlapping areas; real planner, shard workers and senders in separate processes: per-alert time to last push, urgent before standard, exactly one push per (alert, device), statements push nothing.',
  estimatedMinutes: 15,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const wall0 = Date.now();
    const smoke = ctx.scale === 'smoke';
    const devices = devicesFor(ctx.scale);
    const script = smoke ? SMOKE_SCRIPT : FULL_SCRIPT;
    const runDir = join(ctx.dataDir, 't8-run');
    mkdirSync(runDir, { recursive: true });
    const cfg = fleetConfig(ctx, devices);
    let fleet: Fleet | undefined;
    let peakRssMb = 0;
    let error: string | undefined;
    let outcomes: AlertOutcome[] = [];
    let drain: DrainState | undefined;
    let collected: Collected | undefined;
    let expected: ExpectedReply | undefined;
    let seed: SeedInfo | undefined;
    let tokenCheck: ReturnType<typeof tokenLevelCheck> | undefined;
    let lostTokens = 0;
    const scenarioStart = Date.now();
    try {
      ctx.log(`t8: ${script.length} alerts over ${fmt(devices)} devices (${ctx.scale})`);
      const keys = createAlertKeys();
      const alerts = await buildScript(keys, script, ctx.scale);
      const alertsFile = writeAlertsFile(runDir, alerts);
      ctx.log(
        `t8: alert cells: ${alerts.map((a, i) => `#${i} ${fmt(a.area.h3Cells.length)}`).join(', ')}`,
      );
      seed = await ensureCountySeed(ctx, cfg, devices, 't8');

      // Who must get what, from the seed generator (a separate process, before anything is measured).
      const ex = await spawnCtl(here('expected-worker.ts'), 'expected', { heapMb: cfg.heapMb.ctl });
      try {
        expected = await ex.req<ExpectedReply>('run', {
          alertsFile,
          devices,
          seed: cfg.seed,
          outDir: runDir,
        });
      } finally {
        await ex.stop();
      }
      ctx.log(
        `t8: expected pushes per alert: ${expected.perAlert.map((n, i) => `#${i} ${fmt(n)}`).join(', ')} (${fmt(expected.pairs)} in all, ${round(expected.ms / 1000, 1)} s)`,
      );

      fleet = await startFleet(cfg, keys.publicPem);
      const rss = watchMemory(fleet);
      try {
        await prepareRun(fleet, seed, alertsFile, keys.publicPem);
        await startSenders(fleet, keys.publicPem, alertsFile, runDir);
        ctx.log(
          `t8: fleet up (${cfg.senders} senders, ${cfg.shardWorkers} shard workers, ${cfg.apnsMocks} APNs and ${cfg.fcmMocks} FCM mocks, ${cfg.projects} FCM projects); running`,
        );
        drain = await conduct(fleet, script, { log: (m) => ctx.log(`t8: ${m}`) });
        if (drain.error) error = drain.error;
        collected = await collect(fleet, runDir);
      } finally {
        peakRssMb = rss.stop();
      }

      // ---------- verification ----------
      const senders = collected.senders;
      const timelines = mergeTimelines(
        senders.map((s) => s.timeline),
        alerts.length,
        cfg.slotMs,
      );
      outcomes = alerts.map((alert, idx): AlertOutcome => {
        const spec = script[idx] as AlertSpec;
        const ex1 = sortedUnique(readWords(expected!.files.perAlert[idx] as string));
        const delivered = concatWords(senders.map((s) => readWords(s.files.ok[idx] as string)));
        const dead = concatWords(senders.map((s) => readWords(s.files.dead[idx] as string)));
        const eo = exactlyOnceDetailed(ex1, delivered, dead);
        const st = drain!.planner.alerts[idx];
        const tl = timelines[idx] as AlertTimeline;
        const arrivedAt = st?.arrivedAt ?? drain!.arrivals[idx] ?? 0;
        const t90At = timeAtFraction(tl, 0.9);
        const lastAt = lastOf(tl);
        return {
          idx,
          spec,
          alert,
          arrivedAt,
          expectedDevices: ex1.length,
          accepted: tl.total,
          eo,
          t90At,
          lastAt,
          t90Ms: t90At > 0 ? t90At - arrivedAt : 0,
          lastMs: lastAt > 0 ? lastAt - arrivedAt : 0,
          shardMs: st && st.shardDoneAt > 0 ? st.shardDoneAt - arrivedAt : 0,
          planMs: st?.planMs ?? 0,
          recipients: st?.recipients ?? 0,
          refused: st?.refused ?? null,
        };
      });

      // What the mocks themselves recorded, per token, against how many alerts cover each device.
      const expAll = readWords(expected.files.all);
      expAll.sort();
      const mockDelivered = concatWords(collected.mockFiles.map((f) => readWords(f.delivered)));
      mockDelivered.sort();
      const mockDead = sortedUnique(concatWords(collected.mockFiles.map((f) => readWords(f.dead))));
      const lost = sortedUnique(concatWords(senders.map((s) => readWords(s.files.lost))));
      lostTokens = lost.length;
      tokenCheck = tokenLevelCheck(expAll, mockDelivered, mockDead, lost);

      timelinesForReport = timelines;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      ctx.log(`t8 failed: ${error}`);
    } finally {
      peakRssMb = Math.max(peakRssMb, ...(fleet?.procs() ?? []).map((p) => Math.round(p.rssPeak)));
      await fleet?.stop();
      rmSync(runDir, { recursive: true, force: true });
    }

    const scenario = buildScenario(
      ctx,
      cfg,
      script,
      devices,
      outcomes,
      drain,
      collected,
      expected,
      tokenCheck,
      lostTokens,
      error,
      Date.now() - scenarioStart,
      seed,
      timelinesForReport,
    );
    const verdict = scenario.verdict;
    const result: TestResult = {
      id: 't8',
      title: t8.title,
      verdict,
      startedAt,
      durationMs: Date.now() - wall0,
      scenarios: [scenario],
      peakRssMb,
      ...(error ? { error } : {}),
    };
    writeTestReport(ctx, result, intro(cfg, devices, script, outcomes, drain));
    return result;
  },
};

let timelinesForReport: AlertTimeline[] = [];

// ---------- analysis ----------

export interface PriorityResult {
  compared: number;
  violations: string[];
  /** Pairs left out because the standard alert was at least half done when the urgent one arrived. */
  skipped: number;
}

/**
 * Urgent-lane alerts must finish before standard-lane alerts that were still queued when they
 * arrived: for every (urgent U, standard S) pair where S had under half of its pushes accepted at U's
 * arrival, U's 90% time must not be later than S's. `which` narrows the severities compared.
 */
export function priorityCheck(
  outcomes: AlertOutcome[],
  timelines: AlertTimeline[],
  which: (u: AlertOutcome, s: AlertOutcome) => boolean,
): PriorityResult {
  const res: PriorityResult = { compared: 0, violations: [], skipped: 0 };
  const live = outcomes.filter((o) => o.accepted > 0 && o.t90At > 0);
  for (const u of live.filter((o) => isUrgent(o.spec.severity))) {
    for (const s of live.filter((o) => isStandard(o.spec.severity))) {
      if (!which(u, s)) continue;
      const progress = fractionAt(timelines[s.idx] as AlertTimeline, u.arrivedAt);
      if (progress >= 0.5) {
        res.skipped++;
        continue;
      }
      res.compared++;
      if (u.t90At > s.t90At) {
        res.violations.push(
          `#${u.idx} ${u.spec.severity} 90% at ${round((u.t90At - u.arrivedAt) / 1000, 2)} s after its arrival, after #${s.idx} ${s.spec.severity} 90% (${round((s.t90At - u.arrivedAt) / 1000, 2)} s after the same instant)`,
        );
      }
    }
  }
  return res;
}

function buildScenario(
  ctx: SuiteContext,
  cfg: FleetConfig,
  script: AlertSpec[],
  devices: number,
  outcomes: AlertOutcome[],
  drain: DrainState | undefined,
  collected: Collected | undefined,
  expected: ExpectedReply | undefined,
  tokenCheck: ReturnType<typeof tokenLevelCheck> | undefined,
  lostTokens: number,
  error: string | undefined,
  durationMs: number,
  seed: SeedInfo | undefined,
  timelines: AlertTimeline[],
): ScenarioResult {
  const checks: ScenarioResult['checks'] = [];
  const metrics: Metric[] = [];
  const notes: string[] = [];
  const add = (name: string, ok: boolean, detail?: string): void => {
    checks.push({ name, ok, ...(detail ? { detail } : {}) });
  };
  add('run completed without an error', !error, error);
  if (outcomes.length === 0 || !drain || !collected || !expected) {
    return { name: 'concurrent alerts', verdict: 'fail', durationMs, metrics, checks, notes };
  }

  const pushing = outcomes.filter((o) => o.spec.severity !== 'statement');
  const statements = outcomes.filter((o) => o.spec.severity === 'statement');
  const sum = (f: (o: AlertOutcome) => number): number => outcomes.reduce((s, o) => s + f(o), 0);

  // ----- planning -----
  const refusedPushing = pushing.filter((o) => o.refused !== null);
  add(
    'every alert that pushes was planned (none refused)',
    refusedPushing.length === 0,
    refusedPushing.map((o) => `#${o.idx} ${o.refused}`).join(', ') || undefined,
  );
  const stmtBad = statements.filter(
    (o) =>
      o.refused !== 'informational' ||
      o.accepted > 0 ||
      o.eo.outside > 0 ||
      (drain.planner.alerts[o.idx]?.jobsTotal ?? 0) > 0 ||
      o.recipients > 0,
  );
  add(
    `statements push nothing (${statements.length} statements: planner refused them as informational, no shard job, no push)`,
    statements.length > 0 && stmtBad.length === 0,
    stmtBad.map((o) => `#${o.idx}`).join(', ') || undefined,
  );
  const planMismatch = pushing.filter((o) => o.recipients !== o.expectedDevices);
  add(
    'recipients the shard workers planned == devices in each alert area (per alert)',
    planMismatch.length === 0,
    planMismatch
      .map((o) => `#${o.idx} planned ${o.recipients} expected ${o.expectedDevices}`)
      .join('; ') || undefined,
  );
  add(
    'no shard job failed or was refused',
    drain.planner.alerts.every((a) => a.failedJobs === 0 && a.refusedJobs === 0),
  );

  // ----- exactly once per (alert, device) -----
  const missed = sum((o) => o.eo.missed);
  const dupes = sum((o) => o.eo.duplicatePushes);
  const outside = sum((o) => o.eo.outside);
  add('missed (alert, device) pairs == 0', missed === 0, `${missed}`);
  add('duplicate pushes per (alert, device) == 0', dupes === 0, `${dupes}`);
  add(
    'pushes to a device outside the alert area, or under the wrong alert == 0',
    outside === 0,
    `${outside}`,
  );
  const overlap3 = (expected.overlap ?? []).reduce((s, n, k) => s + (k >= 3 ? n : 0), 0);
  const overlapMax = Math.max(0, (expected.overlap ?? []).length - 1);
  add(
    'the script has devices covered by 3 or more alerts at once',
    overlap3 > 0,
    `${fmt(overlap3)} devices, up to ${overlapMax} alerts`,
  );
  if (tokenCheck) {
    add(
      'mock side: accepted pushes per token == number of alerts covering the device',
      tokenCheck.extra === 0 && tokenCheck.missing === 0,
      `extra ${tokenCheck.extra}, missing ${tokenCheck.missing}; excused: ${tokenCheck.excusedLost} lost-response retries, ${tokenCheck.excusedDead} dead tokens`,
    );
  }
  const ledgerOk = sum((o) => o.accepted);
  const mockOk = collected.mocks.ok;
  add(
    'senders and mocks agree on the number of accepted pushes',
    mockOk >= ledgerOk && mockOk - ledgerOk <= lostTokens,
    `senders ${fmt(ledgerOk)}, mocks ${fmt(mockOk)}, lost responses ${lostTokens}`,
  );
  add(
    'queues drained and nothing went to the dead-letter queue',
    collected.brokerFinal.empty && collected.brokerFinal.dlq === 0,
    `dlq ${collected.brokerFinal.dlq}`,
  );

  // ----- urgent before standard -----
  const all = priorityCheck(outcomes, timelines, () => true);
  add(
    `urgent alerts (emergency, warning) reach 90% before standard alerts (watch, advisory) that were still queued (${all.compared} pairs compared, ${all.skipped} skipped)`,
    all.compared > 0 && all.violations.length === 0,
    all.violations.slice(0, 3).join('; ') ||
      (all.compared === 0 ? 'no pair was queued together' : undefined),
  );
  const emAd = priorityCheck(
    outcomes,
    timelines,
    (u, s) => u.spec.severity === 'emergency' && s.spec.severity === 'advisory',
  );
  add(
    `every emergency reaches 90% before every advisory that was still queued (${emAd.compared} pairs compared)`,
    emAd.compared > 0 && emAd.violations.length === 0,
    emAd.violations.slice(0, 3).join('; ') ||
      (emAd.compared === 0 ? 'no pair was queued together' : undefined),
  );

  // ----- numbers -----
  const arrivalSpreadS =
    (Math.max(...outcomes.map((o) => o.arrivedAt)) -
      Math.min(...outcomes.map((o) => o.arrivedAt))) /
    1000;
  metrics.push(
    { name: 'devices (seed)', value: devices, unit: 'devices' },
    { name: 'alerts', value: outcomes.length, unit: 'alerts' },
    { name: 'arrival spread', value: round(arrivalSpreadS, 1), unit: 's' },
    { name: 'pushes accepted (all alerts)', value: ledgerOk, unit: 'pushes' },
    { name: 'expected (alert, device) pairs', value: expected.pairs, unit: 'pairs' },
    { name: 'devices in 3+ alerts', value: overlap3, unit: 'devices' },
    { name: 'most alerts covering one device', value: overlapMax, unit: 'alerts' },
    {
      name: 'missed (alert, device) pairs',
      value: missed,
      unit: 'pairs',
      target: { op: '==', value: 0 },
    },
    { name: 'duplicate pushes', value: dupes, unit: 'pushes', target: { op: '==', value: 0 } },
    {
      name: 'dead tokens (unregistered answers)',
      value: collected.mocks.unregistered,
      unit: 'tokens',
    },
    {
      name: 'injected 429/500 (mock)',
      value: collected.mocks.tooManyRequests + collected.mocks.serverError,
      unit: 'responses',
    },
    { name: 'FCM quota 429s (mock)', value: collected.mocks.rateLimited, unit: 'responses' },
    { name: 'SQS messages sent', value: collected.brokerFinal.sentMessages, unit: 'messages' },
    { name: 'SQS redeliveries', value: collected.brokerFinal.redeliveries, unit: 'messages' },
    { name: 'broker backlog peak', value: collected.brokerFinal.peakBytesMb, unit: 'MB' },
    { name: 'run time (first alert to drained)', value: round(drain.wallMs / 1000, 1), unit: 's' },
    {
      name: 'pushes/s overall (first alert to drained)',
      value: Math.round(ledgerOk / Math.max(0.001, drain.wallMs / 1000)),
      unit: 'pushes/s',
    },
  );
  if (seed) metrics.push({ name: 'seed time', value: round(seed.seedMs / 1000, 1), unit: 's' });
  for (const o of outcomes) {
    if (o.spec.severity === 'statement') continue;
    const tag = `#${String(o.idx).padStart(2, '0')} ${o.spec.severity} ${o.spec.name}`;
    metrics.push(
      { name: `${tag}: time to 90%`, value: round(o.t90Ms / 1000, 2), unit: 's' },
      { name: `${tag}: time to last push`, value: round(o.lastMs / 1000, 2), unit: 's' },
    );
  }
  for (const sev of ['emergency', 'warning', 'watch', 'advisory'] as const) {
    const rows = outcomes.filter((o) => o.spec.severity === sev && o.accepted > 0);
    if (rows.length === 0) continue;
    const t90 = rows.map((o) => o.t90Ms / 1000);
    const last = rows.map((o) => o.lastMs / 1000);
    metrics.push(
      {
        name: `${sev}: time to 90% (median of ${rows.length})`,
        value: round(quantile(t90, 0.5), 2),
        unit: 's',
      },
      { name: `${sev}: time to 90% (slowest)`, value: round(Math.max(...t90), 2), unit: 's' },
      {
        name: `${sev}: time to last push (slowest)`,
        value: round(Math.max(...last), 2),
        unit: 's',
      },
    );
  }
  void ctx;
  void cfg;
  void script;
  notes.push(
    "Times are measured from the moment the planner received each alert. Within one queue the order is first in, first out, so an emergency that arrives behind a million-recipient warning waits for the warning's messages already queued; strict priority only separates the urgent queue from the standard queue.",
  );
  const verdict = checks.every((c) => c.ok) ? 'pass' : 'fail';
  return { name: 'concurrent alerts', verdict, durationMs, metrics, checks, notes };
}

function intro(
  cfg: FleetConfig,
  devices: number,
  script: AlertSpec[],
  outcomes: AlertOutcome[],
  drain: DrainState | undefined,
): string {
  const lines = [
    `${script.length} alerts arrive over ${fmt(devices)} seeded devices in Santa Clara County while the REAL planner (planShards), shard workers (processShard, SqsSendQueue), senders (SenderConsumer, processBatch, ApnsSender, FcmProjectSender) run as separate processes (${cfg.senders} senders, ${cfg.shardWorkers} shard workers, 1 planner, ${cfg.apnsMocks} mock APNs, ${cfg.fcmMocks} mock FCM serving ${cfg.projects} Firebase projects, 1 broker for the SQS queues and the idempotency table). Alerts overlap: a device in central San Jose is in several at once and must get exactly one push from each.`,
  ];
  if (outcomes.length > 0 && drain) {
    lines.push(
      '',
      '| # | Severity | Area | Arrives (s) | Devices | Pushes | Shards done (s) | 90% (s) | Last push (s) | Missed | Dupes |',
      '| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    );
    for (const o of outcomes) {
      lines.push(
        `| ${o.idx} | ${o.spec.severity} | ${o.spec.name} | ${round((o.arrivedAt - drain.t0) / 1000, 1)} | ${fmt(o.expectedDevices)} | ${fmt(o.accepted)} | ${o.shardMs > 0 ? round(o.shardMs / 1000, 1) : '-'} | ${o.t90Ms > 0 ? round(o.t90Ms / 1000, 2) : '-'} | ${o.lastMs > 0 ? round(o.lastMs / 1000, 2) : '-'} | ${o.eo.missed} | ${o.eo.duplicatePushes} |`,
      );
    }
    lines.push(
      '',
      'Times in the table are from the instant each alert reached the planner, except "Arrives".',
    );
  }
  return lines.join('\n');
}
