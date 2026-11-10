/** Writes `<outDir>/t2.md`: the human report for test 2. */
import { writeFileSync } from 'node:fs';
import path from 'node:path';
import type { SuiteContext, TestResult } from '../../types.js';
import type { T2Config } from './config.js';
import type { ScenarioRaw } from './index.js';

const s1 = (ms: number) => (ms / 1000).toFixed(1);
const k = (n: number) => Math.round(n).toLocaleString('en-US');

export function writeReport(
  ctx: SuiteContext,
  cfg: T2Config,
  result: TestResult,
  raws: ScenarioRaw[],
): void {
  const m = ctx.machine;
  const L: string[] = [];
  L.push(`# t2: multi-process fan-out (${result.verdict.toUpperCase()})`, '');
  L.push(
    `Scale **${cfg.scale}**, ${s1(result.durationMs)} s total. Machine: ${m.cpuModel}, ${m.logicalCpus} logical CPUs, ` +
      `${k(m.memoryTotalMb)} MB RAM (${k(m.memoryFreeAtStartMb)} MB free at start), ${m.platform}, Node ${m.node}.`,
    '',
  );
  L.push(
    'What ran: the REAL planner (planShards), shard worker (processShard + SqsSendQueue), sender ' +
      '(SenderConsumer + processBatch, breaker, dead-target queue, alert signature check) and push adapters ' +
      '(ApnsSender over HTTP/2 + TLS, FcmProjectSender/FcmSender over HTTP/1.1) in separate processes, against ' +
      'the loadtest mock APNs and mock FCM (one 600,000/min quota bucket per Firebase project, injected 0.5% dead ' +
      'tokens, 0.2% 429, 0.1% 500, GOAWAYs) in their own processes. SQS queues (visibility timeout, redelivery, ' +
      'delays, DLQ after 100 receives) and the idempotency table live in a broker process. Every accepted push is ' +
      'recorded by the mocks and checked against the seed: missed, duplicate and outside-area devices.',
    '',
  );
  L.push(
    `Processes: ${cfg.senders} senders, ${cfg.shardWorkers} shard workers (${cfg.jobsPerShardWorker} jobs each at once), ` +
      `${cfg.apnsMocks} mock APNs, ${cfg.fcmMocks} mock FCM, 1 broker. Heap caps: sender ${cfg.heapMb.sender} MB, ` +
      `shard ${cfg.heapMb.shard} MB, mock ${cfg.heapMb.mock} MB, broker ${cfg.heapMb.broker} MB (+${cfg.brokerCapMb} MB queue cap), ` +
      `seed ${cfg.heapMb.seed} MB. FCM rate mode: ${cfg.fcmRateMode}.`,
    '',
  );

  if (raws.length > 0) {
    L.push('## Results', '');
    L.push(
      '| Scenario | Devices in area | Senders | FCM proj. | Seed s | Plan ms | Shards done s | Last iOS s | Last Android s | 99% s | Last push s | Pushes/s | Steady pushes/s per sender | Batch p50/p95 ms | Missed | Dupes | Outside |',
      '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|',
    );
    for (const r of raws) {
      const lastS = r.lastPushMs / 1000;
      const rate = lastS > 0 ? r.totalOk / lastS : 0;
      L.push(
        `| ${r.scenario.name} | ${k(r.eo.expected)} | ${r.scenario.senders} | ${r.scenario.projects} | ${s1(r.seed.seedMs)} | ` +
          `${k(r.planMs)} | ${s1(r.shardPhaseMs)} | ${s1(r.iosLastMs)} | ${s1(r.androidLastMs)} | ${s1(r.t99Ms)} | ${s1(r.lastPushMs)} | ` +
          `${k(rate)} | ${k(r.steadyRate / r.scenario.senders)} | ${r.batchP50}/${r.batchP95} | ${r.eo.missed} | ${r.eo.duplicatePushes} | ${r.eo.outsideArea} |`,
      );
    }
    L.push('');
    L.push(
      'Peak RSS per role (largest single process, MB) and busiest-process CPU (% of one core):',
      '',
    );
    L.push(
      '| Scenario | sender | shard-worker | broker | mock-apns | mock-fcm | parent | all processes | sender CPU | mock CPU | broker CPU | machine CPU |',
      '|---|---|---|---|---|---|---|---|---|---|---|---|',
    );
    for (const r of raws) {
      const p = r.peakRss;
      L.push(
        `| ${r.scenario.name} | ${p['sender'] ?? 0} | ${p['shard-worker'] ?? 0} | ${p['broker'] ?? 0} | ${p['mock-apns'] ?? 0} | ` +
          `${p['mock-fcm'] ?? 0} | ${p['parent'] ?? 0} | ${k(p['total'] ?? 0)} | ${r.cpuPct['sender'] ?? 0}% | ` +
          `${Math.max(r.cpuPct['mock-apns'] ?? 0, r.cpuPct['mock-fcm'] ?? 0)}% | ${r.cpuPct['broker'] ?? 0}% | ` +
          `${r.cpuPct['machine (all processes, % of all cores)'] ?? 0}% |`,
      );
    }
    L.push('');
  }

  L.push(
    'Pushes/s is all accepted pushes over AlertReady to last push. Steady pushes/s per sender counts the pushes between ' +
      '5% and 95% of the total (no ramp-up, no retry tail) divided by the sender count; the sweep sizes Fargate with it.',
    '',
  );
  L.push('## Checks', '');
  for (const sc of result.scenarios) {
    const failed = sc.checks.filter((c) => !c.ok);
    L.push(
      `- **${sc.name}**: ${sc.verdict}` +
        (failed.length
          ? ` (failed: ${failed.map((c) => `${c.name}${c.detail ? ` [${c.detail}]` : ''}`).join('; ')})`
          : ', all checks passed'),
    );
  }
  L.push('');
  const sweep = result.scenarios.find((s) => s.name.startsWith('e) sweep summary'));
  if (sweep) {
    L.push('## Sender scaling and Fargate sizing', '');
    for (const mt of sweep.metrics) L.push(`- ${mt.name}: ${k(mt.value)} ${mt.unit}`);
    for (const n of sweep.notes ?? []) L.push(`- ${n}`);
    L.push('');
  }

  // Docs comparison for the statewide runs.
  const state = raws.filter((r) => r.scenario.region === 'state');
  if (state.length > 0) {
    L.push('## Statewide vs docs/scale-quotas.md', '');
    for (const r of state) {
      const est = r.androidOk / (r.scenario.projects * 9000);
      L.push(
        `- ${r.scenario.name}: last Android push ${s1(r.androidLastMs)} s; the docs' formula (Android recipients / ` +
          `(projects x 9,000/s)) gives ${est.toFixed(1)} s; last iOS push ${s1(r.iosLastMs)} s.`,
      );
    }
    L.push('');
  }

  L.push('## Memory', '');
  const capSum =
    cfg.senders * cfg.heapMb.sender +
    cfg.shardWorkers * cfg.heapMb.shard +
    (cfg.apnsMocks + cfg.fcmMocks) * cfg.heapMb.mock +
    cfg.heapMb.broker +
    cfg.brokerCapMb;
  const observed = Math.max(0, ...raws.map((r) => r.peakRss['total'] ?? 0));
  L.push(
    `- Sum of heap caps this run: ${k(capSum)} MB (an upper bound for the JS heaps; RSS adds roughly 100 MB per process).`,
    `- Observed peak RSS, all processes together: ${k(observed)} MB.`,
    '- Full-scale estimate on the 12-core / 32 GB machine (24 workers: 12 senders, 8 shard workers, 3+3 mocks): ' +
      'senders about 250-400 MB each, shard workers about 300-500 MB, mocks 0.5-1 GB each (they keep one map entry ' +
      'per delivered token: about 150 B for APNs, 250 B for FCM), broker up to 1.5 GB (queue backlog capped at ' +
      `${cfg.scale === 'full' ? cfg.brokerCapMb : 768} MB), parent about 0.5 GB during verification: about 12-15 GB, ` +
      'under the 26 GB of heap caps and well inside 32 GB.',
    '',
  );
  L.push('## Model and caveats', '');
  L.push(
    '- County seeds use the existing Santa Clara generator (seed.ts). The statewide seed is a rough California model: ' +
      '85% of devices around 10 metro areas by approximate population share (LA/Orange 33%, Inland Empire 9%, SF/Oakland ' +
      '10%, San Diego 8.5%, Sacramento 6%, San Jose 5%, Stockton/Modesto 4%, Fresno 3.5%, Ventura/Santa Barbara 3%, ' +
      'Bakersfield 2.5%), 13% uniform over a simplified state outline, and 2% decoys just outside it (Reno, Las Vegas, ' +
      'Tijuana) that must never be pushed. Same platform (55% iOS), language and multi-place shares as seed.ts.',
    '- The alert area is the county bounding box or the state outline (in smoke: a Northern California box).',
    '- The broker holds at most the queue cap in memory; above it shard workers wait (SQS would not). This can stretch ' +
      'the shard phase but never limits the send rate.',
    '- FCM legs are plain HTTP/1.1 (the mock has no TLS); APNs legs use TLS. Real FCM TLS would cost senders some extra CPU.',
    '- One sender process stands for one Fargate task (cpu 1024 = 1 vCPU). Mocks and the system under test share this ' +
      "machine's cores; if a mock is near 100% CPU the rate is a lower bound.",
    '- Finding: the per-project FCM token bucket (FCM_PROJECT_RATE_PER_SEC, default 9,000/s) is per sender task and infra ' +
      'does not set it, so N tasks together may send N x 9,000/s per project against a 10,000/s quota and lean on 429 ' +
      'retries. The default "fleet" mode here divides 9,000/s by the sender count (what the docs assume); ' +
      'T2_FCM_RATE_MODE=per-task reproduces the deployed default.',
    '- Rerun one scenario: T2_SCENARIOS=a|b|c|d|e plus T2_DEVICES_A, T2_DEVICES_B, T2_DEVICES_STATE, T2_DEVICES_SWEEP, ' +
      'T2_SWEEP, T2_SENDERS, T2_SHARD_WORKERS, T2_SWEEP_PROJECTS, T2_FCM_QUOTA, T2_FCM_RATE_MODE, T2_APNS_MOCKS, T2_FCM_MOCKS. ' +
      `Seeds are cached under ${path.join(ctx.dataDir, 't2-seed')}.`,
  );
  if (result.error) L.push('', '## Error', '', '```', result.error, '```');
  writeFileSync(path.join(ctx.outDir, 't2.md'), L.join('\n') + '\n');
}
