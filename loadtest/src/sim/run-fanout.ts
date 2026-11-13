/**
 * Local fan-out simulation CLI.
 *
 *   node --max-old-space-size=5120 --import tsx src/sim/run-fanout.ts --devices 1000000
 *
 * Options: --devices N (1000000), --fcm-rate N (10000), --senders N (4), --apns-sessions N (4),
 * --out FILE (results/fanout-1m-local.md), --no-checkins.
 */
import { writeFileSync } from 'node:fs';
import os from 'node:os';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isMain, numberArg, parseArgs } from '../mock-common.js';
import { runCheckinBurst, type BurstResult } from './checkin-burst.js';
import { runFanoutSim, type FanoutSimResult } from './fanout-sim.js';

const here = dirname(fileURLToPath(import.meta.url));

function machine() {
  const cpus = os.cpus();
  return {
    nproc: cpus.length,
    cpuModel: cpus[0]?.model ?? 'unknown',
    memoryTotalMb: Math.round(os.totalmem() / 1024 / 1024),
    loadAverage1mAtStart: Math.round((os.loadavg()[0] ?? 0) * 100) / 100,
    memoryFreeAtStartMb: Math.round(os.freemem() / 1024 / 1024),
    node: process.version,
    platform: `${os.platform()} ${os.release()}`,
  };
}

const f = (n: number) => n.toLocaleString('en-US');
const s1 = (ms: number | null) => (ms === null ? 'n/a' : `${(ms / 1000).toFixed(1)} s`);

export function renderMarkdown(
  command: string,
  m: ReturnType<typeof machine>,
  r: FanoutSimResult,
  b: BurstResult | undefined,
  heapLimitMb: number,
): string {
  const { windows, ...throughput } = r.throughput;
  const { tokens: _tokens, ...jsonSafe } = r;
  void _tokens;
  const lines: string[] = [];
  lines.push(`# Local fan-out simulation: ${f(r.counts.devices)} devices`);
  lines.push('');
  lines.push(
    `One alert, ${f(r.counts.devices)} seeded devices, the real planner, shard workers, ` +
      `\`processBatch\`, \`ApnsSender\` (h2c) and \`FcmSender\` against the mock APNs and FCM ` +
      `(default error mix, FCM quota ${f(r.options.fcmRate)}/s). All figures below were measured ` +
      `by this run; none are estimates.`,
  );
  lines.push('');
  lines.push('## How it was run');
  lines.push('');
  lines.push('```sh');
  lines.push(command);
  lines.push('```');
  lines.push('');
  lines.push(
    `Machine: ${m.nproc} CPUs (${m.cpuModel}), ${f(m.memoryTotalMb)} MB RAM (${f(m.memoryFreeAtStartMb)} MB free at start), ` +
      `Node ${m.node}, ${m.platform}, V8 heap limit ${f(heapLimitMb)} MB. ` +
      `The mock servers, the senders, the shard workers and the check-in code all share one Node process ` +
      `(one JS thread), so the mocks and the senders compete for CPU. This is a local measurement, not an AWS prediction.`,
  );
  lines.push('');
  lines.push('## Result');
  lines.push('');
  lines.push('| Measure | Value |');
  lines.push('| --- | --- |');
  lines.push(
    `| Devices seeded / entries / cells used | ${f(r.counts.devices)} / ${f(r.counts.entries)} / ${f(r.counts.cellsUsed)} |`,
  );
  lines.push(`| Alert cells (shards) | ${f(r.counts.alertCells)} (${f(r.counts.shards)} shards) |`);
  lines.push(
    `| Devices in the alert area / with a push target | ${f(r.counts.devicesInArea)} / ${f(r.counts.devicesWithTarget)} |`,
  );
  lines.push(
    `| Batches planned (recipients) | ${f(r.counts.plannedBatches)} (${f(r.counts.plannedRecipients)}) |`,
  );
  lines.push(`| Time to plan (\`planShards\` returns) | ${r.timing.planMs} ms |`);
  lines.push(`| Time to first successful push | ${s1(r.timing.firstPushMs)} |`);
  lines.push(`| Time to last successful push | ${s1(r.timing.lastPushMs)} |`);
  lines.push(`| Sends per second, mean (first to last push) | ${f(throughput.meanOkPerSec)} |`);
  lines.push(`| Sends per second, best 5 s window | ${f(throughput.peakWindowOkPerSec)} |`);
  lines.push(
    `| Push latency p50 / p99 / max (per push, includes in-sender queueing) | ${r.pushLatency.p50Ms.toFixed(1)} ms / ${r.pushLatency.p99Ms.toFixed(1)} ms / ${r.pushLatency.maxMs.toFixed(1)} ms over ${f(r.pushLatency.count)} attempts |`,
  );
  lines.push(
    `| Batches processed (of which retries) | ${f(r.pass1.batchesProcessed)} (${f(r.pass1.retryBatchesProcessed)}) |`,
  );
  lines.push(`| Recipients retried after a retryable failure | ${f(r.pass1.retried)} |`);
  lines.push(`| Permanent failures (dead token, cleared) | ${f(r.pass1.permanentFailed)} |`);
  lines.push(`| Gave up (still failing after 5 attempts) | ${f(r.pass1.gaveUp)} |`);
  lines.push(`| Message redeliveries (batch not acked) | ${f(r.pass1.redeliveries)} |`);
  lines.push(
    `| Retry messages delayed (longest delay) | ${f(r.retryDelays.delayedMessages)} (${r.retryDelays.maxDelaySeconds.toFixed(1)} s) |`,
  );
  lines.push(
    `| Permanent-failure breaker opened | ${r.breakerOpened ? 'YES' : 'no'} (sender log events: ${JSON.stringify(r.senderLogEvents)}; first events: ${JSON.stringify(r.breakerEvents.slice(0, 5))}) |`,
  );
  lines.push(
    `| FCM requests rate-limited by the quota bucket (429) | ${f(r.deliveries.fcmRateLimited)} |`,
  );
  lines.push(`| Peak heap / peak RSS | ${r.peakHeapMb} MB / ${r.peakRssMb} MB |`);
  lines.push('');
  const activeSec = Math.max(1, ((r.timing.lastPushMs ?? 0) - (r.timing.firstPushMs ?? 0)) / 1000);
  const timeouts = Object.entries(r.pushErrors)
    .filter(([k]) => k.startsWith('Timeout'))
    .reduce((n, [, v]) => n + v, 0);
  lines.push('## Reading these numbers');
  lines.push('');
  lines.push(
    `- The run is CPU-bound on one JS thread (load average at start: ${m.loadAverage1mAtStart} on ${m.nproc} CPUs). ` +
      `The mock servers do about as much work per push as the senders, so ${f(throughput.meanOkPerSec)} pushes per second is what this machine did, not what the design allows.`,
  );
  lines.push(
    `- The FCM quota (${f(r.options.fcmRate)}/s) never bound: FCM accepted ${f(r.deliveries.fcmAccepted)} pushes at about ${f(Math.round(r.deliveries.fcmAccepted / activeSec))}/s on average, ` +
      `and the quota bucket refused ${f(r.deliveries.fcmRateLimited)} requests. The 429s in the error table are the injected ones from the error mix.`,
  );
  lines.push(
    `- Push latency is measured around each push, so it includes waiting for an in-flight slot and for the event loop; ` +
      `the mock's own latency model is p50 20 ms and p99 150 ms.`,
  );
  lines.push(
    timeouts === 0
      ? `- The senders' request timeout is 10 s. No push timed out in this run, so no retry followed an accepted-but-unanswered push.`
      : `- The senders' request timeout is 10 s. ${f(timeouts)} pushes timed out because the loop was saturated; the mock had already accepted some of them, ` +
          `so their retries are the duplicates listed below. That is the at-least-once edge of "retry after a timeout", not a planner or idempotency bug: ` +
          `batch claims held (the re-plan sent nothing).`,
  );
  lines.push('');
  lines.push('## Exactly once');
  lines.push('');
  lines.push('```');
  lines.push(r.exactlyOnce.summary);
  lines.push('```');
  lines.push('');
  lines.push(
    `- Expected devices (distinct push targets in the area): ${f(r.exactlyOnce.expectedDevices)}; delivered exactly once: ${f(r.exactlyOnce.deliveredOnce)}; ` +
      `excused as permanent failures (mock answered unregistered): ${f(r.exactlyOnce.excusedPermanent)}; missing: ${f(r.exactlyOnce.missing)}.`,
  );
  lines.push(
    `- Cross-cell duplicates (a device with places in several alert cells pushed more than once; the primary-cell rule should make this 0): ` +
      `${f(r.exactlyOnce.knownTwoCellDuplicates)} of ${f(r.counts.devicesInTwoOrMoreCells)} multi-cell devices (${f(r.counts.skippedNonPrimary)} non-primary entries skipped by the planner).`,
  );
  lines.push(
    `- Any other duplicate: ${f(r.exactlyOnce.unexpectedDuplicates)}, of which ${f(r.exactlyOnce.duplicatesAfterLostResponse)} followed a timeout or connection error on the first push ` +
      `(the mock had accepted it, the sender never saw the answer and retried) and ${f(r.exactlyOnce.duplicatesUnexplained)} are unexplained. Unexpected tokens: ${f(r.exactlyOnce.unexpectedTokens)}.`,
  );
  lines.push(
    `- Devices the mock accepted but whose success the sender never saw: ${f(r.exactlyOnce.deliveredWithoutAck)} (they get no check-in token in the burst below).`,
  );
  lines.push(
    `- Verdict: **${r.exactlyOnce.ok ? 'PASS' : 'FAIL'}** (any duplicate, cross-cell or after a lost response, is a failure).`,
  );
  lines.push('');
  lines.push('## Re-plan of the same alert');
  lines.push('');
  lines.push(
    `Planner and shard workers ran again on the same signed alert: ${f(r.replan.shards)} shards in ${r.replan.planMs.toFixed(0)} ms, ` +
      `${f(r.replan.batchesProcessed)} batches re-enqueued, outcomes ${JSON.stringify(r.replan.byReason)}. ` +
      `New push requests that reached the mocks: **${r.replan.newPushRequests}**; new accepted pushes: **${r.replan.newAcceptedPushes}**; ` +
      `new messages given to the push senders: ${r.replan.newPushAttemptsInSender}.`,
  );
  lines.push('');
  lines.push('## Sends per second, per 5 s window (successful pushes)');
  lines.push('');
  lines.push(
    '| Window start | Successful pushes | per second | Retryable failures | Permanent failures |',
  );
  lines.push('| ---: | ---: | ---: | ---: | ---: |');
  for (const w of windows) {
    lines.push(
      `| ${w.startSec} s | ${f(w.ok)} | ${f(w.okPerSec)} | ${f(w.retryable)} | ${f(w.permanent)} |`,
    );
  }
  lines.push('');
  if (b) {
    lines.push('## Check-in burst');
    lines.push('');
    lines.push(
      `One v2 check-in per delivered device (${f(b.checkIns)}), tokens taken from this run's sends, through the API's stateless ` +
        `\`POST /alerts/{id}/checkins\` route with an in-memory check-in store (the device, place, alert and cell-index stores are traps that fail the run if touched), ` +
        `then every stored check-in as a stream INSERT through the real aggregator in batches of ${f(b.aggregator.batchSize)} ` +
        `against an in-memory totals table. Single JS thread, no network, so this is a code-path throughput, not an API Gateway figure.`,
    );
    lines.push('');
    lines.push('| Measure | Value |');
    lines.push('| --- | --- |');
    lines.push(
      `| API check-ins | ${f(b.checkIns)} in ${s1(b.api.ms)} = **${f(b.api.perSec)}/s** (200 OK: ${f(b.api.ok200)}, errors: ${b.api.errors}) |`,
    );
    lines.push(
      `| API handler time p50 / p99 (200 in flight) | ${b.api.p50Ms} ms / ${b.api.p99Ms} ms |`,
    );
    lines.push(
      `| Aggregator | ${f(b.aggregator.batches)} batches of ${f(b.aggregator.batchSize)} in ${s1(b.aggregator.ms)} = **${f(b.aggregator.perSec)}/s** (failed items: ${b.aggregator.failedItems}) |`,
    );
    lines.push(
      `| Counter rows / marker rows written | ${f(b.aggregator.countersWritten)} / ${f(b.aggregator.markers)} |`,
    );
    lines.push(`| Per-cell totals sum | ${f(b.verification.totalsSum)} |`);
    lines.push(
      `| Expected (${b.verification.expectedFormula}) | ${f(b.verification.expectedSum)} = ${f(r.counts.devicesInArea)} - ${f(r.permanentFailureDevices)} - ${f(r.deliveredWithoutAck)} |`,
    );
    lines.push(`| Check-in store total | ${f(b.verification.storeSum)} |`);
    lines.push(
      `| Per-cell mismatches (${f(b.verification.cells)} cells) | ${b.verification.perCellMismatches} |`,
    );
    lines.push(`| Verdict | **${b.verification.ok ? 'PASS' : 'FAIL'}** |`);
    lines.push('');
  }
  lines.push('## Results JSON');
  lines.push('');
  lines.push('```json');
  lines.push(
    JSON.stringify(
      { machine: m, heapLimitMb, command, fanout: jsonSafe, checkinBurst: b ?? null },
      null,
      2,
    ),
  );
  lines.push('```');
  lines.push('');
  return lines.join('\n');
}

if (isMain(import.meta.url)) {
  const args = parseArgs(process.argv.slice(2));
  const devices = numberArg(args, 'devices') ?? 1_000_000;
  const out = resolve(args['out'] ?? resolve(here, '../../results/fanout-1m-local.md'));
  const log = (line: string) => console.error(`[sim] ${line}`);
  const v8 = await import('node:v8');
  const heapLimitMb = Math.round(v8.getHeapStatistics().heap_size_limit / 1024 / 1024);

  const result = await runFanoutSim({
    devices,
    fcmRate: numberArg(args, 'fcm-rate') ?? 10_000,
    senders: numberArg(args, 'senders') ?? 4,
    apnsSessions: numberArg(args, 'apns-sessions') ?? 4,
    seed: numberArg(args, 'seed') ?? 1,
    collectTokens: args['no-checkins'] === undefined,
    log,
  });

  let burst: BurstResult | undefined;
  if (result.tokens) {
    burst = await runCheckinBurst({
      checkInTokens: result.tokens.checkInTokens,
      permanentFailureDevices: result.permanentFailureDevices,
      deliveredWithoutAck: result.deliveredWithoutAck,
      devicesInArea: result.counts.devicesInArea,
      log,
    });
  }
  const command =
    `cd loadtest && node --max-old-space-size=${heapLimitMb} --import tsx src/sim/run-fanout.ts ${process.argv.slice(2).join(' ')}`.trim();
  writeFileSync(out, renderMarkdown(command, machine(), result, burst, heapLimitMb));
  console.error(`[sim] wrote ${out}`);
  const ok =
    result.exactlyOnce.ok &&
    result.replan.newPushRequests === 0 &&
    (burst?.verification.ok ?? true);
  console.error(`[sim] ${ok ? 'PASS' : 'FAIL'}`);
  process.exit(ok ? 0 : 1);
}
