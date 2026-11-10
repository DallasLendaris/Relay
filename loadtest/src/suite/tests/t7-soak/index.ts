/**
 * t7: soak. Runs the long-lived pieces of the system for an hour the way a real day runs, and looks
 * for leaks and unbounded growth. Seven processes, all long-lived:
 *   mock APNs, mock FCM          the existing loadtest mocks
 *   2 x sender                   the real SenderConsumer + processBatch loop against an SQS-like queue
 *   fan-out driver               the real planner + shard workers over the seeded cell index, plus the
 *                                queue, alert store and idempotency store the senders share
 *   API                          the real API handler behind node:http, owning the cell index
 *   load driver                  check-ins, place add/remove, push-token changes, device erasure
 * The parent (this file) runs the alert script, samples every process every 10 s, and judges the run.
 *
 * Full: 60 minutes (RELAY_SOAK_MINUTES overrides), 300,000 devices, an alert event every 4 minutes,
 * 500 check-ins/s, 20 place changes/s, 5 push-token changes/s, 1 erasure/s.
 * Smoke: 60 seconds, 3,000 devices, the same script and rates scaled down.
 */
import { generateKeyPairSync } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { cellsForAlertArea } from '../../../../../backend/src/geo/h3-cells.js';
import { generateEntries } from '../../../seed.js';
import { spawnWorker, type ChildHandle } from '../../lib/child.js';
import { fmt, renderTestReport } from '../../lib/report.js';
import { RssSampler } from '../../lib/rss.js';
import type {
  Metric,
  ScenarioResult,
  SuiteContext,
  SuiteTest,
  TestResult,
  Verdict,
} from '../../types.js';
import {
  firstMiddleLast,
  leakReport,
  loopReport,
  samplesCsv,
  type LeakReport,
  type LoopReport,
} from './analysis.js';
import {
  AREAS,
  addToFingerprint,
  areaPolygon,
  buildEvents,
  callJson,
  emptyFingerprint,
  sleep,
  type AlertEvent,
  type Fingerprint,
  type Sample,
  type T7Config,
  type T7Ports,
} from './common.js';
import type { EpochReply, EpochRequest } from './mock.js';

const here = (name: string): string => fileURLToPath(new URL(`./${name}`, import.meta.url));

// ---------- configuration ----------

export function buildConfig(scale: 'full' | 'smoke', env = process.env): T7Config {
  const full = scale === 'full';
  const override = Number(env['RELAY_SOAK_MINUTES']);
  const minutes = Number.isFinite(override) && override > 0 ? override : full ? 60 : 1;
  const durationS = Math.round(minutes * 60);
  const alertIntervalS = Math.max(full ? 20 : 10, Math.round(durationS / 15));
  const rates = full
    ? { checkins: 500, placeOps: 20, tokenOps: 5, erasures: 1 }
    : { checkins: 50, placeOps: 5, tokenOps: 2, erasures: 1 };
  return {
    scale,
    minutes,
    durationS,
    devices: full ? 300_000 : 3_000,
    seed: 1,
    alertIntervalS,
    alertLifeS: alertIntervalS * 2,
    // Check-in rows are kept for a few minutes after their alert ends (the real default is 30 days).
    retentionDays: (alertIntervalS * (full ? 0.75 : 0.3)) / 86_400,
    rates,
    sampleMs: full ? 10_000 : 2_000,
    eCount: Math.ceil(rates.erasures * durationS) + 10,
    fCount: full ? 200 : 20,
    qCount: full ? 200 : 20,
    fcmRate: 10_000,
  };
}

// ---------- expected recipients of the static devices ----------

/** Fingerprint of the static devices an alert over each area reaches (computed from the seed generator, not from the system). */
async function staticExpectation(
  cfg: T7Config,
  areaSets: Array<Set<string>>,
): Promise<Fingerprint[]> {
  const out = areaSets.map(() => emptyFingerprint());
  let curId = '';
  let curToken = '';
  let mask = 0;
  const flush = (): void => {
    if (!curId) return;
    areaSets.forEach((_s, k) => {
      if (mask & (1 << k)) addToFingerprint(out[k] as Fingerprint, curToken);
    });
  };
  let n = 0;
  for (const e of generateEntries({ devices: cfg.devices, seed: cfg.seed, deviceIds: 'uuid' })) {
    if (e.deviceId !== curId) {
      flush();
      curId = e.deviceId;
      curToken = e.target?.token ?? '';
      mask = 0;
    }
    areaSets.forEach((s, k) => {
      if (s.has(e.h3Cell)) mask |= 1 << k;
    });
    if (++n % 20_000 === 0) await sleep(0);
  }
  flush();
  return out;
}

// ---------- processes ----------

interface Proc {
  name: string;
  handle: ChildHandle;
  ready: Promise<Record<string, unknown>>;
  ctl: number;
}

interface LoadState {
  now: number;
  devices: Array<{
    token: string;
    cell: string;
    kind: 'E' | 'F';
    erase: { start: number; end?: number } | null;
  }>;
}

interface EventOutcome {
  index: number;
  kind: string;
  area: string;
  pushes: boolean;
  ok: boolean;
  expected: number;
  delivered: number;
  unregistered: number;
  windowDelivered: number;
  duplicates: number;
  unexplainedDuplicates: number;
  forbiddenHits: number;
  drainedS: number;
  late: boolean;
  detail: string;
}

const sumFp = (a: Fingerprint, b: Fingerprint): Fingerprint => ({
  count: a.count + b.count,
  h1: a.h1 + b.h1,
  h2: a.h2 + b.h2,
});

export const t7Soak: SuiteTest = {
  id: 't7',
  title: 'Soak: leaks and unbounded growth over a long run',
  description:
    'Mock APNs and FCM, 2 senders, the fan-out driver, the API and a load driver as separate long-lived processes for 60 minutes: an alert event every 4 minutes under steady check-ins, place changes, token changes and erasures; heap after GC, event-loop delay and cache sizes sampled every 10 s.',
  estimatedMinutes: 66,
  async run(ctx: SuiteContext): Promise<TestResult> {
    const startedAt = new Date().toISOString();
    const wall0 = Date.now();
    const cfg = buildConfig(ctx.scale);
    const full = ctx.scale === 'full';
    ctx.log(
      `soak: ${fmt(cfg.minutes, 1)} min, ${fmt(cfg.devices)} static devices, an alert event every ${cfg.alertIntervalS} s`,
    );

    const samples: Sample[] = [];
    const procs: Proc[] = [];
    const rss = new RssSampler();
    let crashed: string | null = null;
    const alertKey = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }).privateKey.export({
      type: 'pkcs8',
      format: 'pem',
    }) as string;
    const ports: T7Ports = {};

    const launch = (
      name: string,
      file: string,
      args: string[],
      heapMb: number,
      env: Record<string, string> = {},
    ): Proc => {
      const handle = spawnWorker(here(file), args, {
        heapMb,
        nodeArgs: ['--expose-gc'],
        env: {
          T7_CONFIG: JSON.stringify(cfg),
          T7_PORTS: JSON.stringify(ports),
          T7_ALERT_KEY: alertKey,
          ...env,
        },
      });
      let resolveReady!: (v: Record<string, unknown>) => void;
      const ready = new Promise<Record<string, unknown>>((r) => {
        resolveReady = r;
      });
      handle.onMessage((m) => {
        const msg = m as { kind?: string };
        if (msg.kind === 'sample') samples.push(m as Sample);
        else if (msg.kind === 'ready') resolveReady(m as Record<string, unknown>);
      });
      handle.exited.then((code) => {
        if (!stopping) crashed = `${name} exited unexpectedly (code ${code})`;
      });
      rss.addSource(() => handle.rssNow);
      const p: Proc = { name, handle, ready, ctl: 0 };
      procs.push(p);
      return p;
    };
    let stopping = false;
    const check = (): void => {
      if (crashed) throw new Error(crashed);
    };
    const sleepUntil = async (t: number): Promise<void> => {
      while (Date.now() < t) {
        check();
        await sleep(Math.min(1000, t - Date.now()));
      }
    };

    const events: AlertEvent[] = [];
    const outcomes: EventOutcome[] = [];
    let error: string | undefined;
    let tStart = 0;
    const notes: string[] = [];
    let apiStats: Record<string, unknown> = {};
    let loadStats: Record<string, unknown> = {};
    let senderStats: Array<Record<string, unknown>> = [];
    let mockTotals: Array<Record<string, number>> = [];
    let fanoutResult: Record<string, unknown> = {};
    let lostCumulative = 0;
    let drainFailures = 0;

    try {
      // ----- start everything -----
      const apnsP = launch('mock-apns', 'mock.ts', ['apns'], 512);
      const fcmP = launch('mock-fcm', 'mock.ts', ['fcm'], 512);
      const apiP = launch('api', 'api.ts', [], full ? 3072 : 512);
      // The expected recipients of the static devices come from the seed generator, in the parent,
      // while the API process seeds the real cell index.
      const areaCells: string[][] = [];
      for (const a of AREAS) {
        areaCells.push(
          await cellsForAlertArea(
            { geometry: areaPolygon(a), ugcCodes: [] },
            {
              getZoneGeometry: () => {
                throw new Error('zone lookup not expected');
              },
            },
          ),
        );
      }
      const areaSets = areaCells.map((c) => new Set(c));
      const staticExpected = await staticExpectation(cfg, areaSets);
      ctx.log(
        `expected recipients of the static devices per area: ${AREAS.map((a, i) => `${a.name} ${fmt(staticExpected[i]?.count ?? 0)}`).join(', ')}`,
      );

      const [apnsR, fcmR, apiR] = await Promise.all([apnsP.ready, fcmP.ready, apiP.ready]);
      apnsP.ctl = Number(apnsR['ctl']);
      fcmP.ctl = Number(fcmR['ctl']);
      ports.apnsUrl = String(apnsR['url']);
      ports.fcmUrl = String(fcmR['url']);
      ports.apiPublic = Number(apiR['pub']);
      ports.apiCtl = Number(apiR['ctl']);
      apiP.ctl = ports.apiCtl;
      ctx.log(`API up, cell index holds ${fmt(Number(apiR['entries']))} entries`);

      const loadP = launch('load', 'load.ts', [], full ? 1024 : 384);
      loadP.ctl = Number((await loadP.ready)['port']);
      ports.load = loadP.ctl;
      const fanoutP = launch('fanout', 'fanout.ts', [], full ? 2048 : 512);
      fanoutP.ctl = Number((await fanoutP.ready)['port']);
      ports.fanout = fanoutP.ctl;
      const senders: Proc[] = [];
      for (let i = 0; i < 2; i++) {
        const s = launch(`sender-${i}`, 'sender.ts', [String(i)], full ? 1024 : 384);
        senders.push(s);
      }
      for (const s of senders) s.ctl = Number((await s.ready)['port']);
      check();

      // ----- the managed devices (erasure pool, flappers, quiet devices) -----
      ctx.log(`creating ${cfg.eCount + cfg.fCount + cfg.qCount} API devices (places, push tokens)`);
      await callJson(
        loadP.ctl,
        'POST',
        '/setup',
        { e: cfg.eCount, f: cfg.fCount, q: cfg.qCount },
        600_000,
      );

      // ----- the script -----
      const offsetS = full ? 20 : 3;
      const lastS = cfg.durationS - cfg.alertIntervalS * 0.5;
      const count = Math.max(1, Math.floor((lastS - offsetS) / cfg.alertIntervalS) + 1);
      events.push(...buildEvents(count));
      ctx.log(`${count} alert events: ${events.map((e) => e.kind).join(', ')}`);

      tStart = Date.now();
      await callJson(loadP.ctl, 'POST', '/start', {});
      const repushed = new Set<string>();

      for (const ev of events) {
        const due = tStart + (offsetS + ev.index * cfg.alertIntervalS) * 1000;
        await sleepUntil(due);
        check();
        const late = Date.now() - due > 5000;
        if (ev.kind === 'update_upgrade' && !repushed.has(ev.alertId)) {
          // Everyone gets a second push for this alert; those tokens must not be counted as new check-ins.
          repushed.add(ev.alertId);
          await callJson(loadP.ctl, 'POST', '/repush', { alertId: ev.alertId, on: true });
        }
        const t0 = Date.now();
        const published = await callJson<{ refused: string | null }>(
          fanoutP.ctl,
          'POST',
          '/alert',
          ev,
        );
        if (published.refused && ev.pushes) {
          notes.push(
            `event ${ev.index} (${ev.kind}) was refused by the planner: ${published.refused}`,
          );
        }
        // Drain: the fan-out has finished and the queue (including delayed retries) is empty.
        const drainTimeoutMs = Math.max(60_000, cfg.alertIntervalS * 3000);
        let drained = false;
        let lastStatusLog = Date.now();
        while (Date.now() - t0 < drainTimeoutMs) {
          check();
          const s = await callJson<{
            shardsRunning: boolean;
            shardError: string | null;
            queued: number;
          }>(fanoutP.ctl, 'GET', '/status');
          if (s.shardError) throw new Error(`fan-out failed: ${s.shardError}`);
          if (!s.shardsRunning && s.queued === 0) {
            await sleep(400);
            const again = await callJson<{ shardsRunning: boolean; queued: number }>(
              fanoutP.ctl,
              'GET',
              '/status',
            );
            if (!again.shardsRunning && again.queued === 0) {
              drained = true;
              break;
            }
          }
          if (Date.now() - lastStatusLog > 15_000) {
            lastStatusLog = Date.now();
            ctx.log(
              `  waiting for event ${ev.index} to drain: ${JSON.stringify(s)} (${fmt((Date.now() - t0) / 1000)} s)`,
            );
          }
          await sleep(250);
        }
        const t1 = Date.now();
        if (!drained) drainFailures++;

        // ----- judge this event -----
        const state = await callJson<LoadState>(loadP.ctl, 'GET', '/state');
        const areaSet = areaSets[ev.areaIdx] ?? new Set<string>();
        const must = emptyFingerprint();
        const exclude: string[] = [];
        const forbid: string[] = [];
        for (const d of state.devices) {
          const e = d.erase;
          if (d.kind === 'E' && e && e.end !== undefined && e.end < t0) {
            forbid.push(d.token);
            continue;
          }
          if (!areaSet.has(d.cell)) continue;
          if (d.kind === 'E' && e && !(e.start > t1)) {
            exclude.push(d.token); // erased while the alert was going out: either outcome is right
            continue;
          }
          if (ev.pushes) addToFingerprint(must, d.token);
        }
        const req: EpochRequest = { exclude, forbid };
        const replies = await Promise.all(
          [apnsP, fcmP].map((p) => callJson<EpochReply>(p.ctl, 'POST', '/epoch', req, 300_000)),
        );
        const lostLists = await Promise.all(
          senders.map((s) => callJson<string[]>(s.ctl, 'GET', '/lost?reset=1')),
        );
        const lost = new Set(lostLists.flat());
        lostCumulative += lost.size;

        const exp = ev.pushes
          ? sumFp(staticExpected[ev.areaIdx] ?? emptyFingerprint(), must)
          : emptyFingerprint();
        let got = emptyFingerprint();
        let delivered = 0;
        let unreg = 0;
        let windowDelivered = 0;
        let dupCount = 0;
        let unexplained = 0;
        let forbiddenHits = 0;
        for (const r of replies) {
          got = sumFp(got, sumFp(r.delivered, r.unregistered));
          delivered += r.delivered.count;
          unreg += r.unregistered.count;
          dupCount += r.duplicateCount;
          forbiddenHits += r.forbiddenHits;
          for (const t of r.duplicateTokens) if (!lost.has(t)) unexplained++;
          if (r.duplicateCount > r.duplicateTokens.length)
            unexplained += r.duplicateCount - r.duplicateTokens.length;
          for (const [t, n] of Object.entries(r.excluded)) {
            windowDelivered += n;
            if (n > 1 && !lost.has(t)) unexplained++;
          }
        }
        const setOk = got.count === exp.count && got.h1 === exp.h1 && got.h2 === exp.h2;
        const ok = drained && setOk && unexplained === 0 && forbiddenHits === 0;
        const out: EventOutcome = {
          index: ev.index,
          kind: ev.kind,
          area: AREAS[ev.areaIdx]?.name ?? '?',
          pushes: ev.pushes,
          ok,
          expected: exp.count,
          delivered,
          unregistered: unreg,
          windowDelivered,
          duplicates: dupCount,
          unexplainedDuplicates: unexplained,
          forbiddenHits,
          drainedS: (t1 - t0) / 1000,
          late,
          detail: [
            drained ? '' : 'not drained in time',
            setOk
              ? ''
              : `recipient set differs: expected ${exp.count}, delivered+dead ${got.count}`,
            unexplained ? `${unexplained} duplicate pushes` : '',
            forbiddenHits ? `${forbiddenHits} pushes to erased devices` : '',
          ]
            .filter(Boolean)
            .join(', '),
        };
        outcomes.push(out);
        ctx.log(
          `event ${ev.index} ${ev.kind} (${out.area}): ${out.ok ? 'ok' : 'FAILED'}, ${fmt(delivered)} pushes (+${fmt(unreg)} dead tokens) for ${fmt(exp.count)} expected, drained in ${fmt(out.drainedS, 1)} s${late ? ', started late' : ''}`,
        );
      }

      // ----- run out the clock, then stop the workload and settle -----
      await sleepUntil(tStart + cfg.durationS * 1000);
      await callJson(loadP.ctl, 'POST', '/stop-workload', {}, 120_000);
      for (let i = 0; i < 60; i++) {
        const s = await callJson<{ shardsRunning: boolean; queued: number }>(
          fanoutP.ctl,
          'GET',
          '/status',
        );
        if (!s.shardsRunning && s.queued === 0) break;
        await sleep(500);
      }
      await sleep(cfg.sampleMs + 500); // one last sample of every process at rest

      apiStats = await callJson<Record<string, unknown>>(apiP.ctl, 'GET', '/stats');
      loadStats = await callJson<Record<string, unknown>>(loadP.ctl, 'GET', '/stats');
      senderStats = await Promise.all(
        senders.map((s) => callJson<Record<string, unknown>>(s.ctl, 'GET', '/stats')),
      );
      mockTotals = await Promise.all(
        [apnsP, fcmP].map((p) => callJson<Record<string, number>>(p.ctl, 'GET', '/totals')),
      );

      // ----- shut everything down (senders first, so nothing is left half-sent) -----
      stopping = true;
      const finish = (p: Proc): Promise<unknown> =>
        Promise.race([
          p.handle.result,
          sleep(30_000).then(() => Promise.reject(new Error(`${p.name}: stop timeout`))),
        ]);
      const stopProc = async (p: Proc): Promise<void> => {
        try {
          await callJson(p.ctl, 'POST', '/stop', {}, 30_000);
        } catch {
          p.handle.kill();
        }
      };
      // Senders first and to the end of their drain, so nothing is cut off mid-send.
      await Promise.all(senders.map((p) => stopProc(p)));
      await Promise.allSettled(senders.map(finish));
      for (const p of [loadP, fanoutP, apiP, apnsP, fcmP]) await stopProc(p);
      const results = await Promise.allSettled(procs.map(finish));
      const fi = procs.indexOf(fanoutP);
      const fr = results[fi];
      if (fr?.status === 'fulfilled') fanoutResult = fr.value as Record<string, unknown>;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      ctx.log(`soak failed: ${error}`);
    } finally {
      stopping = true;
      for (const p of procs) p.handle.kill();
    }
    const peakRssMb = Math.max(rss.stop(), ...procs.map((p) => Math.round(p.handle.rssPeak)));

    // ---------- analysis ----------
    const durationS = cfg.durationS;
    const sampleS = cfg.sampleMs / 1000;
    const enforce = durationS >= 20 * 60;
    const markS = enforce ? 15 * 60 : durationS * 0.25;
    const names = [...new Set(samples.map((s) => s.proc))].sort();
    const run = tStart > 0 ? samples.filter((s) => s.t >= tStart) : samples;
    const leaks: LeakReport[] = [];
    const loops: LoopReport[] = [];
    for (const n of names) {
      const rows = run.filter((s) => s.proc === n);
      leaks.push(leakReport(n, rows, tStart, { durationS, markS, limitPct: 20, floorMb: 8 }));
      loops.push(loopReport(n, rows, 200, sampleS));
    }
    const leakers = leaks.filter((l) => l.leak);
    const stalled = loops.filter((l) => l.overLimitS > 60);

    const csvPath = join(ctx.outDir, 't7-samples.csv');
    try {
      writeFileSync(csvPath, samplesCsv(samples, tStart || (samples[0]?.t ?? 0)));
    } catch (e) {
      notes.push(`could not write ${csvPath}: ${e instanceof Error ? e.message : String(e)}`);
    }

    // ----- numbers -----
    type Counters = Record<string, number>;
    const lc = (loadStats['counters'] ?? {}) as Counters;
    const accepted = (loadStats['acceptedByAlert'] ?? {}) as Record<string, number>;
    const totals = (apiStats['totalsByAlert'] ?? {}) as Record<string, number>;
    const alertIds = [...new Set([...Object.keys(accepted), ...Object.keys(totals)])];
    let acceptedSum = 0;
    let storedSum = 0;
    let worstGap = 0;
    let overStored = 0;
    for (const id of alertIds) {
      const a = accepted[id] ?? 0;
      const s = totals[id] ?? 0;
      acceptedSum += a;
      storedSum += s;
      worstGap = Math.max(worstGap, a - s);
      if (s > a) overStored += s - a;
    }
    const checkInsExact = overStored === 0 && worstGap <= lostCumulative && acceptedSum > 0;
    const pushes = mockTotals.reduce((n, t) => n + (t['ok'] ?? 0), 0);
    const attempts = mockTotals.reduce((n, t) => n + (t['received'] ?? 0), 0);
    const dead = mockTotals.reduce((n, t) => n + (t['unregistered'] ?? 0), 0);
    const apiServerErrors = Number(apiStats['serverErrors'] ?? 0);
    const senderLogs = senderStats.map((s) => (s['logCounts'] ?? {}) as Counters);
    const senderFailures = senderLogs.reduce(
      (n, l) =>
        n +
        (l['error:sender.message_failed'] ?? 0) +
        (l['error:sender.batch_failed'] ?? 0) +
        (l['error:sender.chunk_threw'] ?? 0),
      0,
    );
    const errors = (lc['errors'] ?? 0) + apiServerErrors + senderFailures + drainFailures;
    const pushingEvents = outcomes.filter((o) => o.pushes).length;
    const eventsOk = outcomes.length === events.length && outcomes.every((o) => o.ok);
    const forbiddenTotal = outcomes.reduce((n, o) => n + o.forbiddenHits, 0);
    const erasures = lc['erasures'] ?? 0;
    const lat = (loadStats['latencyMs'] ?? {}) as Record<
      string,
      { p50: number; p99: number; count: number }
    >;
    const failedEvents = outcomes.filter((o) => !o.ok);

    const metrics: Metric[] = [
      { name: 'run length', value: durationS / 60, unit: 'min' },
      { name: 'alert events', value: outcomes.length, unit: 'events' },
      { name: 'alerts sent (events that pushed)', value: pushingEvents, unit: 'alerts' },
      { name: 'pushes accepted by the mocks', value: pushes, unit: 'pushes' },
      { name: 'dead-token answers (unregistered)', value: dead, unit: 'pushes' },
      { name: 'push attempts at the mocks', value: attempts, unit: 'requests' },
      { name: 'check-ins accepted', value: acceptedSum, unit: 'check-ins' },
      { name: 'check-ins sent (incl. retries)', value: lc['checkinsSent'] ?? 0, unit: 'requests' },
      { name: 'place adds', value: lc['placeAdds'] ?? 0, unit: 'requests' },
      { name: 'place removes', value: lc['placeRemoves'] ?? 0, unit: 'requests' },
      {
        name: 'push-token changes',
        value: (lc['tokenSame'] ?? 0) + (lc['tokenRotate'] ?? 0),
        unit: 'requests',
      },
      { name: 'device erasures', value: erasures, unit: 'devices' },
      { name: 'check-in latency p99 (client)', value: lat['checkin']?.p99 ?? 0, unit: 'ms' },
      { name: 'errors', value: errors, unit: 'errors', target: { op: '==', value: 0 } },
    ];
    for (const l of leaks) {
      metrics.push({
        name: `heap slope ${l.proc}`,
        value: Math.round(l.slopeMbPerHour * 10) / 10,
        unit: 'MB/h',
      });
    }
    for (const l of loops) {
      metrics.push({ name: `max loop delay p99 ${l.proc}`, value: l.maxP99Ms, unit: 'ms' });
    }

    const longRun = enforce
      ? ''
      : ` (not enforced: the run is shorter than 20 minutes; measured from ${fmt(markS)} s)`;
    const checks: ScenarioResult['checks'] = [
      {
        name: 'every alert reached every device in its area exactly once',
        ok: eventsOk,
        detail: eventsOk
          ? `${outcomes.length} events, ${fmt(pushes)} pushes`
          : failedEvents.map((o) => `event ${o.index} ${o.kind}: ${o.detail}`).join('; ') ||
            `only ${outcomes.length} of ${events.length} events ran`,
      },
      {
        name: 'repeat updates and cancels sent nothing',
        ok:
          outcomes
            .filter((o) => !o.pushes)
            .every((o) => o.delivered === 0 && o.unregistered === 0 && o.windowDelivered === 0) &&
          outcomes.some((o) => !o.pushes),
        detail: `${outcomes.filter((o) => !o.pushes).length} such events`,
      },
      {
        name: 'erased devices received no further pushes',
        ok: forbiddenTotal === 0 && erasures > 0,
        detail: `${fmt(erasures)} devices erased, ${forbiddenTotal} pushes to an erased device`,
      },
      {
        name: 'check-in totals exact at the end',
        ok: checkInsExact,
        detail: `API counted ${fmt(storedSum)}, driver saw ${fmt(acceptedSum)} accepted (gap ${worstGap}, allowed ${lostCumulative} lost-answer duplicates)`,
      },
      {
        name: 'no unexpected errors (API 5xx, failed requests, sender failures, undrained alerts)',
        ok: errors === 0,
        detail: `${errors}${((loadStats['errorSamples'] ?? []) as string[]).length ? `: ${((loadStats['errorSamples'] ?? []) as string[]).slice(0, 3).join(' | ')}` : ''}`,
      },
      {
        name: `heap after GC grows less than 20% from the ${fmt(markS / 60, 1)}-minute mark to the end${longRun}`,
        ok: !enforce || leakers.length === 0,
        detail: leaks.map((l) => `${l.proc} ${fmt(l.growthPct, 1)}%`).join(', '),
      },
      {
        name: `event-loop delay p99 over 200 ms for no more than 60 s${enforce ? '' : ' (not enforced in a short run)'}`,
        ok: !enforce || stalled.length === 0,
        detail:
          loops
            .filter((l) => l.overLimitS > 0)
            .map((l) => `${l.proc} ${l.overLimitS} s`)
            .join(', ') || 'never over',
      },
    ];
    if (error) checks.push({ name: 'run completed', ok: false, detail: error });

    const ok = checks.every((c) => c.ok);
    const verdict: Verdict = ok ? 'pass' : 'fail';
    const scenario: ScenarioResult = {
      name: `Soak, ${fmt(cfg.minutes, 1)} minutes`,
      verdict,
      durationMs: Date.now() - wall0,
      metrics,
      checks,
      notes: [
        ...notes,
        `Samples: ${csvPath} (${samples.length} rows: RSS, heapUsed after GC, event-loop delay p99, cache sizes per process).`,
        `Leak rule: fail when a process's heap after GC grows more than 20% (and more than 8 MB) between the ${fmt(markS / 60, 1)}-minute mark and the end (least-squares fit); the MB/h slope uses the second half of the run.`,
        ...(enforce
          ? []
          : [
              'Short run: memory and event-loop verdicts are reported but not enforced; use at least 20 minutes (RELAY_SOAK_MINUTES).',
            ]),
        'Stand-ins that would otherwise grow without bound (check-in rows, idempotency claims) honour their TTLs, like DynamoDB; tombstones of erased devices grow by design (one per erasure).',
      ],
    };

    const result: TestResult = {
      id: 't7',
      title: t7Soak.title,
      verdict,
      startedAt,
      durationMs: Date.now() - wall0,
      scenarios: [scenario],
      peakRssMb,
      ...(error ? { error } : {}),
    };

    // ---------- report ----------
    const md: string[] = [
      renderTestReport(
        ctx,
        result,
        `Seven long-lived processes for ${fmt(cfg.minutes, 1)} minutes: ${fmt(cfg.devices)} static devices, an alert event every ${cfg.alertIntervalS} s (new, update, upgrade, cancel; rotating areas), ${cfg.rates.checkins} check-ins/s, ${cfg.rates.placeOps} place changes/s, ${cfg.rates.tokenOps} push-token changes/s, ${cfg.rates.erasures} erasure/s.`,
      ),
    ];
    md.push('## Memory and event loop per process', '');
    md.push(
      '| Process | RSS MB (first / middle / last) | Heap after GC MB (first / middle / last) | Heap slope MB/h | Growth from mark | Loop delay p99, max (ms) | Seconds over 200 ms |',
      '| --- | --- | --- | ---: | ---: | ---: | ---: |',
    );
    for (const n of names) {
      const rows = run.filter((s) => s.proc === n);
      const [r1, r2, r3] = firstMiddleLast(rows.map((r) => r.rssMb));
      const [h1, h2, h3] = firstMiddleLast(rows.map((r) => r.heapMb));
      const l = leaks.find((x) => x.proc === n);
      const p = loops.find((x) => x.proc === n);
      md.push(
        `| ${n} | ${fmt(r1)} / ${fmt(r2)} / ${fmt(r3)} | ${fmt(h1)} / ${fmt(h2)} / ${fmt(h3)} | ${fmt(l?.slopeMbPerHour ?? 0, 1)} | ${fmt(l?.growthPct ?? 0, 1)}% | ${fmt(p?.maxP99Ms ?? 0, 1)} | ${p?.overLimitS ?? 0} |`,
      );
    }
    md.push(
      '',
      '## Cache and queue sizes (first / middle / last)',
      '',
      '| Process | Size | First | Middle | Last |',
      '| --- | --- | ---: | ---: | ---: |',
    );
    for (const n of names) {
      const rows = run.filter((s) => s.proc === n);
      const keys = [...new Set(rows.flatMap((r) => Object.keys(r.caches)))].sort();
      for (const k of keys) {
        const [a, b, c] = firstMiddleLast(rows.map((r) => r.caches[k] ?? 0));
        md.push(`| ${n} | ${k} | ${fmt(a)} | ${fmt(b)} | ${fmt(c)} |`);
      }
    }
    md.push(
      '',
      '## Alert events',
      '',
      '| # | Event | Area | Expected | Pushed | Dead tokens | Erased mid-flight | Drain s | Result |',
      '| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |',
    );
    for (const o of outcomes) {
      md.push(
        `| ${o.index} | ${o.kind} | ${o.area} | ${fmt(o.expected)} | ${fmt(o.delivered)} | ${fmt(o.unregistered)} | ${fmt(o.windowDelivered)} | ${fmt(o.drainedS, 1)} | ${o.ok ? 'ok' : `FAILED: ${o.detail}`} |`,
      );
    }
    const fo = fanoutResult as {
      queue?: Record<string, number>;
      shardTotals?: Record<string, number>;
    };
    md.push('', '## Other numbers', '');
    md.push(
      `- API requests: ${fmt(Number(apiStats['requests'] ?? 0))}, 5xx: ${apiServerErrors}. By route: \`${JSON.stringify(apiStats['byRoute'] ?? {})}\`.`,
    );
    md.push(
      `- Latency (client side, ms p50 / p99): check-in ${lat['checkin']?.p50 ?? 0} / ${lat['checkin']?.p99 ?? 0}, place add ${lat['placeAdd']?.p50 ?? 0} / ${lat['placeAdd']?.p99 ?? 0}, place remove ${lat['placeRemove']?.p50 ?? 0} / ${lat['placeRemove']?.p99 ?? 0}, push token ${lat['token']?.p50 ?? 0} / ${lat['token']?.p99 ?? 0}, erasure ${lat['erase']?.p50 ?? 0} / ${lat['erase']?.p99 ?? 0}.`,
    );
    md.push(`- Load driver: ${JSON.stringify(lc)}.`);
    md.push(
      `- Send queue: ${JSON.stringify(fo.queue ?? {})}; shards ${JSON.stringify(fo.shardTotals ?? {})}.`,
    );
    md.push(`- Sender log events: ${JSON.stringify(senderLogs)}.`);
    md.push('');
    try {
      writeFileSync(join(ctx.outDir, 't7.md'), md.join('\n'));
    } catch {
      /* the result still goes back to the runner */
    }
    return result;
  },
};
