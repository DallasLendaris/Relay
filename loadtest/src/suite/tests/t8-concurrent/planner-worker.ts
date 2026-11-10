/**
 * t8/t9 child: the planner. Runs the REAL planShards (eligibility, signature check, shard jobs) for
 * every alert the parent says has arrived, and plays the asynchronous Lambda invocation of shard
 * workers: jobs wait in two lanes (urgent alerts first, like the elastic Lambda fleet that never
 * leaves an emergency's shards behind an advisory's) and go to the shard-worker processes, a
 * few at a time each, retried twice on failure like Lambda does.
 *
 * Control: init, reset, arrive, status, stop.
 */
import { readFileSync } from 'node:fs';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type { ShardJob } from '../../../../../backend/src/scale/contracts.js';
import { CachedAlertReader } from '../../../../../backend/src/scale/fanout/alert-cache.js';
import { planShards } from '../../../../../backend/src/scale/fanout/planner.js';
import type { ShardResult } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import { verifierFromPem } from '../t2-fanout/alert.js';
import { call } from '../t5-chaos/common.js';
import { sleep, runCtlWorker } from './ctl.js';
import { isUrgent } from './alerts.js';

interface InitBody {
  alertsFile: string;
  publicPem: string;
  /** Control URLs of the shard-worker processes. */
  shards: string[];
  jobsPerShard: number;
}

export interface AlertPlanStatus {
  arrivedAt: number;
  /** planShards returned (jobs dispatched or alert refused). */
  planned: boolean;
  planMs: number;
  refused: string | null;
  jobsTotal: number;
  jobsDone: number;
  /** Epoch ms the last shard job of this alert finished (0 until then). */
  shardDoneAt: number;
  cells: number;
  entries: number;
  recipients: number;
  batches: number;
  skippedNoTarget: number;
  skippedNonPrimary: number;
  duplicates: number;
  failedJobs: number;
  refusedJobs: number;
}

const blank = (): AlertPlanStatus => ({
  arrivedAt: 0,
  planned: false,
  planMs: 0,
  refused: null,
  jobsTotal: 0,
  jobsDone: 0,
  shardDoneAt: 0,
  cells: 0,
  entries: 0,
  recipients: 0,
  batches: 0,
  skippedNoTarget: 0,
  skippedNonPrimary: 0,
  duplicates: 0,
  failedJobs: 0,
  refusedJobs: 0,
});

runCtlWorker(async () => {
  let alerts: Alert[] = [];
  let verifyPem = '';
  let shards: Array<{ base: string; inflight: number }> = [];
  let jobsPerShard = 2;
  let reader = new CachedAlertReader(new InMemoryAlertStore());
  let status: AlertPlanStatus[] = [];
  type Lane = Array<{ idx: number; job: ShardJob }>;
  let lanes: { urgent: Lane; standard: Lane } = { urgent: [], standard: [] };
  let jobError: string | undefined;
  const logs = new Map<string, number>();
  const logger: Logger = {
    info: () => undefined,
    warn: (m) => void logs.set(m, (logs.get(m) ?? 0) + 1),
    error: (m) => void logs.set(m, (logs.get(m) ?? 0) + 1),
  };

  const runJob = async (
    w: { base: string; inflight: number },
    idx: number,
    job: ShardJob,
  ): Promise<void> => {
    const st = status[idx] as AlertPlanStatus;
    for (let attempt = 0; ; attempt++) {
      try {
        const r = await call<ShardResult>(w.base, 'POST', '/shard', { job }, 900_000);
        if (r.refused) st.refusedJobs++;
        st.cells += r.cells;
        st.entries += r.entries;
        st.recipients += r.recipients;
        st.batches += r.batches;
        st.skippedNoTarget += r.skippedNoTarget;
        st.skippedNonPrimary += r.skippedNonPrimary;
        st.duplicates += r.duplicates;
        return;
      } catch (err) {
        // Lambda retries a failed asynchronous invoke twice.
        if (attempt >= 2) {
          st.failedJobs++;
          jobError ??= err instanceof Error ? err.message : String(err);
          return;
        }
        await sleep(200);
      }
    }
  };

  const pump = (): void => {
    for (const w of shards) {
      while (w.inflight < jobsPerShard) {
        const next = lanes.urgent.shift() ?? lanes.standard.shift();
        if (!next) return;
        w.inflight++;
        void runJob(w, next.idx, next.job).then(() => {
          w.inflight--;
          const st = status[next.idx] as AlertPlanStatus;
          st.jobsDone++;
          if (st.planned && st.jobsDone >= st.jobsTotal) st.shardDoneAt = Date.now();
          pump();
        });
      }
    }
  };

  const plan = async (idx: number): Promise<void> => {
    const alert = alerts[idx] as Alert;
    const st = status[idx] as AlertPlanStatus;
    const verify = verifierFromPem(verifyPem);
    const t0 = Date.now();
    st.arrivedAt = t0;
    let total = 0;
    try {
      const res = await planShards(
        { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: alert.relaySeverity },
        {
          alerts: reader,
          verifyAlert: (a) => verify(a),
          isDeliveryPaused: async () => false,
          clock: { now: () => new Date() },
          logger,
          shardDispatcher: {
            async dispatch(jobs) {
              total = jobs.length;
              // jobsTotal is set before the first job can finish, so completion counting is exact.
              st.jobsTotal = jobs.length;
              const lane = isUrgent(alert.relaySeverity) ? lanes.urgent : lanes.standard;
              for (const job of jobs) lane.push({ idx, job });
              pump();
            },
          },
        },
      );
      st.planMs = Date.now() - t0;
      st.refused = res.refused ?? null;
      st.planned = true;
      if (total === 0 || res.refused) st.shardDoneAt = Date.now();
      else if (st.jobsDone >= st.jobsTotal) st.shardDoneAt = Date.now();
    } catch (err) {
      jobError ??= `planner failed for alert ${idx}: ${err instanceof Error ? err.message : String(err)}`;
      st.planned = true;
    }
  };

  return {
    handlers: {
      init: async (p: InitBody) => {
        alerts = JSON.parse(readFileSync(p.alertsFile, 'utf8')) as Alert[];
        verifyPem = p.publicPem;
        shards = p.shards.map((base) => ({ base, inflight: 0 }));
        jobsPerShard = p.jobsPerShard;
        return { alerts: alerts.length };
      },
      reset: async () => {
        const store = new InMemoryAlertStore();
        for (const a of alerts) await store.save(a);
        reader = new CachedAlertReader(store);
        status = alerts.map(blank);
        lanes = { urgent: [], standard: [] };
        for (const w of shards) w.inflight = 0;
        jobError = undefined;
        logs.clear();
        return { ok: true };
      },
      arrive: (p: { idx: number }) => {
        void plan(p.idx);
        return { ok: true };
      },
      status: () => {
        const arrived = status.filter((s) => s.arrivedAt > 0).length;
        const idle =
          arrived === alerts.length &&
          status.every((s) => s.planned && s.jobsDone >= s.jobsTotal) &&
          lanes.urgent.length === 0 &&
          lanes.standard.length === 0 &&
          shards.every((w) => w.inflight === 0);
        return {
          idle,
          arrived,
          alerts: status,
          error: jobError ?? null,
          log: Object.fromEntries(logs),
        };
      },
    },
  };
});
