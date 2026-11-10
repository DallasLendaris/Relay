/**
 * t9 child: the late-join consumer (stands in for the late-join Lambda and its SQS event source).
 * It polls the API's intent queue and runs the REAL `buildLateJoinHandler` (processLateJoin: primary
 * cell rule, planned-fan-out skip, per-target claim, rate limit, check-in token, SendBatch on the
 * urgent or standard queue). The device, its places and its push-target times come from the API
 * process (one call per intent, like one Aurora read); the claims and queues are the broker's, the
 * same ones the planned fan-out and the senders use.
 *
 * A record the handler reports as failed goes back for another try after half a second, like SQS
 * visibility; after ten tries it counts as dead-lettered.
 *
 * Control: init, start, status, finish, stop.
 */
import { readFileSync } from 'node:fs';
import type { SQSEvent } from 'aws-lambda';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type { Device, Place } from '../../../../../backend/src/ports.js';
import type { PushTargetMeta } from '../../../../../backend/src/scale/latejoin/push-target-rules.js';
import type { LateJoinIntent } from '../../../../../backend/src/scale/latejoin/intent.js';
import { buildLateJoinHandler } from '../../../../../backend/src/scale/latejoin/handler.js';
import type { LateJoinDeps } from '../../../../../backend/src/scale/latejoin/latejoin.js';
import { SqsSendQueue } from '../../../../../backend/src/scale/sender/sqs-send-queue.js';
import { createCheckInToken } from '../../../../../backend/src/tokens/checkInToken.js';
import { TOKEN_KEY, verifierFromPem } from '../t2-fanout/alert.js';
import { BrokerClient, QUEUE_URLS, RemoteIdempotencyStore, fakeSqs } from '../t2-fanout/wire.js';
import { ctlCall, runCtlWorker, sleep } from '../t8-concurrent/ctl.js';
import { quantile } from '../t8-concurrent/ledger.js';

interface Context {
  device: Device | null;
  places: Place[];
  meta: { updatedAt: string; changedAt: string } | null;
}

export interface LateJoinFinal {
  polled: number;
  processed: number;
  retried: number;
  deadLettered: number;
  /** Time an intent waited in the queue before it was picked up, ms. */
  waitP50: number;
  waitP99: number;
  waitMax: number;
  /** Time from pick-up to the handler finishing (reads, claims, enqueue), ms. */
  handleP50: number;
  handleP99: number;
  logEvents: Record<string, number>;
}

const MAX_TRIES = 10;

runCtlWorker(async () => {
  let broker: BrokerClient | undefined;
  let api = '';
  let handler:
    | ((e: SQSEvent) => Promise<{ batchItemFailures: Array<{ itemIdentifier: string }> }>)
    | undefined;
  let running = false;
  let loops: Array<Promise<void>> = [];
  let polled = 0;
  let processed = 0;
  let retried = 0;
  let dead = 0;
  let inflight = 0;
  let retryWaiting = 0;
  let apiWaiting = 0;
  const waits: number[] = [];
  const handles: number[] = [];
  const logEvents = new Map<string, number>();
  const count = (msg: string): void => void logEvents.set(msg, (logEvents.get(msg) ?? 0) + 1);
  const logger: Logger = {
    info: count,
    warn: count,
    error: count,
  };
  // One read per intent: get() always fetches the device with its places and times (it is the first
  // call processLateJoin makes), listForDevice and pushTargetMeta read what get() just fetched.
  const contexts = new Map<string, { at: number; value: Promise<Context> }>();
  const fetchContext = (deviceId: string, fresh: boolean): Promise<Context> => {
    const hit = contexts.get(deviceId);
    if (!fresh && hit) return hit.value;
    const value = ctlCall<Context>(api, '/lj_context', { deviceId }, 60_000);
    contexts.set(deviceId, { at: Date.now(), value });
    return value;
  };

  const sweep = setInterval(() => {
    const cutoff = Date.now() - 2_000;
    for (const [k, v] of contexts) if (v.at < cutoff) contexts.delete(k);
  }, 1_000);
  sweep.unref();

  const loop = async (batchSize: number): Promise<void> => {
    const retries: Array<{
      at: number;
      due: number;
      intent: LateJoinIntent;
      tries: number;
      id: string;
    }> = [];
    let seq = 0;
    while (running || retries.length > 0) {
      let records: Array<{ at: number; intent: LateJoinIntent; tries: number; id: string }> = [];
      const now = Date.now();
      const due = retries.filter((r) => r.due <= now).slice(0, batchSize);
      for (const r of due) retries.splice(retries.indexOf(r), 1);
      records = due;
      if (running && records.length < batchSize) {
        const r = await ctlCall<{
          items: Array<{ intent: LateJoinIntent; at: number }>;
          waiting: number;
        }>(api, '/lj_poll', { max: batchSize - records.length }, 60_000);
        apiWaiting = r.waiting;
        polled += r.items.length;
        for (const it of r.items) {
          waits.push(Date.now() - it.at);
          records.push({ at: it.at, intent: it.intent, tries: 0, id: `m${process.pid}-${seq++}` });
        }
      }
      retryWaiting = retries.length;
      if (records.length === 0) {
        await sleep(20);
        continue;
      }
      inflight += records.length;
      const t0 = Date.now();
      const event = {
        Records: records.map((r) => ({
          messageId: r.id,
          body: JSON.stringify(r.intent),
          attributes: { SentTimestamp: String(r.at) },
        })),
      } as unknown as SQSEvent;
      let failed = new Set<string>();
      try {
        const res = await (handler as NonNullable<typeof handler>)(event);
        failed = new Set(res.batchItemFailures.map((f) => f.itemIdentifier));
      } catch {
        failed = new Set(records.map((r) => r.id));
      }
      const took = Date.now() - t0;
      for (const r of records) {
        handles.push(took / records.length);
        if (!failed.has(r.id)) {
          processed++;
          continue;
        }
        if (r.tries + 1 >= MAX_TRIES) {
          dead++;
          processed++;
          continue;
        }
        retried++;
        retries.push({ ...r, tries: r.tries + 1, due: Date.now() + 500 });
      }
      inflight -= records.length;
      retryWaiting = retries.length;
    }
  };

  return {
    handlers: {
      init: async (p: {
        apiCtl: string;
        brokerPort: number;
        publicPem: string;
        alertsFile: string;
      }) => {
        api = p.apiCtl;
        broker = await BrokerClient.connect(p.brokerPort);
        const store = new InMemoryAlertStore();
        for (const a of JSON.parse(readFileSync(p.alertsFile, 'utf8')) as Alert[])
          await store.save(a);
        const verify = verifierFromPem(p.publicPem);
        const deps: LateJoinDeps = {
          devices: {
            get: async (id) => (await fetchContext(id, true)).device,
            pushTargetMeta: async (id): Promise<PushTargetMeta | null> => {
              const m = (await fetchContext(id, false)).meta;
              return m
                ? { updatedAt: new Date(m.updatedAt), changedAt: new Date(m.changedAt) }
                : null;
            },
          },
          places: { listForDevice: async (id) => (await fetchContext(id, false)).places },
          alerts: store,
          verifyAlert: (a) => verify(a),
          idempotency: new RemoteIdempotencyStore(broker),
          sendQueue: new SqsSendQueue(fakeSqs(broker, 'sender'), QUEUE_URLS),
          makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) =>
            createCheckInToken({ alertId, deviceId, expiresAt, h3Cell }, TOKEN_KEY),
          isDeliveryPaused: async () => false,
          clock: { now: () => new Date() },
          logger,
        };
        handler = buildLateJoinHandler(deps);
        return { ok: true };
      },
      start: (p: { concurrency: number; batchSize: number }) => {
        if (running) throw new Error('already running');
        running = true;
        loops = Array.from({ length: p.concurrency }, () => loop(p.batchSize));
        return { ok: true };
      },
      status: () => ({ polled, processed, inflight, retryWaiting, apiWaiting, dead }),
      finish: async (): Promise<LateJoinFinal> => {
        running = false;
        await Promise.all(loops);
        return {
          polled,
          processed,
          retried,
          deadLettered: dead,
          waitP50: quantile(waits, 0.5),
          waitP99: quantile(waits, 0.99),
          waitMax: waits.reduce((m, v) => Math.max(m, v), 0),
          handleP50: quantile(handles, 0.5),
          handleP99: quantile(handles, 0.99),
          logEvents: Object.fromEntries(logEvents),
        };
      },
    },
    onStop: () => {
      clearInterval(sweep);
      broker?.close();
    },
  };
});
