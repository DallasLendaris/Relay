/**
 * Local mode: the real Relay engine and API on one machine, with no AWS.
 *
 * Everything that decides what an alert is, who gets it and what a check-in counts as is the
 * same code the Lambdas run (normalize, H3 cells, lifecycle, signing, planDelivery,
 * deliveryWorker, createApi). Only the adapters change: in-memory stores instead of Aurora,
 * DynamoDB and SQS, an in-memory ECDSA key instead of KMS, and either Expo's real push service
 * or a log of what would have been pushed.
 *
 * Feeds:
 *   live   - polls api.weather.gov (needs internet).
 *   replay - serves a saved NWS response from disk, with the clock set to that response's time,
 *            so the alerts in it are active. Offline. Zone geometry comes from a saved zones file.
 */
import { randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import type { CheckIn } from '@relay/shared';
import type { SQSEvent } from 'aws-lambda';
import { createApi, type ApiEvent, type ApiResult } from '../api/index.js';
import { MemoryCheckInStore } from '../adapters/memory/checkin-store.js';
import { InMemoryAlertStore } from '../adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../adapters/memory/place-store.js';
import { MemoryPushSender } from '../adapters/memory/push-sender.js';
import { MemoryRawArchive } from '../adapters/memory/raw-archive.js';
import { InMemorySeenStore } from '../adapters/memory/seen-store.js';
import { ExpoPushSender } from '../adapters/expo/expo-push-sender.js';
import type { Keyring } from '../config/secrets.js';
import { deliveryWorker, planDelivery } from '../delivery/index.js';
import type { LateJoinQueue } from '../scale/latejoin/intent.js';
import { createIngestPipeline } from '../ingest/pipeline.js';
import { createNwsClient } from '../ingest/nws-client.js';
import { pollOnce } from '../ingest/poll.js';
import type { Logger } from '../delivery/types.js';
import type { Geocoder, PushMessage, PushSender } from '../ports.js';
import { MemoryCellIndex } from '../scale/cell-index/memory-cell-index.js';
import { createAlertVerifier } from '../signing/index.js';
import { createMemoryAlertSigning } from '../signing/memory.js';

export type FeedMode = 'live' | 'replay';
export type PushMode = 'expo' | 'log';

export interface LocalRelayOptions {
  feed: FeedMode;
  push: PushMode;
  /** replay: the saved /alerts/active response to serve. */
  replayFile?: string;
  /** replay: saved zone geometries, `{ "forecast/CAZ513": <NWS zone JSON>, ... }`. Optional. */
  replayZonesFile?: string;
  /** push=expo: optional Expo access token (only needed if the Expo project enforces it). */
  expoAccessToken?: string;
  /** Each pipeline stage is reported here (the server and simulate print it). */
  onStage?: (stage: string, detail: Record<string, unknown>) => void;
  /** Engine logs (JSON lines by default). */
  logger?: Logger;
}

/** What the push sender was asked to send, kept for /local/status and simulate. */
export interface SentPush {
  at: string;
  to: string;
  title: string;
  body: string;
  alertId: string;
  ok: boolean;
  error?: string;
}

const USER_AGENT = '(Relay SJSU capstone local mode, github.com/DallasLendaris/relay)';

/**
 * Latest `sent` in a saved feed: replay mode starts its clock there. That is the moment the
 * feed looked like this, so every alert in it has been issued and the unexpired ones are active.
 */
export function feedStartTime(body: string): Date {
  const parsed = JSON.parse(body) as { features?: Array<{ properties?: { sent?: string } }> };
  const times = (parsed.features ?? [])
    .map((f) => Date.parse(f.properties?.sent ?? ''))
    .filter((t) => Number.isFinite(t));
  if (!times.length) return new Date();
  // One minute after the newest message, so all of them are clearly in effect.
  return new Date(Math.max(...times) + 60_000);
}

/** Check-in store that also keeps per-alert, per-cell totals (the agency rollup). */
class LocalCheckInStore extends MemoryCheckInStore {
  readonly totals = new Map<string, Map<string, number>>();
  override async save(checkIn: CheckIn, retainUntil?: Date): Promise<boolean> {
    const saved = await super.save(checkIn, retainUntil);
    if (saved) {
      const cells = this.totals.get(checkIn.alertId) ?? new Map<string, number>();
      cells.set(checkIn.h3Cell, (cells.get(checkIn.h3Cell) ?? 0) + 1);
      this.totals.set(checkIn.alertId, cells);
    }
    return saved;
  }
}

/** Local mode has no Amazon Location: places are saved by coordinates only. */
const noGeocoder: Geocoder = { geocode: async () => null };

const quietLogger: Logger = { info: () => {}, warn: () => {}, error: () => {} };

export function createLocalRelay(opts: LocalRelayOptions) {
  const stage = opts.onStage ?? (() => {});
  const logger = opts.logger ?? quietLogger;

  // Replay: serve the file, and run the clock from the file's time onward.
  let replayBody: string | undefined;
  let zones: Record<string, unknown> = {};
  let offsetMs = 0;
  if (opts.feed === 'replay') {
    if (!opts.replayFile) throw new Error('replay mode needs a replay file');
    replayBody = readFileSync(opts.replayFile, 'utf8');
    if (opts.replayZonesFile) {
      zones = JSON.parse(readFileSync(opts.replayZonesFile, 'utf8')) as Record<string, unknown>;
    }
    offsetMs = feedStartTime(replayBody).getTime() - Date.now();
  }
  const clock = { now: () => new Date(Date.now() + offsetMs) };

  const replayFetch = (async (input: string | URL | Request) => {
    const url = String(input instanceof Request ? input.url : input);
    if (url.includes('/alerts/active')) {
      return new Response(replayBody, {
        status: 200,
        headers: { 'content-type': 'application/geo+json' },
      });
    }
    // Zones are keyed by their path after /zones/, for example "forecast/CAZ513".
    const key = /\/zones\/([^?]+)/.exec(url)?.[1] ?? '';
    const zone = zones[key];
    if (zone) return new Response(JSON.stringify(zone), { status: 200 });
    return new Response('{}', { status: 404 });
  }) as typeof fetch;
  const nwsFetch = opts.feed === 'replay' ? replayFetch : fetch;

  // Stores. Aurora, DynamoDB and SQS are replaced by their in-memory adapters.
  const alerts = new InMemoryAlertStore();
  const places = new MemoryPlaceStore();
  const devices = new MemoryDeviceStore();
  const checkins = new LocalCheckInStore();
  const cellIndex = new MemoryCellIndex();
  const queue = new MemoryDeliveryQueue();
  const seen = new InMemorySeenStore();
  const idempotency = new MemoryIdempotencyStore(clock);
  const archive = new MemoryRawArchive();

  // Keys are made fresh on every start; nothing secret is read from disk or the environment.
  const signing = createMemoryAlertSigning('local-1');
  const tokenSecret = randomBytes(32).toString('base64url');
  const tokenKeys: Keyring = {
    current: { id: 'local-t1', secret: tokenSecret },
    all: new Map([['local-t1', tokenSecret]]),
  };
  const credentialKey = { id: 'local-c1', secret: randomBytes(32).toString('base64url') };

  // Push: Expo's real service, or a record of what would have been sent.
  const sentLog: SentPush[] = [];
  /** The full messages (with check-in tokens). Used by simulate; never served over HTTP. */
  const sentMessages: PushMessage[] = [];
  const inner: PushSender =
    opts.push === 'expo'
      ? new ExpoPushSender({
          ...(opts.expoAccessToken ? { accessToken: opts.expoAccessToken } : {}),
          logger,
        })
      : new MemoryPushSender();
  const sender: PushSender = {
    async send(messages: PushMessage[]) {
      const results = await inner.send(messages);
      messages.forEach((m, i) => {
        const r = results[i];
        const entry: SentPush = {
          at: clock.now().toISOString(),
          to: m.to,
          title: m.title,
          body: m.body,
          alertId: String(m.data['alertId'] ?? ''),
          ok: r?.ok === true,
          ...(r && !r.ok ? { error: r.error } : {}),
        };
        sentLog.push(entry);
        if (entry.ok) sentMessages.push(m);
        stage(opts.push === 'expo' ? 'push.sent_to_expo' : 'push.logged', {
          title: m.title,
          ok: entry.ok,
          ...(entry.error ? { error: entry.error } : {}),
        });
      });
      return results;
    },
  };

  const engine = createIngestPipeline({
    alertStore: alerts,
    places,
    devices,
    queue,
    alertSigning: signing,
    tokenKeys,
    isDeliveryPaused: async () => false,
    fetch: nwsFetch,
    userAgent: USER_AGENT,
    clock,
    logger,
    env: {}, // in-line planning; no plan queue, zone pack or CDN publish
  });
  const client = createNwsClient({
    fetch: nwsFetch,
    userAgent: USER_AGENT,
    ...(opts.feed === 'replay' ? { now: () => clock.now().getTime() } : {}),
  });

  const verifyAlert = createAlertVerifier(signing.publicKeys);
  let delivered = 0;
  let nextMessage = 0;
  const inflight: Array<{ messageId: string; body: string }> = [];

  /** Runs the delivery worker over every queued job, like SQS would. Returns failed ids. */
  async function drain(): Promise<string[]> {
    for (const job of queue.jobs.slice(delivered)) {
      inflight.push({ messageId: `local-${nextMessage++}`, body: JSON.stringify(job) });
    }
    delivered = queue.jobs.length;
    const batch = [...inflight];
    if (!batch.length) return [];
    stage('delivery.worker', { jobs: batch.length });
    const res = await deliveryWorker({ Records: batch } as unknown as SQSEvent, {
      sender,
      alerts,
      idempotency,
      verifyAlert,
      isDeliveryPaused: async () => false,
      onDeadToken: (token) => devices.clearPushToken(token),
      clock,
      logger,
    });
    const failed = new Set(res.batchItemFailures.map((f) => f.itemIdentifier));
    for (const r of batch) if (!failed.has(r.messageId)) inflight.splice(inflight.indexOf(r), 1);
    return [...failed];
  }

  let lastPoll: { at: string; counts: Record<string, number> } | undefined;

  /** One poll of the feed, then delivery of whatever it planned. */
  async function pollAndDeliver() {
    const queuedBefore = queue.jobs.length;
    stage('ingest.poll', { feed: opts.feed });
    const counts = await pollOnce({
      client,
      archive,
      seen,
      ...engine,
      log: (entry) => logger.info('poll', entry),
    });
    lastPoll = { at: clock.now().toISOString(), counts: { ...counts } };
    stage('ingest.done', { ...counts });
    stage('delivery.planned', { jobs: queue.jobs.length - queuedBefore });
    const failed = await drain();
    return { counts, queued: queue.jobs.length - queuedBefore, failed: failed.length };
  }

  /**
   * Late joiners. A phone that saves a place (or sets its push token) while an alert is already
   * active missed that alert's fan-out. In AWS the API queues an intent for the late-join Lambda;
   * here the same intent re-plans every active alert covering the device's places at once.
   * Re-planning is safe: the worker's claim on alert:severity:device prevents a second push.
   */
  const lateJoinQueue: LateJoinQueue = {
    async enqueue(intent) {
      const cells = (await places.listForDevice(intent.deviceId)).map((p) => p.h3Cell);
      if (!cells.length) return;
      const active = await alerts.listActiveForCells([...new Set(cells)], clock.now());
      if (!active.length) return;
      stage('latejoin.replan', { reason: intent.reason, alerts: active.length });
      for (const alert of active) {
        await planDelivery(alert, {
          places,
          devices,
          queue,
          tokenKey: tokenKeys.current,
          verifyAlert,
          isDeliveryPaused: async () => false,
          clock,
          logger,
        });
      }
      await drain();
    },
  };

  const route = createApi({
    lateJoin: { queue: lateJoinQueue },
    devices,
    places,
    alerts,
    geocoder: noGeocoder,
    checkins,
    cellIndex,
    tokenKeys: tokenKeys.all,
    deviceCredentialKeys: new Map([[credentialKey.id, credentialKey.secret]]),
    deviceCredentialKey: credentialKey,
    clock,
    newId: randomUUID,
  });

  async function handle(event: ApiEvent): Promise<ApiResult> {
    const res = await route(event);
    if (event.rawPath.endsWith('/checkins') && res.statusCode === 200) {
      stage('checkin.counted', { alert: event.rawPath.split('/')[2] });
    }
    return res;
  }

  /** Per-cell "I'm okay" totals for every alert: what the agency map would read. */
  function checkInTotals() {
    return [...checkins.totals].map(([alertId, cells]) => ({
      alertId,
      cells: [...cells].map(([h3Cell, okCount]) => ({ h3Cell, okCount })),
    }));
  }

  return {
    clock,
    stores: { alerts, places, devices, checkins, queue },
    sentLog,
    sentMessages,
    handle,
    pollAndDeliver,
    drain,
    checkInTotals,
    lastPoll: () => lastPoll,
    signing,
  };
}

export type LocalRelay = ReturnType<typeof createLocalRelay>;
