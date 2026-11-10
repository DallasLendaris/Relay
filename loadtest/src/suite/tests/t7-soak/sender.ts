/**
 * t7 child: one sender process. It runs the REAL sender loop (SenderConsumer + processBatch with
 * the real ApnsSender, FcmSender and RoutingPushSender, CachedAlertStore, alert verifier memo,
 * circuit breaker, dead-target queue, kill switch) against stand-ins for AWS that live in the
 * fan-out process: an SQS-like queue, the alert store and the idempotency store, all over HTTP.
 * Pushes go to the mock APNs / FCM processes.
 *
 * Every successful push reports its check-in token to the load driver (sampled), which is how the
 * check-in load gets real tokens. Pushes whose answer was lost (timeout, connection error) are
 * remembered: a duplicate for such a token is legitimate (the sender cannot know).
 *
 * Args: <index>
 */
import { generateKeyPairSync } from 'node:crypto';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
} from '@aws-sdk/client-sqs';
import type { Alert } from '@relay/shared';
import { ApnsSender } from '../../../../../backend/src/adapters/push/apns-sender.js';
import { FcmSender } from '../../../../../backend/src/adapters/push/fcm-sender.js';
import { RoutingPushSender } from '../../../../../backend/src/adapters/push/routing-push-sender.js';
import { createKillSwitch } from '../../../../../backend/src/config/kill-switch.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
} from '../../../../../backend/src/scale/contracts.js';
import { SenderConsumer } from '../../../../../backend/src/scale/sender/consumer.js';
import { CachedAlertStore } from '../../../../../backend/src/scale/sender/cached-alerts.js';
import { SqsSendQueue } from '../../../../../backend/src/scale/sender/sqs-send-queue.js';
import { SENDER_LEASE_MS, processBatch } from '../../../../../backend/src/scale/sender/sender.js';
import type { IdempotencyStore } from '../../../../../backend/src/ports.js';
import { createAlertVerifier } from '../../../../../backend/src/signing/index.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import { alertSigningFromPem } from './keys.js';
import {
  call,
  deferred,
  quietMetricLines,
  fraction,
  readConfig,
  readPorts,
  sizeOf,
  startSampler,
  startServer,
} from './common.js';

const TOKEN_SAMPLE = Number(process.env['T7_TOKEN_SAMPLE'] ?? '0.6');

runWorkerMain(async () => {
  quietMetricLines();
  const index = Number(process.argv[2] ?? 0);
  const cfg = readConfig();
  const ports = readPorts();
  const fanout = ports.fanout ?? 0;
  const load = ports.load ?? 0;
  const { publicKeys } = alertSigningFromPem(process.env['T7_ALERT_KEY'] ?? '');
  const verifyAlert = createAlertVerifier(publicKeys);

  // ----- logger -----
  const logCounts = new Map<string, number>();
  const count = (key: string, fields?: Record<string, unknown>): void => {
    const n = (logCounts.get(key) ?? 0) + 1;
    logCounts.set(key, n);
    // The first time each kind of event happens, say so (names and counts only, never tokens).
    if (n === 1)
      console.error(
        `[t7-sender-${index}] first ${key} ${JSON.stringify(fields ?? {}).slice(0, 200)}`,
      );
  };
  const logger: Logger = {
    info: () => undefined,
    warn: (msg, f) => count(`warn:${msg}`, f),
    error: (msg, f) => count(`error:${msg}`, f),
  };

  // ----- stand-ins for AWS, over HTTP to the fan-out process -----
  const post = async (path: string, body: unknown): Promise<string> => {
    const r = await call(fanout, 'POST', path, JSON.stringify(body));
    if (r.status !== 200) throw new Error(`${path} -> ${r.status}`);
    return r.body;
  };
  const sqs = {
    async send(cmd: unknown): Promise<unknown> {
      if (cmd instanceof ReceiveMessageCommand) {
        const i = cmd.input;
        const body = await post('/q/receive', {
          queue: i.QueueUrl,
          max: i.MaxNumberOfMessages ?? 1,
          wait: i.WaitTimeSeconds ?? 0,
        });
        return { Messages: JSON.parse(body) as unknown[] };
      }
      if (cmd instanceof DeleteMessageBatchCommand) {
        await post('/q/delete', {
          receipts: (cmd.input.Entries ?? []).map((e) => e.ReceiptHandle),
        });
        return { Successful: [], Failed: [] };
      }
      if (cmd instanceof ChangeMessageVisibilityCommand) {
        await post('/q/visibility', {
          receipt: cmd.input.ReceiptHandle,
          seconds: cmd.input.VisibilityTimeout ?? 0,
        });
        return {};
      }
      if (cmd instanceof SendMessageBatchCommand) {
        await post('/q/send', {
          queue: cmd.input.QueueUrl,
          entries: (cmd.input.Entries ?? []).map((e) => ({
            body: e.MessageBody,
            delay: e.DelaySeconds ?? 0,
          })),
        });
        return { Successful: [], Failed: [] };
      }
      throw new Error('unexpected SQS command');
    },
  };
  const idem = async (op: string, key: string, until?: Date): Promise<unknown> =>
    (
      JSON.parse(
        await post(`/idem/${op}`, { key, ...(until ? { until: until.getTime() } : {}) }),
      ) as { v: unknown }
    ).v;
  const idempotency: IdempotencyStore = {
    claim: async (key, until) => (await idem('claim', key, until)) as 'claimed' | 'sent' | 'leased',
    markSent: async (key, expires) => void (await idem('markSent', key, expires)),
    release: async (key) => void (await idem('release', key)),
    extend: async (key, until) => (await idem('extend', key, until)) as boolean,
    peek: async (key) => (await idem('peek', key)) as 'absent' | 'leased' | 'sent',
  };
  const alertStore = {
    async get(id: string): Promise<Alert | null> {
      const r = await call(fanout, 'GET', `/alert/${id}`);
      if (r.status === 404) return null;
      if (r.status !== 200) throw new Error(`alert ${r.status}`);
      return JSON.parse(r.body) as Alert;
    },
  };
  const alerts = new CachedAlertStore(alertStore);

  let killSwitchReads = 0;
  const isPaused = createKillSwitch(
    '/relay/soak/killswitch',
    {
      send: async () => {
        killSwitchReads++;
        return { Parameter: { Value: 'running' } };
      },
    } as never,
    Date.now,
  );

  // ----- push senders against the mocks, wrapped to report successes -----
  const ec = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const rsa = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const apnsKey = ec.privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
  const fcmKey = rsa.privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
  const apns = new ApnsSender({
    privateKeyPem: apnsKey,
    keyId: 'SOAKKEY001',
    teamId: 'SOAKTEAM01',
    bundleId: 'org.relay.soak',
    authority: ports.apnsUrl ?? '',
    sessions: 4,
  });
  const fcm = new FcmSender({
    serviceAccount: { client_email: 'soak@relay-soak.iam', private_key: fcmKey },
    projectId: 'relay-soak',
    tokenUri: `${ports.fcmUrl}/token`,
    baseUrl: ports.fcmUrl ?? '',
  });

  const stats = { pushOk: 0, pushFailed: 0, tokensPosted: 0, tokenPostErrors: 0 };
  const lost = new Set<string>();
  let outbox: Array<[string, string]> = [];
  const flushTokens = async (): Promise<void> => {
    if (outbox.length === 0) return;
    const body = outbox;
    outbox = [];
    try {
      const r = await call(load, 'POST', '/tokens', JSON.stringify(body));
      if (r.status !== 200) stats.tokenPostErrors++;
      else stats.tokensPosted += body.length;
    } catch {
      stats.tokenPostErrors++;
    }
  };
  const flushTimer = setInterval(() => void flushTokens(), 250);
  flushTimer.unref();

  const routing = new RoutingPushSender({ ios: apns, android: fcm, expo: apns });
  const recording: NativePushSender = {
    async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
      const results = await routing.send(messages);
      messages.forEach((m, i) => {
        const r = results[i];
        if (r?.ok) {
          stats.pushOk++;
          const token = m.data['checkInToken'] ?? '';
          if (token && fraction(token) < TOKEN_SAMPLE) {
            outbox.push([m.data['alertId'] ?? '', token]);
          }
        } else if (r) {
          stats.pushFailed++;
          if (r.error.startsWith('Timeout') || r.error.includes('ConnectionError')) {
            lost.add(m.target.token.toLowerCase());
            lost.add(m.target.token);
          }
        }
      });
      return results;
    },
  };

  const sendQueue = new SqsSendQueue(sqs, { urgent: 'urgent', standard: 'standard' });
  const deps = {
    idempotency,
    alerts,
    verifyAlert: (a: Alert) => verifyAlert(a),
    isDeliveryPaused: isPaused,
    push: recording,
    sendQueue,
    onDeadTarget: async () => undefined,
    clock: { now: () => new Date() },
    logger,
    writeMetric: () => undefined,
    invalidateAlert: (id: string) => alerts.invalidate(id),
  };
  const consumer = new SenderConsumer({
    sqs,
    urgentQueueUrl: 'urgent',
    standardQueueUrl: 'standard',
    handle: (batch, control) => processBatch(batch, deps, control),
    extendLease: (batch) =>
      idempotency.extend
        ? idempotency.extend(batch.batchKey, new Date(Date.now() + SENDER_LEASE_MS))
        : Promise.resolve(true),
    isPaused,
    logger,
    concurrency: cfg.scale === 'full' ? 50 : 24,
    writeMetric: () => undefined,
    drainTimeoutMs: 15_000,
  });

  const stop = deferred();
  const sampler = startSampler(`sender-${index}`, cfg.sampleMs, () => ({
    inFlightMessages: consumer.inFlightCount,
    alertCache: sizeOf(alerts, 'cache'),
    lostResponseTokens: lost.size,
    tokenOutbox: outbox.length,
    killSwitchReads,
    logKeys: logCounts.size,
  }));

  const server = await startServer(async (r) => {
    if (r.method === 'GET' && r.path === '/lost') {
      const list = [...lost];
      if (r.query.get('reset') === '1') lost.clear();
      return { body: list };
    }
    if (r.method === 'GET' && r.path === '/stats') {
      return { body: { ...stats, logCounts: Object.fromEntries(logCounts) } };
    }
    if (r.method === 'POST' && r.path === '/stop') {
      stop.resolve();
      return { body: { ok: true } };
    }
    return { status: 404, body: 'not found' };
  });

  await sendMessage({ kind: 'ready', port: server.port });
  const running = consumer.run();
  await stop.promise;
  consumer.stop();
  await running;
  sampler();
  clearInterval(flushTimer);
  await flushTokens();
  apns.close();
  fcm.close();
  server.close();
  return { ...stats, logCounts: Object.fromEntries(logCounts) };
});
