/**
 * Entry point of the sender service (Fargate container). Builds real dependencies from the
 * environment, runs the consumer, and shuts down cleanly on SIGTERM.
 *
 * Env: URGENT_QUEUE_URL, STANDARD_QUEUE_URL, IDEMPOTENCY_TABLE, CELL_INDEX_TABLE, DB_CLUSTER_ARN,
 * DB_SECRET_ARN, DB_NAME, ALERT_KEYS_SECRET_ARN, KILL_SWITCH_PARAM, APNS_KEY_SECRET_ARN,
 * FCM_SERVICE_ACCOUNT_SECRET_ARN. Optional: SENDER_CONCURRENCY (default 50).
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { Alert } from '@relay/shared';
import { DynamoIdempotencyStore } from '../../adapters/aws/dynamo-idempotency-store.js';
import { PostgresAlertStore } from '../../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../adapters/postgres/place-store.js';
import {
  ApnsSender,
  ExpoNativeAdapter,
  FcmSender,
  RoutingPushSender,
  type ServiceAccount,
} from '../../adapters/push/index.js';
import { ExpoPushSender } from '../../adapters/expo/expo-push-sender.js';
import { createKillSwitch } from '../../config/kill-switch.js';
import { clearKeyringCache, loadKeyring, loadSecretString } from '../../config/secrets.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { Logger } from '../../delivery/types.js';
import { createAlertVerifier } from '../../signing/index.js';
import { CachedAlertStore } from './cached-alerts.js';
import { SenderConsumer } from './consumer.js';
import { createDeadTargetClearer } from './dead-target.js';
import { processBatch, type SenderDeps } from './sender.js';
import { SqsSendQueue } from './sqs-send-queue.js';

const logger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

const REQUIRED = [
  'URGENT_QUEUE_URL',
  'STANDARD_QUEUE_URL',
  'IDEMPOTENCY_TABLE',
  'CELL_INDEX_TABLE',
  'DB_CLUSTER_ARN',
  'DB_SECRET_ARN',
  'DB_NAME',
  'ALERT_KEYS_SECRET_ARN',
  'KILL_SWITCH_PARAM',
  'APNS_KEY_SECRET_ARN',
  'FCM_SERVICE_ACCOUNT_SECRET_ARN',
] as const;

function readEnv(
  env: Record<string, string | undefined>,
): Record<(typeof REQUIRED)[number], string> {
  const missing = REQUIRED.filter((k) => !env[k]);
  if (missing.length) throw new Error(`Missing environment variables: ${missing.join(', ')}`);
  return Object.fromEntries(REQUIRED.map((k) => [k, env[k] as string])) as Record<
    (typeof REQUIRED)[number],
    string
  >;
}

async function main(): Promise<void> {
  const env = readEnv(process.env);
  const sqs = new SQSClient({});
  const doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));
  const db = new DataApiExecutor(
    new RDSDataClient({}),
    env.DB_CLUSTER_ARN,
    env.DB_SECRET_ARN,
    env.DB_NAME,
  );

  const apns = JSON.parse(await loadSecretString(env.APNS_KEY_SECRET_ARN)) as {
    keyId: string;
    teamId: string;
    bundleId: string;
    p8: string;
    sandbox: boolean;
  };
  const fcm = JSON.parse(
    await loadSecretString(env.FCM_SERVICE_ACCOUNT_SECRET_ARN),
  ) as ServiceAccount;
  // Refuse to start with the placeholder secrets CDK creates, rather than failing every push.
  if (apns.p8.startsWith('REPLACE_ME') || String(fcm.private_key ?? '').startsWith('REPLACE_ME')) {
    throw new Error('APNs/FCM secrets not configured (placeholder values)');
  }

  const alerts = new CachedAlertStore(new PostgresAlertStore(db));
  let verify = createAlertVerifier((await loadKeyring(env.ALERT_KEYS_SECRET_ARN)).all);
  const deps: SenderDeps = {
    idempotency: new DynamoIdempotencyStore(doc, env.IDEMPOTENCY_TABLE),
    alerts,
    // Evaluated per call so a rotated key (refreshKeys) takes effect.
    verifyAlert: (alert: Alert) => verify(alert),
    isDeliveryPaused: createKillSwitch(env.KILL_SWITCH_PARAM),
    push: new RoutingPushSender({
      ios: new ApnsSender({
        privateKeyPem: apns.p8,
        keyId: apns.keyId,
        teamId: apns.teamId,
        bundleId: apns.bundleId,
        sandbox: apns.sandbox,
      }),
      android: new FcmSender({ serviceAccount: fcm }),
      // Development builds only (Expo is limited to about 600 pushes per second per project).
      expo: new ExpoNativeAdapter(new ExpoPushSender()),
    }),
    sendQueue: new SqsSendQueue(sqs, {
      urgent: env.URGENT_QUEUE_URL,
      standard: env.STANDARD_QUEUE_URL,
    }),
    onDeadTarget: createDeadTargetClearer({
      devices: new PostgresDeviceStore(db),
      places: new PostgresPlaceStore(db),
      doc,
      cellIndexTable: env.CELL_INDEX_TABLE,
    }),
    clock: { now: () => new Date() },
    logger,
    invalidateAlert: (id) => alerts.invalidate(id),
    refreshKeys: async () => {
      clearKeyringCache();
      verify = createAlertVerifier((await loadKeyring(env.ALERT_KEYS_SECRET_ARN)).all);
    },
  };

  const consumer = new SenderConsumer({
    sqs,
    urgentQueueUrl: env.URGENT_QUEUE_URL,
    standardQueueUrl: env.STANDARD_QUEUE_URL,
    handle: (batch) => processBatch(batch, deps),
    logger,
    concurrency: Number(process.env['SENDER_CONCURRENCY'] ?? 50),
  });

  const shutdown = (signal: string) => {
    logger.info('sender.shutdown', { signal });
    consumer.stop();
    // Fargate sends SIGKILL 30 s after SIGTERM. The consumer drains for 25 s; this is the backstop.
    setTimeout(() => process.exit(1), 29_000).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  logger.info('sender.started', {});
  await consumer.run();
  logger.info('sender.stopped', {});
}

main().then(
  () => process.exit(0),
  (err: unknown) => {
    logger.error('sender.fatal', { error: err instanceof Error ? err.message : String(err) });
    process.exit(1);
  },
);
