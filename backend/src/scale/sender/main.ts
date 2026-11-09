/**
 * Entry point of the sender service (Fargate container). Builds real dependencies from the
 * environment, runs the consumer, and shuts down cleanly on SIGTERM.
 *
 * Env: URGENT_QUEUE_URL, STANDARD_QUEUE_URL, IDEMPOTENCY_TABLE, CELL_INDEX_TABLE, DB_CLUSTER_ARN,
 * DB_SECRET_ARN, DB_NAME, ALERT_KEYS_SECRET_ARN, KILL_SWITCH_PARAM, APNS_KEY_SECRET_ARN,
 * FCM_SERVICE_ACCOUNT_SECRET_ARN. Optional: SENDER_CONCURRENCY (default 50), PUBLIC_ALERT_BASE_URL
 * (CDN origin; puts data.alertUrl in every push), FCM_PROJECT_IDS
 * (comma-separated Firebase project ids; must match the secret's service accounts in order) and
 * FCM_PROJECT_RATE_PER_SEC (sends per second per Firebase project, default 9000).
 * The FCM secret is a JSON array of service accounts, one per Firebase project (the first is the
 * default project), or one service account object for a single project.
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
  validateApnsSecret,
  validateFcmServiceAccounts,
  FcmProjectSender,
  DEFAULT_FCM_PROJECT_RATE,
  parseFcmProjectIds,
} from '../../adapters/push/index.js';
import { ExpoPushSender } from '../../adapters/expo/expo-push-sender.js';
import { DynamoCellIndex } from '../cell-index/dynamo-cell-index.js';
import { createKillSwitch } from '../../config/kill-switch.js';
import { clearKeyringCache, loadKeyring, loadSecretString } from '../../config/secrets.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { Logger } from '../../delivery/types.js';
import { createAlertVerifier } from '../../signing/index.js';
import { CachedAlertStore } from './cached-alerts.js';
import { SenderConsumer } from './consumer.js';
import { createDeadTargetClearer } from './dead-target.js';
import { DeadTargetQueue } from './dead-target-queue.js';
import { SENDER_LEASE_MS, processBatch, type SenderDeps } from './sender.js';
import { errorFields } from '../../delivery/redact.js';
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

  // Refuse to start with the placeholder secrets CDK creates, or with malformed ones, rather
  // than failing (or, worse, "killing" every token) once pushes start. Errors name fields only.
  const apns = validateApnsSecret(JSON.parse(await loadSecretString(env.APNS_KEY_SECRET_ARN)));
  const fcmAccounts = validateFcmServiceAccounts(
    JSON.parse(await loadSecretString(env.FCM_SERVICE_ACCOUNT_SECRET_ARN)),
  );
  // The configured project list (also given to the API, which assigns devices to projects) must
  // match the secret exactly: a device's token only works with its own project.
  const fcmProjectIds = parseFcmProjectIds(process.env['FCM_PROJECT_IDS']);
  const secretIds = fcmAccounts.map((a) => a.project_id);
  if (fcmProjectIds.length > 0 && fcmProjectIds.join(',') !== secretIds.join(',')) {
    throw new Error('FCM_PROJECT_IDS does not match the project_id list in the FCM secret');
  }
  const fcmRate = Number(process.env['FCM_PROJECT_RATE_PER_SEC'] ?? DEFAULT_FCM_PROJECT_RATE);
  if (!Number.isFinite(fcmRate) || fcmRate <= 0) {
    throw new Error('FCM_PROJECT_RATE_PER_SEC must be a positive number');
  }

  const alerts = new CachedAlertStore(new PostgresAlertStore(db));
  let verify = createAlertVerifier((await loadKeyring(env.ALERT_KEYS_SECRET_ARN)).all);
  const idempotency = new DynamoIdempotencyStore(doc, env.IDEMPOTENCY_TABLE);
  const clearDeadTarget = createDeadTargetClearer({
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    cellIndex: new DynamoCellIndex(doc, env.CELL_INDEX_TABLE),
  });
  // Cleanup runs in the background (4 at a time, 10,000 queued) so it never delays a batch.
  const deadTargets = new DeadTargetQueue({ process: clearDeadTarget, logger });
  const isPaused = createKillSwitch(env.KILL_SWITCH_PARAM);
  const deps: SenderDeps = {
    idempotency,
    alerts,
    // Evaluated per call so a rotated key (refreshKeys) takes effect.
    verifyAlert: (alert: Alert) => verify(alert),
    isDeliveryPaused: isPaused,
    push: new RoutingPushSender({
      ios: new ApnsSender({
        privateKeyPem: apns.p8,
        keyId: apns.keyId,
        teamId: apns.teamId,
        bundleId: apns.bundleId,
        sandbox: apns.sandbox,
      }),
      android: new FcmProjectSender({
        projects: fcmAccounts.map((serviceAccount) => ({
          projectId: serviceAccount.project_id ?? '',
          sender: new FcmSender({ serviceAccount, logger }),
        })),
        ratePerSecond: fcmRate,
        logger,
      }),
      // Development builds only (Expo is limited to about 600 pushes per second per project).
      expo: new ExpoNativeAdapter(new ExpoPushSender()),
    }),
    sendQueue: new SqsSendQueue(sqs, {
      urgent: env.URGENT_QUEUE_URL,
      standard: env.STANDARD_QUEUE_URL,
    }),
    onDeadTarget: clearDeadTarget,
    deadTargets,
    clock: { now: () => new Date() },
    logger,
    ...(process.env['PUBLIC_ALERT_BASE_URL']
      ? { publicAlertBaseUrl: process.env['PUBLIC_ALERT_BASE_URL'] }
      : {}),
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
    handle: (batch, control) => processBatch(batch, deps, control),
    extendLease: (batch) =>
      idempotency.extend(batch.batchKey, new Date(Date.now() + SENDER_LEASE_MS)),
    isPaused,
    logger,
    concurrency: Number(process.env['SENDER_CONCURRENCY'] ?? 50),
  });

  let shutdownAt = 0;
  const shutdown = (signal: string) => {
    logger.info('sender.shutdown', { signal });
    shutdownAt = Date.now();
    consumer.stop();
    // Fargate sends SIGKILL 30 s after SIGTERM. The consumer drains for 25 s (plus 2 s for
    // aborted batches to release), the dead-target queue gets the rest; this is the backstop.
    setTimeout(() => process.exit(1), 29_500).unref();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  logger.info('sender.started', {});
  await consumer.run();
  // Flush pending dead-target cleanups with whatever is left of the 29 s budget.
  const left = Math.max(500, 29_000 - (Date.now() - (shutdownAt || Date.now())));
  await deadTargets.flush(left);
  logger.info('sender.stopped', {});
}

main().then(
  () => process.exit(0),
  (err: unknown) => {
    logger.error('sender.fatal', errorFields(err));
    process.exit(1);
  },
);
