/**
 * Delivery worker Lambda, triggered by the SQS delivery queue. Sends Expo pushes and
 * returns batchItemFailures so SQS retries only the jobs that failed.
 * Dependencies are built on the first invocation and reused while warm.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { ExpoPushSender } from '../adapters/expo/expo-push-sender.js';
import { DynamoIdempotencyStore } from '../adapters/aws/dynamo-idempotency-store.js';
import { PostgresAlertStore } from '../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../adapters/postgres/device-store.js';
import { createKillSwitch } from '../config/kill-switch.js';
import { loadKeyring, loadSecretString } from '../config/secrets.js';
import { createAlertVerifier } from '../signing/index.js';
import { DataApiExecutor } from '../db/sql.js';
import type { Logger } from './types.js';
import { deliveryWorker } from './worker.js';

const logger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

type Deps = Omit<Parameters<typeof deliveryWorker>[1], 'verifyAlert'> & {
  alertKeysArn: string;
};
let deps: Promise<Deps> | undefined;

async function buildDeps(env: Record<string, string | undefined>): Promise<Deps> {
  const {
    DB_CLUSTER_ARN,
    DB_SECRET_ARN,
    DB_NAME,
    EXPO_ACCESS_TOKEN_SECRET_ARN,
    IDEMPOTENCY_TABLE,
    ALERT_KEYS_SECRET_ARN,
    KILL_SWITCH_PARAM,
  } = env;
  if (
    !DB_CLUSTER_ARN ||
    !DB_SECRET_ARN ||
    !DB_NAME ||
    !IDEMPOTENCY_TABLE ||
    !ALERT_KEYS_SECRET_ARN ||
    !KILL_SWITCH_PARAM
  ) {
    throw new Error(
      'DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME, IDEMPOTENCY_TABLE, ALERT_KEYS_SECRET_ARN and KILL_SWITCH_PARAM must be set',
    );
  }
  // The Expo access token is optional. When set, it goes out as a bearer token.
  const accessToken = EXPO_ACCESS_TOKEN_SECRET_ARN
    ? (await loadSecretString(EXPO_ACCESS_TOKEN_SECRET_ARN)).trim()
    : undefined;
  const db = new DataApiExecutor(new RDSDataClient({}), DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME);
  const devices = new PostgresDeviceStore(db);
  return {
    sender: new ExpoPushSender(accessToken ? { accessToken } : {}),
    alerts: new PostgresAlertStore(db),
    idempotency: new DynamoIdempotencyStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      IDEMPOTENCY_TABLE,
    ),
    alertKeysArn: ALERT_KEYS_SECRET_ARN,
    isDeliveryPaused: createKillSwitch(KILL_SWITCH_PARAM),
    onDeadToken: (token) => devices.clearPushToken(token),
    clock: { now: () => new Date() },
    logger,
  };
}

export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  // Clear the cached promise on failure so the next invocation retries the build.
  deps ??= buildDeps(process.env).catch((err: unknown) => {
    deps = undefined;
    throw err;
  });
  const built = await deps;
  // Loaded per invocation (cached 5 minutes inside loadKeyring) so key rotation is picked up.
  const keys = await loadKeyring(built.alertKeysArn);
  return deliveryWorker(event, { ...built, verifyAlert: createAlertVerifier(keys.all) });
};
