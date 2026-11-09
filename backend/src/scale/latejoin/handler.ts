/**
 * Late-join Lambda: the ONLY consumer of the late-join queue, and (with the shard workers and the
 * sender) one of the few identities allowed to send to the urgent and standard send queues.
 *
 * Env: DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME, URGENT_QUEUE_URL, STANDARD_QUEUE_URL,
 * IDEMPOTENCY_TABLE, ALERT_VERIFY_KEYS, TOKEN_KEYS_SECRET_ARN, KILL_SWITCH_PARAM.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { DynamoIdempotencyStore } from '../../adapters/aws/dynamo-idempotency-store.js';
import { PostgresAlertStore } from '../../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../../adapters/postgres/place-store.js';
import { createKillSwitch } from '../../config/kill-switch.js';
import { clearKeyringCache, loadKeyring } from '../../config/secrets.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { Logger } from '../../delivery/types.js';
import { alertVerifierFromEnv } from '../../signing/kms.js';
import { createCheckInToken } from '../../tokens/index.js';
import { SqsSendQueue } from '../sender/sqs-send-queue.js';
import { isLateJoinIntent } from './intent.js';
import { processLateJoin, type LateJoinDeps, type LateJoinResult } from './latejoin.js';

const logger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

/**
 * Runs one intent. An alert whose signature fails may only be signed with a key rotated in after
 * our keyring cache filled, so refresh the keys once and run again (claims make the rerun safe).
 */
export async function runIntent(
  intent: { deviceId: string },
  deps: LateJoinDeps,
  refresh?: () => Promise<LateJoinDeps>,
): Promise<LateJoinResult> {
  let result = await processLateJoin(intent, deps);
  if (result.unsigned > 0 && refresh) result = await processLateJoin(intent, await refresh());
  return result;
}

/**
 * Per record: a body that is not an intent goes back to the queue (and the DLQ after the retry
 * limit, where the alarm sees it); a failure retries; a paused kill switch retries.
 */
export const buildLateJoinHandler =
  (deps: LateJoinDeps, refresh?: () => Promise<LateJoinDeps>) =>
  async (event: SQSEvent): Promise<SQSBatchResponse> => {
    const batchItemFailures: SQSBatchResponse['batchItemFailures'] = [];
    for (const record of event.Records) {
      try {
        const body: unknown = JSON.parse(record.body);
        if (!isLateJoinIntent(body)) throw new Error('message is not a late-join intent');
        // Only the device id is used; see processLateJoin.
        const result = await runIntent({ deviceId: body.deviceId }, deps, refresh);
        if (result.refused === 'paused')
          batchItemFailures.push({ itemIdentifier: record.messageId });
      } catch (err) {
        deps.logger.error('latejoin.record_failed', {
          messageId: record.messageId,
          error: err instanceof Error ? err.name : 'UnknownError',
        });
        batchItemFailures.push({ itemIdentifier: record.messageId });
      }
    }
    return { batchItemFailures };
  };

const LATEJOIN_ENV = [
  'DB_CLUSTER_ARN',
  'DB_SECRET_ARN',
  'DB_NAME',
  'URGENT_QUEUE_URL',
  'STANDARD_QUEUE_URL',
  'IDEMPOTENCY_TABLE',
  'ALERT_VERIFY_KEYS',
  'TOKEN_KEYS_SECRET_ARN',
  'KILL_SWITCH_PARAM',
] as const;

function build(env: Record<string, string | undefined>) {
  const missing = LATEJOIN_ENV.filter((n) => !env[n]);
  if (missing.length > 0) throw new Error(`Missing environment variables: ${missing.join(', ')}`);
  const e = Object.fromEntries(LATEJOIN_ENV.map((n) => [n, env[n] as string])) as Record<
    (typeof LATEJOIN_ENV)[number],
    string
  >;
  const db = new DataApiExecutor(
    new RDSDataClient({}),
    e.DB_CLUSTER_ARN,
    e.DB_SECRET_ARN,
    e.DB_NAME,
  );
  const base = {
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    alerts: new PostgresAlertStore(db),
    idempotency: new DynamoIdempotencyStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      e.IDEMPOTENCY_TABLE,
    ),
    sendQueue: new SqsSendQueue(new SQSClient({}), {
      urgent: e.URGENT_QUEUE_URL,
      standard: e.STANDARD_QUEUE_URL,
    }),
    isDeliveryPaused: createKillSwitch(e.KILL_SWITCH_PARAM),
    clock: { now: () => new Date() },
    logger,
  };
  const withKeys = async (): Promise<LateJoinDeps> => {
    const [verifyAlert, tokenKeys] = await Promise.all([
      alertVerifierFromEnv(env),
      loadKeyring(e.TOKEN_KEYS_SECRET_ARN),
    ]);
    return {
      ...base,
      verifyAlert,
      makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) =>
        createCheckInToken({ alertId, deviceId, h3Cell, expiresAt }, tokenKeys.current),
    };
  };
  return async (event: SQSEvent) =>
    buildLateJoinHandler(await withKeys(), async () => {
      clearKeyringCache();
      return withKeys();
    })(event);
}

// Clients are built once per container; a failed build is not cached, so the next call retries.
let built: Promise<(event: SQSEvent) => Promise<SQSBatchResponse>> | undefined;
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  built ??= Promise.resolve().then(() => build(process.env));
  built.catch(() => (built = undefined));
  return (await built)(event);
};
