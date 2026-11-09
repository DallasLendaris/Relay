/**
 * Lambda entry points for the fan-out: plannerHandler (plan queue) and shardHandler (shard
 * queue). Both return batchItemFailures. Dependencies are built from env on the first
 * invocation and reused while warm. The testable cores take their dependencies as arguments.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { LambdaClient } from '@aws-sdk/client-lambda';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { PostgresAlertStore } from '../../adapters/postgres/alert-store.js';
import { createKillSwitch } from '../../config/kill-switch.js';
import { clearKeyringCache, loadKeyring } from '../../config/secrets.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { Logger } from '../../delivery/types.js';
import { createAlertVerifier } from '../../signing/index.js';
import { createCheckInToken } from '../../tokens/index.js';
import type { ShardJob } from '../contracts.js';
import type { FanoutRefusal } from './common.js';
import { isAlertReady, isShardJob } from './messages.js';
import { DynamoCellReader } from './dynamo-cell-reader.js';
import { planShards, type PlannerDeps } from './planner.js';
import {
  LambdaShardDispatcher,
  SqsSendQueue,
  SqsShardDispatcher,
  type ShardDispatcher,
} from './queues.js';
import { processShard, type ShardWorkerDeps } from './shard-worker.js';

const logger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

/**
 * Runs `handle` for each SQS record and reports the ones to retry. A record is retried when
 * its body cannot be parsed (it ends in the DLQ, where the alarm sees it), when handling
 * throws, or when the result is 'paused' (the kill switch is not final: try again later).
 * Other refusals are final and the record is dropped after logging.
 */
export async function runRecords<T>(
  event: SQSEvent,
  isValid: (body: unknown) => body is T,
  handle: (body: T) => Promise<{ refused?: FanoutRefusal }>,
  log: Logger = logger,
): Promise<SQSBatchResponse> {
  const batchItemFailures: SQSBatchResponse['batchItemFailures'] = [];
  for (const record of event.Records) {
    try {
      const body: unknown = JSON.parse(record.body);
      if (!isValid(body)) throw new Error('message has the wrong shape');
      const result = await handle(body);
      if (result.refused === 'paused') batchItemFailures.push({ itemIdentifier: record.messageId });
    } catch (err) {
      log.error('fan-out record failed', { messageId: record.messageId, error: String(err) });
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }
  return { batchItemFailures };
}

export const buildPlannerHandler =
  (deps: PlannerDeps, refreshKeys?: () => Promise<PlannerDeps>) =>
  (event: SQSEvent): Promise<SQSBatchResponse> =>
    runRecords(event, isAlertReady, async (ready) => {
      const result = await planShards(ready, deps);
      if (result.refused !== 'unsigned' || !refreshKeys) return result;
      // A signature failure may just mean a key was rotated in after our cache filled.
      return planShards(ready, await refreshKeys());
    });

/**
 * The shard worker takes a ShardJob two ways:
 *  - directly, from an async Lambda invoke (the default): the event IS the job. Failures throw,
 *    so Lambda's async retry (2 retries, then the on-failure DLQ) takes over. 'paused' throws too.
 *  - from SQS (SHARD_DISPATCH=sqs): returns batchItemFailures.
 */
export const buildShardHandler =
  (deps: ShardWorkerDeps, refreshKeys?: () => Promise<ShardWorkerDeps>) =>
  async (event: SQSEvent | ShardJob): Promise<SQSBatchResponse | undefined> => {
    const run = async (job: ShardJob) => {
      const result = await processShard(job, deps);
      if (result.refused !== 'unsigned' || !refreshKeys) return result;
      return processShard(job, await refreshKeys());
    };
    if ('Records' in event) return runRecords(event, isShardJob, run);
    if (!isShardJob(event)) throw new Error('shard event is not a ShardJob');
    const result = await run(event);
    if (result.refused === 'paused') {
      throw new Error(`delivery paused; shard ${event.shardId} will retry`);
    }
    return undefined;
  };

// ---------- Real AWS wiring ----------

function requireEnv<K extends string>(
  env: Record<string, string | undefined>,
  names: readonly K[],
): Record<K, string> {
  const missing = names.filter((n) => !env[n]);
  if (missing.length > 0) throw new Error(`Missing environment variables: ${missing.join(', ')}`);
  return Object.fromEntries(names.map((n) => [n, env[n]])) as Record<K, string>;
}

const clock = { now: () => new Date() };

function alertStore(env: { DB_CLUSTER_ARN: string; DB_SECRET_ARN: string; DB_NAME: string }) {
  const db = new DataApiExecutor(
    new RDSDataClient({}),
    env.DB_CLUSTER_ARN,
    env.DB_SECRET_ARN,
    env.DB_NAME,
  );
  return new PostgresAlertStore(db);
}

/** SHARD_DISPATCH is 'lambda' (default; needs SHARD_FUNCTION_NAME) or 'sqs' (needs SHARD_QUEUE_URL). */
const PLANNER_ENV = [
  'DB_CLUSTER_ARN',
  'DB_SECRET_ARN',
  'DB_NAME',
  'ALERT_KEYS_SECRET_ARN',
  'KILL_SWITCH_PARAM',
] as const;

const SHARD_ENV = [
  'DB_CLUSTER_ARN',
  'DB_SECRET_ARN',
  'DB_NAME',
  'URGENT_QUEUE_URL',
  'STANDARD_QUEUE_URL',
  'CELL_INDEX_TABLE',
  'ALERT_KEYS_SECRET_ARN',
  'TOKEN_KEYS_SECRET_ARN',
  'KILL_SWITCH_PARAM',
] as const;

let planner: Promise<(event: SQSEvent) => Promise<SQSBatchResponse>> | undefined;
let shard:
  Promise<(event: SQSEvent | ShardJob) => Promise<SQSBatchResponse | undefined>> | undefined;

/** Lambda invoke by default; SQS when SHARD_DISPATCH=sqs. */
export function shardDispatcherFromEnv(env: Record<string, string | undefined>): ShardDispatcher {
  if (env.SHARD_DISPATCH === 'sqs') {
    const { SHARD_QUEUE_URL } = requireEnv(env, ['SHARD_QUEUE_URL']);
    return new SqsShardDispatcher(new SQSClient({}), SHARD_QUEUE_URL);
  }
  const { SHARD_FUNCTION_NAME } = requireEnv(env, ['SHARD_FUNCTION_NAME']);
  return new LambdaShardDispatcher(new LambdaClient({}), SHARD_FUNCTION_NAME, 50);
}

function buildPlanner(env: Record<string, string | undefined>) {
  const e = requireEnv(env, PLANNER_ENV);
  const base = {
    alerts: alertStore(e),
    shardDispatcher: shardDispatcherFromEnv(env),
    isDeliveryPaused: createKillSwitch(e.KILL_SWITCH_PARAM),
    clock,
    logger,
  };
  const withKeys = async (): Promise<PlannerDeps> => ({
    ...base,
    verifyAlert: createAlertVerifier((await loadKeyring(e.ALERT_KEYS_SECRET_ARN)).all),
  });
  // Keys are read per invocation (loadKeyring caches for 5 minutes), so rotation is picked up.
  return async (event: SQSEvent) =>
    buildPlannerHandler(await withKeys(), async () => {
      clearKeyringCache();
      return withKeys();
    })(event);
}

function buildShard(env: Record<string, string | undefined>) {
  const e = requireEnv(env, SHARD_ENV);
  const base = {
    alerts: alertStore(e),
    cellIndex: new DynamoCellReader(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      e.CELL_INDEX_TABLE,
    ),
    sendQueue: new SqsSendQueue(new SQSClient({}), {
      urgent: e.URGENT_QUEUE_URL,
      standard: e.STANDARD_QUEUE_URL,
    }),
    isDeliveryPaused: createKillSwitch(e.KILL_SWITCH_PARAM),
    clock,
    logger,
  };
  const withKeys = async (): Promise<ShardWorkerDeps> => {
    const [alertKeys, tokenKeys] = await Promise.all([
      loadKeyring(e.ALERT_KEYS_SECRET_ARN),
      loadKeyring(e.TOKEN_KEYS_SECRET_ARN),
    ]);
    return {
      ...base,
      verifyAlert: createAlertVerifier(alertKeys.all),
      makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) => {
        // Token v2 binds the cell in. Passed as a variable so this compiles both before and
        // after createCheckInToken gains the h3Cell field.
        const params = { alertId, deviceId, h3Cell, expiresAt };
        return createCheckInToken(params, tokenKeys.current);
      },
    };
  };
  return async (event: SQSEvent | ShardJob) =>
    buildShardHandler(await withKeys(), async () => {
      clearKeyringCache();
      return withKeys();
    })(event);
}

// Clients are built once per container; a failed build is not cached, so the next call retries.
export const plannerHandler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  planner ??= Promise.resolve().then(() => buildPlanner(process.env));
  planner.catch(() => (planner = undefined));
  return (await planner)(event);
};

export const shardHandler = async (
  event: SQSEvent | ShardJob,
): Promise<SQSBatchResponse | undefined> => {
  shard ??= Promise.resolve().then(() => buildShard(process.env));
  shard.catch(() => (shard = undefined));
  return (await shard)(event);
};
