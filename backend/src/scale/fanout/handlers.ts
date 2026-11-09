/**
 * Lambda entry points for the fan-out: plannerHandler (plan queue) and shardHandler (shard
 * queue). Both return batchItemFailures. Dependencies are built from env on the first
 * invocation and reused while warm. The testable cores take their dependencies as arguments.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { ECSClient } from '@aws-sdk/client-ecs';
import { LambdaClient } from '@aws-sdk/client-lambda';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import { PostgresAlertStore } from '../../adapters/postgres/alert-store.js';
import { createKillSwitch } from '../../config/kill-switch.js';
import { clearKeyringCache, loadKeyring } from '../../config/secrets.js';
import { DataApiExecutor } from '../../db/sql.js';
import type { Context } from 'aws-lambda';
import type { Logger } from '../../delivery/types.js';
import { createAlertVerifier } from '../../signing/index.js';
import { createCheckInToken, type TokenKey } from '../../tokens/index.js';
import type { FanoutRefusal } from './common.js';
import { CachedAlertReader } from './alert-cache.js';
import { fleetScalerFromEnv } from './fleet-scaler.js';
import { SqsPausedQueue, type PausedQueue } from './paused-queue.js';
import { isAlertReady, isShardJob, type ResumableShardJob } from './messages.js';
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

/**
 * 'unsigned' is not final here (the sender retries it too): the alert may be signed with a key
 * that was rotated in after our keyring cache filled. So: refresh the keys once and try again;
 * if it is still unsigned, throw so SQS (planner) or Lambda's async retry (shard) tries again
 * later, and log `fanout.signature_failed` for the alarm-minded.
 */
async function retryUnsigned<D extends { logger: Logger }, R extends { refused?: FanoutRefusal }>(
  ids: { alertId: string; shardId?: string },
  deps: D,
  refreshKeys: (() => Promise<D>) | undefined,
  run: (d: D) => Promise<R>,
): Promise<R> {
  let result = await run(deps);
  if (result.refused === 'unsigned' && refreshKeys) result = await run(await refreshKeys());
  if (result.refused === 'unsigned') {
    deps.logger.error('fanout.signature_failed', { ...ids, refreshed: refreshKeys !== undefined });
    throw new Error(`alert ${ids.alertId} failed signature verification; will retry`);
  }
  return result;
}

/**
 * A paused kill switch is not final, and must not throw (a throw ends in a DLQ and a manual
 * redrive): with a parking queue the message is parked there, the run counts as handled, and the
 * paused-queue consumer re-dispatches it once the switch is back to running.
 */
async function parkIfPaused<R extends { refused?: FanoutRefusal }>(
  result: R,
  park: PausedQueue | undefined,
  message: Parameters<PausedQueue['park']>[0],
  log: Logger,
): Promise<R | { refused?: undefined }> {
  if (result.refused !== 'paused' || !park) return result;
  await park.park(message);
  log.warn('fanout.parked_while_paused', {
    kind: message.kind,
    alertId: message.kind === 'shard' ? message.job.alertId : message.ready.alertId,
    ...(message.kind === 'shard' ? { shardId: message.job.shardId } : {}),
  });
  return {};
}

export const buildPlannerHandler =
  (deps: PlannerDeps, refreshKeys?: () => Promise<PlannerDeps>, paused?: PausedQueue) =>
  (event: SQSEvent): Promise<SQSBatchResponse> =>
    runRecords(event, isAlertReady, async (ready) =>
      parkIfPaused(
        await retryUnsigned({ alertId: ready.alertId }, deps, refreshKeys, (d) =>
          planShards(ready, d),
        ),
        paused,
        { kind: 'plan', ready, parkedAt: deps.clock.now().toISOString() },
        deps.logger,
      ),
    );

/**
 * The shard worker takes a ShardJob two ways:
 *  - directly, from an async Lambda invoke (the default): the event IS the job. Failures throw,
 *    so Lambda's async retry (2 retries, then the on-failure DLQ) takes over.
 *  - from SQS (SHARD_DISPATCH=sqs): returns batchItemFailures.
 * A paused kill switch never throws when `paused` (the parking queue) is given: the job is
 * parked and the invoke succeeds. Without it, the old behavior: throw (direct) or retry (SQS).
 */
export const buildShardHandler =
  (deps: ShardWorkerDeps, refreshKeys?: () => Promise<ShardWorkerDeps>, paused?: PausedQueue) =>
  async (event: SQSEvent | ResumableShardJob): Promise<SQSBatchResponse | undefined> => {
    const run = async (job: ResumableShardJob) =>
      parkIfPaused(
        await retryUnsigned(
          { alertId: job.alertId, shardId: job.shardId },
          deps,
          refreshKeys,
          (d) => processShard(job, d),
        ),
        paused,
        { kind: 'shard', job, parkedAt: deps.clock.now().toISOString() },
        deps.logger,
      );
    if ('Records' in event) return runRecords(event, isShardJob, run);
    if (!isShardJob(event)) throw new Error('shard event is not a ShardJob');
    const result = await run(event);
    if (result.refused === 'paused') {
      throw new Error(`delivery paused; shard ${event.shardId} will retry`);
    }
    return undefined;
  };

/** The shard worker's token factory: a v2 token with the device's cell signed in. */
export const makeCheckInTokenFor =
  (key: TokenKey): ShardWorkerDeps['makeCheckInToken'] =>
  (alertId, deviceId, h3Cell, expiresAt) =>
    createCheckInToken({ alertId, deviceId, h3Cell, expiresAt }, key);

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
/** Optional: SCALER_ENV (fleet-scaler.ts) names the pre-scale settings. Off when any is missing. */
const PLANNER_ENV = [
  'DB_CLUSTER_ARN',
  'DB_SECRET_ARN',
  'DB_NAME',
  'ALERT_KEYS_SECRET_ARN',
  'KILL_SWITCH_PARAM',
  'PAUSED_QUEUE_URL',
] as const;

/** Upper bound of the random delay before a cold shard container's first kill-switch read. */
const COLD_START_JITTER_MS = 2_000;

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
  'PAUSED_QUEUE_URL',
] as const;

let planner: Promise<(event: SQSEvent) => Promise<SQSBatchResponse>> | undefined;
let shard:
  | Promise<
      (
        event: SQSEvent | ResumableShardJob,
        context?: Pick<Context, 'getRemainingTimeInMillis'>,
      ) => Promise<SQSBatchResponse | undefined>
    >
  | undefined;

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
  const fleetScaler = fleetScalerFromEnv(env, new ECSClient({}), logger);
  const base = {
    // One cached read per alert per 10 s per warm planner (see alert-cache.ts).
    alerts: new CachedAlertReader(alertStore(e)),
    shardDispatcher: shardDispatcherFromEnv(env),
    ...(fleetScaler ? { fleetScaler } : {}),
    isDeliveryPaused: createKillSwitch(e.KILL_SWITCH_PARAM),
    clock,
    logger,
  };
  const withKeys = async (): Promise<PlannerDeps> => ({
    ...base,
    verifyAlert: createAlertVerifier((await loadKeyring(e.ALERT_KEYS_SECRET_ARN)).all),
  });
  // Keys are read per invocation (loadKeyring caches for 5 minutes), so rotation is picked up.
  const paused = new SqsPausedQueue(new SQSClient({}), e.PAUSED_QUEUE_URL);
  return async (event: SQSEvent) =>
    buildPlannerHandler(
      await withKeys(),
      async () => {
        clearKeyringCache();
        return withKeys();
      },
      paused,
    )(event);
}

function buildShard(env: Record<string, string | undefined>) {
  const e = requireEnv(env, SHARD_ENV);
  const base = {
    alerts: new CachedAlertReader(alertStore(e)),
    // Continuations go back to this same function: Lambda sets AWS_LAMBDA_FUNCTION_NAME, so the
    // stack needs no env var that would make the function's configuration refer to itself.
    continuation: new LambdaShardDispatcher(
      new LambdaClient({}),
      env.AWS_LAMBDA_FUNCTION_NAME ?? '',
      1,
    ),
    cellIndex: new DynamoCellReader(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      e.CELL_INDEX_TABLE,
    ),
    sendQueue: new SqsSendQueue(new SQSClient({}), {
      urgent: e.URGENT_QUEUE_URL,
      standard: e.STANDARD_QUEUE_URL,
    }),
    // Up to 2 s of jitter before the first read: hundreds of cold shards must not hit SSM at once.
    isDeliveryPaused: createKillSwitch(e.KILL_SWITCH_PARAM, undefined, undefined, {
      coldJitterMs: COLD_START_JITTER_MS,
    }),
    clock,
    logger,
  };
  const paused = new SqsPausedQueue(new SQSClient({}), e.PAUSED_QUEUE_URL);
  const withKeys = async (): Promise<ShardWorkerDeps> => {
    const [alertKeys, tokenKeys] = await Promise.all([
      loadKeyring(e.ALERT_KEYS_SECRET_ARN),
      loadKeyring(e.TOKEN_KEYS_SECRET_ARN),
    ]);
    return {
      ...base,
      verifyAlert: createAlertVerifier(alertKeys.all),
      makeCheckInToken: makeCheckInTokenFor(tokenKeys.current),
    };
  };
  return async (
    event: SQSEvent | ResumableShardJob,
    context?: Pick<Context, 'getRemainingTimeInMillis'>,
  ) => {
    const remainingMs = context ? () => context.getRemainingTimeInMillis() : undefined;
    const timed = async (): Promise<ShardWorkerDeps> => ({
      ...(await withKeys()),
      ...(remainingMs ? { remainingMs } : {}),
    });
    return buildShardHandler(
      await timed(),
      async () => {
        clearKeyringCache();
        return timed();
      },
      paused,
    )(event);
  };
}

// Clients are built once per container; a failed build is not cached, so the next call retries.
export const plannerHandler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  planner ??= Promise.resolve().then(() => buildPlanner(process.env));
  planner.catch(() => (planner = undefined));
  return (await planner)(event);
};

export const shardHandler = async (
  event: SQSEvent | ResumableShardJob,
  context?: Pick<Context, 'getRemainingTimeInMillis'>,
): Promise<SQSBatchResponse | undefined> => {
  shard ??= Promise.resolve().then(() => buildShard(process.env));
  shard.catch(() => (shard = undefined));
  return (await shard)(event, context);
};
