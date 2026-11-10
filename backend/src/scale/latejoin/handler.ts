/**
 * Late-join Lambda: the ONLY consumer of the late-join queue, and (with the shard workers and the
 * sender) one of the few identities allowed to send to the urgent and standard send queues.
 *
 * Env: DB_CLUSTER_ARN, DB_SECRET_ARN, DB_NAME, URGENT_QUEUE_URL, STANDARD_QUEUE_URL,
 * IDEMPOTENCY_TABLE, ALERT_VERIFY_KEYS, TOKEN_KEYS_SECRET_ARN, KILL_SWITCH_PARAM, and
 * LATEJOIN_QUEUE_URL (its own queue: it parks intents there while delivery is paused).
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
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
import {
  isLateJoinIntent,
  MAX_DELAY_SECONDS,
  PARK_GIVE_UP_MS,
  PLAN_WAIT_DELAY_SECONDS,
  PLAN_WAIT_FIRST_DELAY_SECONDS,
  PLAN_WAIT_GIVE_UP_MS,
  planWaitDelaySeconds,
  SqsLateJoinQueue,
  type LateJoinIntent,
  type LateJoinQueue,
} from './intent.js';
import {
  processLateJoin,
  type LateJoinDeps,
  type LateJoinOptions,
  type LateJoinResult,
} from './latejoin.js';

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
  intent: { deviceId: string; reason?: string },
  deps: LateJoinDeps,
  refresh?: () => Promise<LateJoinDeps>,
  opts: LateJoinOptions = {},
): Promise<LateJoinResult> {
  let result = await processLateJoin(intent, deps, opts);
  if (result.unsigned > 0 && refresh) {
    result = await processLateJoin(intent, await refresh(), opts);
  }
  return result;
}

export interface ParkOptions {
  /** The late-join queue itself: a paused intent goes back to it with a delay. */
  queue: LateJoinQueue;
  /** Delay between parks, seconds (SQS maximum 900). Default 900. */
  delaySeconds?: number;
  /** Give up this long after the intent was first enqueued. Default 24 h. */
  giveUpMs?: number;
  /**
   * G5: wait for the planned fan-out instead of risking a duplicate of it. An alert the planned
   * fan-out may still carry to the device (latejoin.ts plannedFate) is deferred: the intent goes
   * back to this queue, stamped with when the waiting began, after planWaitDelaySeconds: half the
   * time waited so far, from `firstDelaySeconds` (default PLAN_WAIT_FIRST_DELAY_SECONDS) up to
   * `delaySeconds` (the cap, default PLAN_WAIT_DELAY_SECONDS). Once it has waited `giveUpMs`
   * (default PLAN_WAIT_GIVE_UP_MS) late join decides without waiting, which sends where in doubt.
   * Absent: no waiting (decide at once).
   */
  planWait?: { delaySeconds?: number; firstDelaySeconds?: number; giveUpMs?: number };
}

/**
 * Records of one batch handled at once (distinct devices only). Each record is a handful of
 * sequential Aurora and DynamoDB round trips, so handling a batch of 10 one by one made the last
 * record wait for the other nine: during a signup surge that serial wait, times the batch size,
 * was most of a late joiner's queue time. Bounded so one invocation adds at most this many
 * concurrent Aurora statements (the event source's maxConcurrency bounds the invocations).
 */
export const LATEJOIN_RECORD_CONCURRENCY = 5;

export interface HandlerOptions {
  /** Records handled at once per batch. Default LATEJOIN_RECORD_CONCURRENCY; 1 = one by one. */
  recordConcurrency?: number;
}

/**
 * Per record: a body that is not an intent goes back to the queue (and the DLQ after the retry
 * limit, where the alarm sees it); a failure retries.
 *
 * A paused kill switch PARKS the intent: it is re-enqueued with a delay of up to 15 minutes,
 * carrying its original enqueue time, and the record is then done. Retrying the record instead
 * would burn its receive count (20 x 180 s is an hour) and send the intent to the DLQ during a
 * long pause, so those devices would never get the alert after unpause. A parked intent is given
 * up on only 24 hours after it was first enqueued (an alert is long over by then); that is logged
 * as an error. If the re-enqueue itself fails the record fails and retries as before.
 *
 * Up to `recordConcurrency` devices of a batch are handled at once (a device's own records in
 * order); each record is independent (its own reads and claims), so only the order of a device's
 * records is kept.
 */
export const buildLateJoinHandler = (
  deps: LateJoinDeps,
  refresh?: () => Promise<LateJoinDeps>,
  park?: ParkOptions,
  opts: HandlerOptions = {},
) => {
  /** One record; true when it must go back to the queue (a batch item failure). */
  const handleRecord = async (record: SQSRecord): Promise<boolean> => {
    try {
      const body: unknown = JSON.parse(record.body);
      if (!isLateJoinIntent(body)) throw new Error('message is not a late-join intent');
      const nowMs = deps.clock.now().getTime();
      const waitingSince = waitingSinceMs(body, nowMs);
      const wait = park?.planWait;
      const mayWait =
        wait !== undefined &&
        (waitingSince === undefined ||
          nowMs - waitingSince < (wait.giveUpMs ?? PLAN_WAIT_GIVE_UP_MS));
      if (wait && !mayWait) {
        deps.logger.warn('latejoin.plan_wait_gave_up', { messageId: record.messageId });
      }
      // Only the device id (and the advisory reason) is used; see processLateJoin.
      const result = await runIntent(
        { deviceId: body.deviceId, ...(body.reason === 'outbox' ? { reason: 'outbox' } : {}) },
        deps,
        refresh,
        { waitForPlan: mayWait },
      );
      if (result.refused === 'paused') {
        if (!park) return true;
        const now = deps.clock.now().getTime();
        const first = firstEnqueuedMs(body, record.attributes?.SentTimestamp, now);
        if (now - first >= (park.giveUpMs ?? PARK_GIVE_UP_MS)) {
          deps.logger.error('latejoin.paused_expired', { messageId: record.messageId });
          return false;
        }
        // The plan wait starts again after the pause (waitingSince dropped): the planned
        // batches it waited for could not be sent while paused either, so a pause longer than
        // the wait bound must not make every waiting intent send at once next to them.
        const { waitingSince: _paused, ...rest } = body;
        const parked: LateJoinIntent = { ...rest, enqueuedAt: new Date(first).toISOString() };
        await park.queue.enqueue(parked, {
          delaySeconds: Math.min(MAX_DELAY_SECONDS, park.delaySeconds ?? MAX_DELAY_SECONDS),
        });
        deps.logger.info('latejoin.parked', { messageId: record.messageId });
      } else if (result.deferred > 0 && park && wait) {
        // The alerts sent in this run are 'sent' on their per-target claims, so the re-run
        // skips them; only the deferred ones are decided again.
        const again: LateJoinIntent = {
          ...body,
          waitingSince: new Date(waitingSince ?? nowMs).toISOString(),
        };
        const delaySeconds = planWaitDelaySeconds(
          nowMs - (waitingSince ?? nowMs),
          wait.delaySeconds ?? PLAN_WAIT_DELAY_SECONDS,
          wait.firstDelaySeconds ?? PLAN_WAIT_FIRST_DELAY_SECONDS,
        );
        await park.queue.enqueue(again, { delaySeconds });
        deps.logger.info('latejoin.waiting_for_plan', {
          messageId: record.messageId,
          deferred: result.deferred,
          delaySeconds,
        });
      }
      return false;
    } catch (err) {
      deps.logger.error('latejoin.record_failed', {
        messageId: record.messageId,
        error: err instanceof Error ? err.name : 'UnknownError',
      });
      return true;
    }
  };

  const concurrency = Math.max(
    1,
    Math.floor(opts.recordConcurrency ?? LATEJOIN_RECORD_CONCURRENCY),
  );
  return async (event: SQSEvent): Promise<SQSBatchResponse> => {
    // Records of one device stay in order, one at a time: two intents of a device (its push target,
    // then its place) racing would find each other's per-target claim leased, and the loser would
    // go back for the queue's visibility timeout. Distinct devices run side by side.
    const groups = new Map<string, SQSRecord[]>();
    for (const record of event.Records) {
      const key = deviceKeyOf(record) ?? `message:${record.messageId}`;
      const group = groups.get(key);
      if (group) group.push(record);
      else groups.set(key, [record]);
    }
    const queue = [...groups.values()];
    const failed = new Set<SQSRecord>();
    const worker = async (): Promise<void> => {
      for (let group = queue.shift(); group; group = queue.shift()) {
        for (const record of group) if (await handleRecord(record)) failed.add(record);
      }
    };
    await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, worker));
    return {
      batchItemFailures: event.Records.filter((r) => failed.has(r)).map((r) => ({
        itemIdentifier: r.messageId,
      })),
    };
  };
};

/** The device a record is about (for ordering only), or undefined when the body is not an intent. */
function deviceKeyOf(record: SQSRecord): string | undefined {
  try {
    const body: unknown = JSON.parse(record.body);
    return isLateJoinIntent(body) ? body.deviceId.toLowerCase() : undefined;
  } catch {
    return undefined;
  }
}

/** When this intent first waited for the planned fan-out (never in the future), if it has. */
function waitingSinceMs(intent: LateJoinIntent, now: number): number | undefined {
  const t = intent.waitingSince ? Date.parse(intent.waitingSince) : NaN;
  return Number.isFinite(t) ? Math.min(t, now) : undefined;
}

/** When the intent was first enqueued: our own stamp, else SQS's send time, else now. */
function firstEnqueuedMs(
  intent: LateJoinIntent,
  sentTimestamp: string | undefined,
  now: number,
): number {
  const own = intent.enqueuedAt ? Date.parse(intent.enqueuedAt) : NaN;
  if (Number.isFinite(own)) return Math.min(own, now);
  const sqs = Number(sentTimestamp);
  return Number.isFinite(sqs) && sqs > 0 ? Math.min(sqs, now) : now;
}

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
  'LATEJOIN_QUEUE_URL',
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
  const park: ParkOptions = {
    queue: new SqsLateJoinQueue(new SQSClient({}), e.LATEJOIN_QUEUE_URL),
    planWait: {},
  };
  return async (event: SQSEvent) =>
    buildLateJoinHandler(
      await withKeys(),
      async () => {
        clearKeyringCache();
        return withKeys();
      },
      park,
    )(event);
}

// Clients are built once per container; a failed build is not cached, so the next call retries.
let built: Promise<(event: SQSEvent) => Promise<SQSBatchResponse>> | undefined;
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
  built ??= Promise.resolve().then(() => build(process.env));
  built.catch(() => (built = undefined));
  return (await built)(event);
};
