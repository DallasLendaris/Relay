/**
 * Ingest Lambda. EventBridge fires it every minute; it polls NWS several times
 * (about every 12 s) before returning. buildHandler takes the engine pieces
 * (see pipeline.ts); `handler` at the bottom wires the real AWS dependencies.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { S3Client } from '@aws-sdk/client-s3';
import { SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoSeenStore } from '../adapters/aws/dynamo-seen-store.js';
import { S3RawArchive } from '../adapters/aws/s3-raw-archive.js';
import { SqsDeliveryQueue } from '../adapters/aws/sqs-delivery-queue.js';
import { PostgresAlertStore } from '../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../adapters/postgres/place-store.js';
import { createKillSwitch } from '../config/kill-switch.js';
import { loadKeyring } from '../config/secrets.js';
import { DataApiExecutor } from '../db/sql.js';
import { createNwsClient } from './nws-client.js';
import { createIngestPipeline, type PipelineDeps } from './pipeline.js';
import { pollLoop, type PollCounts, type PollDeps } from './poll.js';

/** The parts of PollDeps that the caller supplies. */
export type EngineDeps = Pick<
  PollDeps,
  'normalize' | 'parseFeed' | 'apply' | 'cellsFor' | 'onApplied'
>;

export interface HandlerEnv {
  RAW_BUCKET?: string;
  STATE_TABLE?: string;
  NWS_USER_AGENT?: string;
  POLL_INTERVAL_SECONDS?: string;
  POLLS_PER_INVOCATION?: string;
}

export interface LambdaContext {
  getRemainingTimeInMillis(): number;
}

export interface HandlerOverrides {
  env?: HandlerEnv;
  /** Replace the real NWS client, archive and seen store (tests). */
  base?: Pick<PollDeps, 'client' | 'archive' | 'seen'>;
  sleep?(ms: number): Promise<void>;
}

/** Read a positive number from an env var; empty or invalid falls back to the default. */
export function positiveNumber(
  raw: string | undefined,
  fallback: number,
  name: string,
  integer: boolean,
): number {
  if (raw === undefined) return fallback;
  const n = raw.trim() === '' ? NaN : Number(raw);
  if (Number.isFinite(n) && n > 0 && (!integer || Number.isInteger(n))) return n;
  console.warn(
    JSON.stringify({ level: 'warn', msg: 'invalid env var, using default', name, raw, fallback }),
  );
  return fallback;
}

const METRICS = [
  'Accepted',
  'Rejected',
  'Applied',
  'Errors',
  'FetchFailed',
  'Quarantined',
] as const;

/**
 * One CloudWatch Embedded Metric Format line for a whole invocation (all its polls summed).
 * CloudWatch turns it into custom metrics in "Relay/Ingest" with no dimensions. It is emitted
 * even when every value is 0, so alarms always have data to evaluate.
 */
export function buildEmf(polls: PollCounts[], now: Date = new Date()): Record<string, unknown> {
  const sum = (pick: (c: PollCounts) => number): number =>
    polls.reduce((total, c) => total + pick(c), 0);
  return {
    _aws: {
      Timestamp: now.getTime(),
      CloudWatchMetrics: [
        {
          Namespace: 'Relay/Ingest',
          Dimensions: [[]],
          Metrics: METRICS.map((Name) => ({ Name, Unit: 'Count' })),
        },
      ],
    },
    Accepted: sum((c) => c.accepted),
    Rejected: sum((c) => c.rejected),
    Applied: sum((c) => c.applied),
    Errors: sum((c) => c.errors),
    FetchFailed: sum((c) => c.fetchFailed),
    Quarantined: sum((c) => c.quarantined),
  };
}

export function buildHandler(engine: EngineDeps, overrides: HandlerOverrides = {}) {
  return async (_event: unknown, context: LambdaContext): Promise<{ polls: PollCounts[] }> => {
    const env: HandlerEnv = overrides.env ?? process.env;
    const intervalMs =
      positiveNumber(env.POLL_INTERVAL_SECONDS, 12, 'POLL_INTERVAL_SECONDS', false) * 1000;
    const polls = positiveNumber(env.POLLS_PER_INVOCATION, 5, 'POLLS_PER_INVOCATION', true);

    let base = overrides.base;
    if (!base) {
      const { RAW_BUCKET, STATE_TABLE, NWS_USER_AGENT } = env;
      if (!RAW_BUCKET || !STATE_TABLE || !NWS_USER_AGENT) {
        throw new Error('RAW_BUCKET, STATE_TABLE and NWS_USER_AGENT must be set');
      }
      base = {
        // The client lives for this invocation, so If-None-Match carries across its polls.
        client: createNwsClient({ fetch, userAgent: NWS_USER_AGENT }),
        archive: new S3RawArchive(new S3Client({}), RAW_BUCKET),
        seen: new DynamoSeenStore(DynamoDBDocumentClient.from(new DynamoDBClient({})), STATE_TABLE),
      };
    }

    const results = await pollLoop(
      { ...base, ...engine },
      {
        polls,
        intervalMs,
        sleep: overrides.sleep,
        remainingMs: () => context.getRemainingTimeInMillis(),
      },
    );
    console.log(JSON.stringify({ event: 'invocation.done', polls: results }));
    // pollOnce swallows per-feature errors so the Lambda "Errors" metric stays quiet during
    // an outage; these custom metrics are what the alarms watch instead.
    console.log(JSON.stringify(buildEmf(results)));
    return { polls: results };
  };
}

const REQUIRED_ENV = [
  'DB_CLUSTER_ARN',
  'DB_SECRET_ARN',
  'DB_NAME',
  'DELIVERY_QUEUE_URL',
  'TOKEN_KEYS_SECRET_ARN',
  'ALERT_KEYS_SECRET_ARN',
  'KILL_SWITCH_PARAM',
  'NWS_USER_AGENT',
] as const;

/** The real AWS-backed dependencies. Built once, on the first invocation, and reused while warm. */
interface Runtime {
  env: Record<(typeof REQUIRED_ENV)[number], string>;
  stores: Pick<PipelineDeps, 'alertStore' | 'places' | 'devices' | 'queue' | 'isDeliveryPaused'>;
  secrets: SecretsManagerClient;
}

function buildRuntime(source: Record<string, string | undefined>): Runtime {
  const missing = REQUIRED_ENV.filter((k) => !source[k]);
  if (missing.length > 0) throw new Error(`Missing environment variables: ${missing.join(', ')}`);
  const env = Object.fromEntries(REQUIRED_ENV.map((k) => [k, source[k]])) as Runtime['env'];

  const db = new DataApiExecutor(
    new RDSDataClient({}),
    env.DB_CLUSTER_ARN,
    env.DB_SECRET_ARN,
    env.DB_NAME,
  );
  return {
    env,
    secrets: new SecretsManagerClient({}),
    stores: {
      alertStore: new PostgresAlertStore(db),
      places: new PostgresPlaceStore(db),
      devices: new PostgresDeviceStore(db),
      queue: new SqsDeliveryQueue(new SQSClient({}), env.DELIVERY_QUEUE_URL),
      isDeliveryPaused: createKillSwitch(env.KILL_SWITCH_PARAM),
    },
  };
}

let runtime: Runtime | undefined;

export const handler = async (
  event: unknown,
  context: LambdaContext,
): Promise<{ polls: PollCounts[] }> => {
  runtime ??= buildRuntime(process.env);
  const { env, stores, secrets } = runtime;
  // loadKeyring caches for 5 minutes, so a rotated secret is picked up while warm.
  const [alertKeys, tokenKeys] = await Promise.all([
    loadKeyring(env.ALERT_KEYS_SECRET_ARN, secrets),
    loadKeyring(env.TOKEN_KEYS_SECRET_ARN, secrets),
  ]);
  const engine = createIngestPipeline({
    ...stores,
    alertKeys,
    tokenKeys,
    fetch,
    userAgent: env.NWS_USER_AGENT,
  });
  return buildHandler(engine)(event, context);
};
