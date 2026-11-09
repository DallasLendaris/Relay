/**
 * Ingest Lambda. EventBridge fires it every minute; it polls NWS twice (every 30 s, the NWS
 * recommended minimum, never faster) before returning. buildHandler takes the engine pieces
 * (see pipeline.ts); `handler` at the bottom wires the real AWS dependencies.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { S3Client } from '@aws-sdk/client-s3';
import { KMSClient } from '@aws-sdk/client-kms';
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
import type { AlertSigner } from '../signing/alertSignature.js';
import { alertSignerFromEnv, loadAlertPublicKeys, parseVerifyKeys } from '../signing/kms.js';
import { DataApiExecutor } from '../db/sql.js';
import { MIN_POLL_INTERVAL_MS, createNwsClient, sharedGate } from './nws-client.js';
import { createIngestPipeline, type PipelineDeps } from './pipeline.js';
import { pollLoop, type PollCounts, type PollDeps } from './poll.js';

/** The parts of PollDeps that the caller supplies. */
export type EngineDeps = Pick<
  PollDeps,
  'normalize' | 'parseFeed' | 'apply' | 'cellsFor' | 'onApplied' | 'reconcile' | 'flush'
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

/** Default seconds between polls: NWS asks for no more than one request per 30 s per URL. */
export const DEFAULT_POLL_INTERVAL_SECONDS = 30;
/** Default polls per (per-minute) invocation: at 0 s and 30 s. A third would overlap the next run. */
export const DEFAULT_POLLS_PER_INVOCATION = 2;

/** The configured poll interval in ms, never below the NWS minimum (a lower value is raised, with a warning). */
export function resolvePollIntervalMs(raw: string | undefined): number {
  const ms =
    positiveNumber(raw, DEFAULT_POLL_INTERVAL_SECONDS, 'POLL_INTERVAL_SECONDS', false) * 1000;
  if (ms >= MIN_POLL_INTERVAL_MS) return ms;
  console.warn(
    JSON.stringify({
      level: 'warn',
      msg: 'POLL_INTERVAL_SECONDS is below the NWS minimum; raised to 30',
      raw,
      minSeconds: MIN_POLL_INTERVAL_MS / 1000,
    }),
  );
  return MIN_POLL_INTERVAL_MS;
}

/** NWS requires a User-Agent that says who to contact. Warns (does not throw) when none is visible. */
export function warnIfNoContact(userAgent: string): void {
  if (/@|https?:\/\//i.test(userAgent)) return;
  console.warn(
    JSON.stringify({
      level: 'warn',
      msg: 'NWS_USER_AGENT has no contact (email or URL); NWS requires one',
    }),
  );
}

const METRICS = [
  'Accepted',
  'Rejected',
  'Applied',
  'Errors',
  'FetchFailed',
  'Quarantined',
  'UnknownZones',
  'PublishFailed',
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
    UnknownZones: sum((c) => c.unknownZones),
    PublishFailed: sum((c) => c.publishFailed),
  };
}

export function buildHandler(engine: EngineDeps, overrides: HandlerOverrides = {}) {
  return async (_event: unknown, context: LambdaContext): Promise<{ polls: PollCounts[] }> => {
    const env: HandlerEnv = overrides.env ?? process.env;
    const intervalMs = resolvePollIntervalMs(env.POLL_INTERVAL_SECONDS);
    const polls = positiveNumber(
      env.POLLS_PER_INVOCATION,
      DEFAULT_POLLS_PER_INVOCATION,
      'POLLS_PER_INVOCATION',
      true,
    );

    let base = overrides.base;
    if (!base) {
      const { RAW_BUCKET, STATE_TABLE, NWS_USER_AGENT } = env;
      if (!RAW_BUCKET || !STATE_TABLE || !NWS_USER_AGENT) {
        throw new Error('RAW_BUCKET, STATE_TABLE and NWS_USER_AGENT must be set');
      }
      warnIfNoContact(NWS_USER_AGENT);
      base = {
        // The client lives for this invocation, so If-None-Match carries across its polls. The
        // shared gate keeps the 30 s spacing (and any Retry-After backoff) across invocations
        // that run in the same warm container.
        client: createNwsClient({
          fetch,
          userAgent: NWS_USER_AGENT,
          minIntervalMs: MIN_POLL_INTERVAL_MS,
          gate: sharedGate,
        }),
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
  'ALERT_SIGNING_KMS_KEY',
  'ALERT_SIGNING_KID',
  'ALERT_VERIFY_KEYS',
  'KILL_SWITCH_PARAM',
  'NWS_USER_AGENT',
] as const;

/**
 * Optional scale env vars (see the integration contract):
 *   FANOUT_MODE          "queue" | "inline" (unset means inline)
 *   PLAN_QUEUE_URL       required only when FANOUT_MODE=queue
 *   PUBLIC_ALERTS_BUCKET publish signed alerts for the CDN when set
 *   PUBLIC_ALERTS_REGION region of that bucket (the Lambda's own region when unset)
 *   ZONE_PACK_BUCKET     read zone geometry from the pack when set (NWS fills misses)
 * Throws on a bad combination so a misconfigured deploy fails on its first invocation.
 */
export function validateScaleEnv(source: Record<string, string | undefined>): void {
  const mode = source.FANOUT_MODE;
  if (mode !== undefined && mode !== '' && mode !== 'queue' && mode !== 'inline') {
    throw new Error(`FANOUT_MODE must be "queue" or "inline", got "${mode}"`);
  }
  if (mode === 'queue' && !source.PLAN_QUEUE_URL) {
    throw new Error('PLAN_QUEUE_URL must be set when FANOUT_MODE=queue');
  }
  if (source.PUBLIC_ALERTS_BUCKET && !source.PUBLIC_ALERTS_REGION) {
    console.warn(
      JSON.stringify({
        level: 'warn',
        msg: 'PUBLIC_ALERTS_REGION not set; using the Lambda region for the public alerts bucket',
      }),
    );
  }
}

/** The real AWS-backed dependencies. Built once, on the first invocation, and reused while warm. */
interface Runtime {
  env: Record<(typeof REQUIRED_ENV)[number], string>;
  stores: Pick<PipelineDeps, 'alertStore' | 'places' | 'devices' | 'queue' | 'isDeliveryPaused'>;
  secrets: SecretsManagerClient;
  kms: KMSClient;
  signer: AlertSigner;
}

function buildRuntime(source: Record<string, string | undefined>): Runtime {
  const missing = REQUIRED_ENV.filter((k) => !source[k]);
  if (missing.length > 0) throw new Error(`Missing environment variables: ${missing.join(', ')}`);
  const env = Object.fromEntries(REQUIRED_ENV.map((k) => [k, source[k]])) as Runtime['env'];
  validateScaleEnv(source);

  const db = new DataApiExecutor(
    new RDSDataClient({}),
    env.DB_CLUSTER_ARN,
    env.DB_SECRET_ARN,
    env.DB_NAME,
  );
  const kms = new KMSClient({});
  return {
    env,
    secrets: new SecretsManagerClient({}),
    kms,
    signer: alertSignerFromEnv(env, kms),
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
  const { env, stores, secrets, kms, signer } = runtime;
  // loadKeyring caches for 5 minutes, so a rotated secret is picked up while warm. Alert public
  // keys are fetched from KMS once per container; signing calls KMS once per saved alert.
  const [publicKeys, tokenKeys] = await Promise.all([
    loadAlertPublicKeys(parseVerifyKeys(env.ALERT_VERIFY_KEYS), kms),
    loadKeyring(env.TOKEN_KEYS_SECRET_ARN, secrets),
  ]);
  const engine = createIngestPipeline({
    ...stores,
    alertSigning: { signer, publicKeys },
    tokenKeys,
    fetch,
    userAgent: env.NWS_USER_AGENT,
  });
  return buildHandler(engine)(event, context);
};
