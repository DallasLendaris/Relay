import { randomUUID } from 'node:crypto';
import { LocationClient } from '@aws-sdk/client-location';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { DynamoCheckInStore } from '../adapters/aws/dynamo-checkin-store.js';
import { LocationGeocoder } from '../adapters/aws/location-geocoder.js';
import { PostgresAlertStore } from '../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../adapters/postgres/place-store.js';
import type { SendQueue } from '../scale/contracts.js';
import { DynamoCellIndex } from '../scale/cell-index/dynamo-cell-index.js';
import { SqsIndexRepairQueue } from '../scale/cell-index/repair-queue.js';
import { SqsSendQueue } from '../scale/sender/sqs-send-queue.js';
import type { TokenKey } from '../tokens/index.js';
import { loadKeyring } from '../config/secrets.js';
import { DataApiExecutor } from '../db/sql.js';
import { createApi, type ApiDeps, type ApiEvent, type ApiResult } from './index.js';
import { secretsManagerOriginGuard, type OriginGuard } from './origin-guard.js';
import { routeOf } from './router.js';

/** The stores the API needs that come from AWS (Aurora, Amazon Location). */
export type StoreDeps = Pick<
  ApiDeps,
  | 'devices'
  | 'places'
  | 'alerts'
  | 'geocoder'
  | 'checkins'
  | 'cellIndex'
  | 'repairQueue'
  | 'checkInRetentionDays'
> & {
  /** When set, requests without CloudFront's secret header are refused (REQUIRE_ORIGIN_HEADER). */
  originGuard?: OriginGuard;
  /** Send queues for late joiners; with `loadTokenSigningKey` it enables the late-join send. */
  sendQueue?: SendQueue;
  /** The current token key, used to sign the check-in tokens of late-join pushes. */
  loadTokenSigningKey?: () => Promise<TokenKey>;
  /** Called per request; the keyring loader caches, so key rotation is picked up within minutes. */
  loadTokenKeys: () => Promise<Map<string, string>>;
  /** Device credential keyring (verify keys plus the current key used to issue credentials). */
  loadDeviceCredentialKeys?: () => Promise<{
    current: { id: string; secret: string };
    all: Map<string, string>;
  }>;
};
export type StoreFactory = () => StoreDeps | Promise<StoreDeps>;

/** Thrown when required environment variables are missing. Details are logged, never returned. */
export class MisconfiguredError extends Error {}

const errorResult = (code: string, message: string): ApiResult => ({
  statusCode: 500,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ code, message }),
});

/**
 * Builds a Lambda handler. The factory runs once, on the first request, so cold
 * starts that never receive traffic do not open database connections.
 * Each request logs one JSON line: method, route, status, ms. Never headers, bodies or coordinates.
 */
export function createHandler(buildStores: StoreFactory): (event: ApiEvent) => Promise<ApiResult> {
  let stores: StoreDeps | undefined;
  return async (event) => {
    const started = Date.now();
    let result: ApiResult;
    try {
      stores ??= await buildStores();
      const {
        loadTokenKeys,
        loadDeviceCredentialKeys,
        loadTokenSigningKey,
        originGuard,
        sendQueue,
        ...rest
      } = stores;
      // CloudFront adds a secret header; a request without it skipped the WAF. Fail closed:
      // if the secret cannot be loaded, the request errors (500) instead of being let through.
      if (originGuard && !(await originGuard.allows(event.headers ?? {}))) {
        result = {
          statusCode: 403,
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ code: 'forbidden', message: 'Forbidden' }),
        };
        return log(event, started, result);
      }
      // The keyring loaders cache, so this is no Secrets Manager call per request.
      const [tokenKeys, credentialKeys, tokenKey] = await Promise.all([
        loadTokenKeys(),
        loadDeviceCredentialKeys?.(),
        sendQueue ? loadTokenSigningKey?.() : undefined,
      ]);
      const route = createApi({
        ...rest,
        ...(sendQueue && tokenKey ? { lateJoin: { sendQueue, tokenKey } } : {}),
        tokenKeys,
        ...(credentialKeys
          ? {
              deviceCredentialKeys: credentialKeys.all,
              deviceCredentialKey: credentialKeys.current,
            }
          : {}),
        clock: { now: () => new Date() },
        newId: randomUUID,
      });
      result = await route(event);
    } catch (err) {
      // Log the error name and a stable code only. err.message from a database error can
      // contain row contents (Postgres "Failing row contains ..." includes coordinates).
      // Our own MisconfiguredError lists env var names only, so it is safe and useful.
      const misconfigured = err instanceof MisconfiguredError;
      console.error(
        JSON.stringify({
          level: 'error',
          msg: 'handler failed',
          error: err instanceof Error ? err.name : 'UnknownError',
          code: misconfigured ? 'misconfigured' : 'internal_error',
          route: routeOf(event.rawPath),
          ...(misconfigured ? { detail: (err as Error).message } : {}),
        }),
      );
      result = misconfigured
        ? errorResult('misconfigured', 'Service is not configured')
        : errorResult('internal_error', 'Internal error');
    }
    return log(event, started, result);
  };
}

function log(event: ApiEvent, started: number, result: ApiResult): ApiResult {
  console.log(
    JSON.stringify({
      method: event.requestContext.http.method,
      route: routeOf(event.rawPath),
      status: result.statusCode,
      ms: Date.now() - started,
    }),
  );
  return result;
}

/** Real factory: Aurora via the Data API plus Amazon Location. Reads env vars when first called. */
export const awsStores: StoreFactory = () => {
  const env = process.env;
  const names = [
    'DB_CLUSTER_ARN',
    'DB_SECRET_ARN',
    'DB_NAME',
    'PLACE_INDEX_NAME',
    'CHECKINS_TABLE',
    'TOKEN_KEYS_SECRET_ARN',
    'DEVICE_CREDENTIAL_KEYS_SECRET_ARN',
    'CELL_INDEX_TABLE',
  ] as const;
  const missing = names.filter((n) => !env[n]);
  const [
    cluster,
    secret,
    dbName,
    index,
    checkinsTable,
    keysArn,
    credentialKeysArn,
    cellIndexTable,
  ] = names.map((n) => env[n]);
  if (
    missing.length > 0 ||
    !cluster ||
    !secret ||
    !dbName ||
    !index ||
    !checkinsTable ||
    !keysArn ||
    !credentialKeysArn ||
    !cellIndexTable
  ) {
    throw new MisconfiguredError(`Missing environment variables: ${missing.join(', ')}`);
  }
  // Optional scale wiring. Missing values turn the feature off (dev), never fail the request.
  const repairQueueUrl = env['INDEX_REPAIR_QUEUE_URL'];
  const urgentUrl = env['URGENT_QUEUE_URL'];
  const standardUrl = env['STANDARD_QUEUE_URL'];
  // Check-in retention after the alert ends (counsel decides; default 30 days).
  const retentionRaw = Number(env['CHECKIN_RETENTION_DAYS']);
  const retentionDays =
    env['CHECKIN_RETENTION_DAYS'] && Number.isFinite(retentionRaw) && retentionRaw > 0
      ? retentionRaw
      : undefined;
  const requireOrigin = env['REQUIRE_ORIGIN_HEADER'] === 'true';
  const originSecretArn = env['ORIGIN_VERIFY_SECRET_ARN'];
  if (requireOrigin && !originSecretArn) {
    throw new MisconfiguredError('Missing environment variables: ORIGIN_VERIFY_SECRET_ARN');
  }
  const sqs = new SQSClient({});
  const db = new DataApiExecutor(new RDSDataClient({}), cluster, secret, dbName);
  return {
    ...(requireOrigin && originSecretArn
      ? { originGuard: secretsManagerOriginGuard(originSecretArn) }
      : {}),
    ...(repairQueueUrl ? { repairQueue: new SqsIndexRepairQueue(sqs, repairQueueUrl) } : {}),
    ...(urgentUrl && standardUrl
      ? {
          sendQueue: new SqsSendQueue(sqs, { urgent: urgentUrl, standard: standardUrl }),
          loadTokenSigningKey: async () => (await loadKeyring(keysArn)).current,
        }
      : {}),
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    alerts: new PostgresAlertStore(db),
    geocoder: new LocationGeocoder(new LocationClient({}), index),
    checkins: new DynamoCheckInStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      checkinsTable,
      retentionDays,
    ),
    ...(retentionDays !== undefined ? { checkInRetentionDays: retentionDays } : {}),
    cellIndex: new DynamoCellIndex(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      cellIndexTable,
    ),
    loadTokenKeys: async () => (await loadKeyring(keysArn)).all,
    loadDeviceCredentialKeys: () => loadKeyring(credentialKeysArn),
  };
};

export const handler = createHandler(awsStores);
