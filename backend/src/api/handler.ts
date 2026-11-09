import { randomUUID } from 'node:crypto';
import { LocationClient } from '@aws-sdk/client-location';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { DynamoCheckInStore } from '../adapters/aws/dynamo-checkin-store.js';
import { LocationGeocoder } from '../adapters/aws/location-geocoder.js';
import { PostgresAlertStore } from '../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../adapters/postgres/place-store.js';
import { DynamoCellIndex } from '../scale/cell-index/dynamo-cell-index.js';
import { loadKeyring } from '../config/secrets.js';
import { DataApiExecutor } from '../db/sql.js';
import { createApi, type ApiDeps, type ApiEvent, type ApiResult } from './index.js';
import { routeOf } from './router.js';

/** The stores the API needs that come from AWS (Aurora, Amazon Location). */
export type StoreDeps = Pick<
  ApiDeps,
  'devices' | 'places' | 'alerts' | 'geocoder' | 'checkins' | 'cellIndex'
> & {
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
      const { loadTokenKeys, loadDeviceCredentialKeys, ...rest } = stores;
      // Both keyring loaders cache, so this is no Secrets Manager call per request.
      const [tokenKeys, credentialKeys] = await Promise.all([
        loadTokenKeys(),
        loadDeviceCredentialKeys?.(),
      ]);
      const route = createApi({
        ...rest,
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
    console.log(
      JSON.stringify({
        method: event.requestContext.http.method,
        route: routeOf(event.rawPath),
        status: result.statusCode,
        ms: Date.now() - started,
      }),
    );
    return result;
  };
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
  const db = new DataApiExecutor(new RDSDataClient({}), cluster, secret, dbName);
  return {
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    alerts: new PostgresAlertStore(db),
    geocoder: new LocationGeocoder(new LocationClient({}), index),
    checkins: new DynamoCheckInStore(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      checkinsTable,
    ),
    cellIndex: new DynamoCellIndex(
      DynamoDBDocumentClient.from(new DynamoDBClient({})),
      cellIndexTable,
    ),
    loadTokenKeys: async () => (await loadKeyring(keysArn)).all,
    loadDeviceCredentialKeys: () => loadKeyring(credentialKeysArn),
  };
};

export const handler = createHandler(awsStores);
