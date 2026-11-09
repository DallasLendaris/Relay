import { randomUUID } from 'node:crypto';
import { LocationClient } from '@aws-sdk/client-location';
import { RDSDataClient } from '@aws-sdk/client-rds-data';
import { LocationGeocoder } from '../adapters/aws/location-geocoder.js';
import { PostgresAlertStore } from '../adapters/postgres/alert-store.js';
import { PostgresDeviceStore } from '../adapters/postgres/device-store.js';
import { PostgresPlaceStore } from '../adapters/postgres/place-store.js';
import { DataApiExecutor } from '../db/sql.js';
import { createApi, type ApiDeps, type ApiEvent, type ApiResult } from './index.js';

/** The stores the API needs that come from AWS (Aurora, Amazon Location). */
export type StoreDeps = Pick<ApiDeps, 'devices' | 'places' | 'alerts' | 'geocoder'>;
export type StoreFactory = () => StoreDeps | Promise<StoreDeps>;

/** Thrown when required environment variables are missing. Details are logged, never returned. */
export class MisconfiguredError extends Error {}

const errorResult = (code: string, message: string): ApiResult => ({
  statusCode: 500,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ code, message }),
});

/** Replaces UUID path segments so logs group by route and carry no identifiers. */
const routeOf = (path: string): string =>
  path.replace(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '{id}');

/**
 * Builds a Lambda handler. The factory runs once, on the first request, so cold
 * starts that never receive traffic do not open database connections.
 * Each request logs one JSON line: method, route, status, ms. Never headers, bodies or coordinates.
 */
export function createHandler(buildStores: StoreFactory): (event: ApiEvent) => Promise<ApiResult> {
  let route: ((event: ApiEvent) => Promise<ApiResult>) | undefined;
  return async (event) => {
    const started = Date.now();
    let result: ApiResult;
    try {
      route ??= createApi({
        ...(await buildStores()),
        clock: { now: () => new Date() },
        newId: randomUUID,
      });
      result = await route(event);
    } catch (err) {
      console.error(
        JSON.stringify({ level: 'error', msg: 'handler failed', error: (err as Error).message }),
      );
      result =
        err instanceof MisconfiguredError
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
  const names = ['DB_CLUSTER_ARN', 'DB_SECRET_ARN', 'DB_NAME', 'PLACE_INDEX_NAME'] as const;
  const missing = names.filter((n) => !env[n]);
  const [cluster, secret, dbName, index] = names.map((n) => env[n]);
  if (missing.length > 0 || !cluster || !secret || !dbName || !index) {
    throw new MisconfiguredError(`Missing environment variables: ${missing.join(', ')}`);
  }
  const db = new DataApiExecutor(new RDSDataClient({}), cluster, secret, dbName);
  return {
    devices: new PostgresDeviceStore(db),
    places: new PostgresPlaceStore(db),
    alerts: new PostgresAlertStore(db),
    geocoder: new LocationGeocoder(new LocationClient({}), index),
  };
};

export const handler = createHandler(awsStores);
