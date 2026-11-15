import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { S3Client } from '@aws-sdk/client-s3';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { secretsManagerOriginGuard, type OriginGuard } from '../api/origin-guard.js';
import { LayeredZoneProvider } from '../geo/layered-zone-provider.js';
import { S3ZoneProvider } from '../geo/s3-zone-provider.js';
import { NwsZoneProvider } from '../geo/zones.js';
import { DynamoCheckInTotals } from '../scale/checkins/totals.js';
import { ZoneCountyResolver } from './areas.js';
import { DEFAULT_AUDIT_PREFIX, S3AuditSink } from './audit.js';
import {
  createAgencyApi,
  type AgencyApi,
  type AgencyRequest,
  type AgencyResponse,
} from './service.js';
import { DEFAULT_MIN_CELL_COUNT } from './suppression.js';

/** The parts of an API Gateway HTTP API (payload 2.0) event with a JWT authorizer we read. */
export interface AgencyHttpEvent {
  routeKey: string;
  rawPath?: string;
  headers?: Record<string, string | undefined>;
  pathParameters?: Record<string, string | undefined>;
  queryStringParameters?: Record<string, string | undefined>;
  requestContext: {
    requestId: string;
    http: { method: string; sourceIp?: string; userAgent?: string };
    authorizer?: { jwt?: { claims?: Record<string, unknown> } };
  };
}

/** Same key as the public API's zone pack (ingest-stack ZONE_PACK_KEY). */
export const ZONE_PACK_KEY = 'zones/v1/california.json.gz';

export function toAgencyRequest(event: AgencyHttpEvent, originAllowed: boolean): AgencyRequest {
  const h = event.headers ?? {};
  return {
    routeKey: event.routeKey,
    pathParameters: event.pathParameters ?? {},
    query: event.queryStringParameters ?? {},
    claims: event.requestContext.authorizer?.jwt?.claims,
    requestId: event.requestContext.requestId,
    ...(event.requestContext.http.sourceIp ? { sourceIp: event.requestContext.http.sourceIp } : {}),
    ...(h['x-forwarded-for'] ? { forwardedFor: h['x-forwarded-for'] } : {}),
    ...(h['user-agent'] ? { userAgent: h['user-agent'] } : {}),
    originAllowed,
  };
}

export interface AgencyRuntime {
  api: AgencyApi;
  originGuard?: OriginGuard;
}

/** Thrown when required environment variables are missing (names only, never values). */
export class AgencyMisconfiguredError extends Error {}

/** Reads the environment once per container (first request). */
export function runtimeFromEnv(env: NodeJS.ProcessEnv = process.env): AgencyRuntime {
  const REQUIRED_ENV = [
    'AGENCY_AUDIT_BUCKET',
    'CHECKIN_TOTALS_TABLE',
    'ZONE_PACK_BUCKET',
    'AGENCY_USER_POOL_CLIENT_ID',
    'AGENCY_TOKEN_ISSUER',
  ] as const;
  const missing = REQUIRED_ENV.filter((n) => !env[n]);
  if (missing.length > 0) {
    throw new AgencyMisconfiguredError(`Missing environment variables: ${missing.join(', ')}`);
  }
  const minRaw = env['AGENCY_MIN_CELL_COUNT'];
  const minCellCount =
    minRaw === undefined || minRaw === '' ? DEFAULT_MIN_CELL_COUNT : Number(minRaw);
  if (!Number.isInteger(minCellCount) || minCellCount < 1) {
    throw new AgencyMisconfiguredError('AGENCY_MIN_CELL_COUNT must be a positive integer');
  }
  const requireOrigin = env['REQUIRE_ORIGIN_HEADER'] === 'true';
  const originSecretArn = env['ORIGIN_VERIFY_SECRET_ARN'];
  if (requireOrigin && !originSecretArn) {
    throw new AgencyMisconfiguredError('Missing environment variables: ORIGIN_VERIFY_SECRET_ARN');
  }

  const s3 = new S3Client({});
  const doc = DynamoDBDocumentClient.from(new DynamoDBClient({}));
  const zones = new LayeredZoneProvider([
    new S3ZoneProvider(env['ZONE_PACK_BUCKET'] as string, ZONE_PACK_KEY, s3),
    new NwsZoneProvider(),
  ]);
  const api = createAgencyApi({
    audit: new S3AuditSink(
      s3,
      env['AGENCY_AUDIT_BUCKET'] as string,
      env['AGENCY_AUDIT_PREFIX'] || DEFAULT_AUDIT_PREFIX,
    ),
    totals: new DynamoCheckInTotals(doc, env['CHECKIN_TOTALS_TABLE'] as string),
    areas: new ZoneCountyResolver(zones),
    minCellCount,
    expected: {
      clientId: env['AGENCY_USER_POOL_CLIENT_ID'] as string,
      issuer: env['AGENCY_TOKEN_ISSUER'] as string,
    },
  });
  return {
    api,
    ...(requireOrigin && originSecretArn
      ? { originGuard: secretsManagerOriginGuard(originSecretArn) }
      : {}),
  };
}

const failure = (statusCode: number, code: string): AgencyResponse => ({
  statusCode,
  headers: { 'content-type': 'application/json', 'cache-control': 'no-store' },
  body: JSON.stringify({ code, message: 'Service unavailable' }),
});

export function createAgencyHandler(build: () => AgencyRuntime) {
  let runtime: AgencyRuntime | undefined;
  return async (event: AgencyHttpEvent): Promise<AgencyResponse> => {
    try {
      runtime ??= build();
    } catch (err) {
      // No audit sink without configuration, so nothing can be served: fail closed.
      console.error(
        JSON.stringify({
          event: 'agency_misconfigured',
          detail: err instanceof AgencyMisconfiguredError ? err.message : 'startup failed',
        }),
      );
      return failure(500, 'misconfigured');
    }
    let originAllowed = true;
    if (runtime.originGuard) {
      try {
        originAllowed = await runtime.originGuard.allows(event.headers ?? {});
      } catch {
        // The secret could not be loaded: treat as not verified (the request is denied, audited).
        originAllowed = false;
      }
    }
    return runtime.api(toAgencyRequest(event, originAllowed));
  };
}

export const handler = createAgencyHandler(() => runtimeFromEnv());
