import type { TotalsPage } from '../scale/checkins/totals.js';
import { AreaDataUnavailableError, type CellAreaResolver } from './areas.js';
import {
  AUDIT_SCHEMA,
  clip,
  type AuditOutcome,
  type AuditRecord,
  type AuditSink,
} from './audit.js';
import { resolveRequestedAreas } from './jurisdiction.js';
import { principalFromClaims, type AgencyPrincipal, type ExpectedToken } from './principal.js';
import { roleAllows, type AgencyAction } from './roles.js';
import { suppressArea, type CellCount, type ReportedArea } from './suppression.js';

/**
 * The agency API (read-only). Every request goes through one pipeline:
 *
 *   route -> origin header -> token claims -> role -> request scope (jurisdiction) -> data
 *         -> audit record (always) -> response
 *
 * Each step can only refuse. Nothing is served unless every step allowed it AND the audit record
 * was stored. There is deliberately no way to send or change an alert here: that belongs to the
 * statewide phase and will get its own actions in roles.ts.
 */

/** A request as the Lambda handler normalizes it from the API Gateway event. */
export interface AgencyRequest {
  /** "GET /agency/me", "GET /agency/alerts/{alertId}/summary", or "$default"/proxy for others. */
  routeKey: string;
  pathParameters: Record<string, string | undefined>;
  query: Record<string, string | undefined>;
  /** JWT claims verified by the API Gateway authorizer. */
  claims: Record<string, unknown> | undefined;
  requestId: string;
  sourceIp?: string;
  forwardedFor?: string;
  userAgent?: string;
  /** False when the request lacks CloudFront's origin-verify header (it skipped the WAF). */
  originAllowed: boolean;
}

export interface AgencyResponse {
  statusCode: number;
  headers: Record<string, string>;
  body: string;
}

export interface TotalsReader {
  listTotals(alertId: string, cursor?: string): Promise<TotalsPage>;
}

export interface AgencyApiDeps {
  audit: AuditSink;
  totals: TotalsReader;
  areas: CellAreaResolver;
  /** Small-cell threshold (AGENCY_MIN_CELL_COUNT; default 10). */
  minCellCount: number;
  expected?: ExpectedToken;
  now?: () => Date;
  /** Most totals pages read for one summary (500 cells each). Beyond it the reply says complete: false. */
  maxTotalsPages?: number;
  /** One JSON line per request; never the actor, IP or any data. Defaults to console.log. */
  log?: (line: Record<string, unknown>) => void;
}

export const ROUTE_ME = 'GET /agency/me';
export const ROUTE_SUMMARY = 'GET /agency/alerts/{alertId}/summary';
export const DEFAULT_MAX_TOTALS_PAGES = 100;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const ACTIONS: Record<string, AgencyAction> = {
  [ROUTE_ME]: 'agency.me.read',
  [ROUTE_SUMMARY]: 'agency.alert.summary.read',
};

interface Outcome {
  status: number;
  outcome: AuditOutcome;
  reason: string | null;
  body: unknown;
}

const deny = (status: number, reason: string, message: string): Outcome => ({
  status,
  outcome: 'deny',
  reason,
  body: { code: reason, message },
});

export interface AgencySummary {
  alertId: string;
  asOf: string;
  minCellCount: number;
  /** False when the alert had more cells than one summary reads (maxTotalsPages). */
  complete: boolean;
  areas: Array<{ countyFips: string } & ReportedArea>;
}

export function createAgencyApi(deps: AgencyApiDeps) {
  const now = deps.now ?? (() => new Date());
  const maxPages = deps.maxTotalsPages ?? DEFAULT_MAX_TOTALS_PAGES;
  const log = deps.log ?? ((line) => console.log(JSON.stringify(line)));

  async function summarize(alertId: string, areas: string[]): Promise<AgencySummary> {
    const cells: CellCount[] = [];
    let cursor: string | undefined;
    let pages = 0;
    do {
      const page = await deps.totals.listTotals(alertId, cursor);
      for (const t of page.totals) cells.push({ h3Cell: t.h3Cell, checkedIn: t.checkedIn });
      cursor = page.cursor;
      pages += 1;
    } while (cursor && pages < maxPages);

    const assigned = await deps.areas.assign(
      cells.map((c) => c.h3Cell),
      areas,
    );
    const byArea = new Map<string, CellCount[]>(areas.map((a) => [a, []]));
    for (const c of cells) {
      const county = assigned.get(c.h3Cell);
      // Cells outside every requested county are dropped here and never leave the function.
      if (county) byArea.get(county)?.push(c);
    }
    return {
      alertId,
      asOf: now().toISOString(),
      minCellCount: deps.minCellCount,
      complete: cursor === undefined,
      areas: areas.map((countyFips) => ({
        countyFips,
        ...suppressArea(byArea.get(countyFips) ?? [], deps.minCellCount),
      })),
    };
  }

  async function decide(
    req: AgencyRequest,
    action: AgencyAction | undefined,
    principal: AgencyPrincipal | undefined,
    scope: { alertId: string | null; areas: string[] },
  ): Promise<Outcome> {
    if (!action) return deny(404, 'unknown_route', 'Not found');
    if (!principal) return deny(403, 'forbidden', 'Forbidden');
    if (!roleAllows(principal.roles, action)) return deny(403, 'role', 'Forbidden');

    if (action === 'agency.me.read') {
      return {
        status: 200,
        outcome: 'allow',
        reason: null,
        body: {
          sub: principal.sub,
          agencyId: principal.agencyId,
          roles: principal.roles,
          jurisdiction: [...principal.jurisdiction].sort(),
        },
      };
    }

    // agency.alert.summary.read
    const alertId = req.pathParameters['alertId'] ?? '';
    if (!UUID.test(alertId)) return deny(400, 'invalid_request', 'alertId must be a UUID');
    scope.alertId = alertId.toLowerCase();
    if (principal.jurisdiction.size === 0) return deny(403, 'no_jurisdiction', 'Forbidden');
    const check = resolveRequestedAreas(req.query['areas'], principal.jurisdiction);
    if (!check.ok) {
      if (check.reason === 'invalid_areas') {
        return deny(400, 'invalid_request', 'areas must be California county FIPS codes');
      }
      scope.areas = check.requested;
      return deny(403, 'area_outside_jurisdiction', 'Forbidden');
    }
    scope.areas = check.areas;
    try {
      const body = await summarize(scope.alertId, check.areas);
      return { status: 200, outcome: 'allow', reason: null, body };
    } catch (err) {
      if (err instanceof AreaDataUnavailableError) {
        return {
          status: 503,
          outcome: 'error',
          reason: 'area_data_unavailable',
          body: { code: 'unavailable', message: 'Service unavailable' },
        };
      }
      throw err;
    }
  }

  return async function handle(req: AgencyRequest): Promise<AgencyResponse> {
    const started = Date.now();
    const action = Object.hasOwn(ACTIONS, req.routeKey) ? ACTIONS[req.routeKey] : undefined;
    const scope: { alertId: string | null; areas: string[] } = { alertId: null, areas: [] };
    const parsed = principalFromClaims(req.claims, deps.expected);
    const principal = parsed.ok ? parsed.principal : undefined;

    let result: Outcome;
    if (!req.originAllowed) {
      result = deny(403, 'origin', 'Forbidden');
    } else if (action && !parsed.ok) {
      result = deny(403, parsed.reason, 'Forbidden');
    } else {
      try {
        result = await decide(req, action, principal, scope);
      } catch (err) {
        log({
          event: 'agency_error',
          error: err instanceof Error ? err.name : 'UnknownError',
          requestId: req.requestId,
        });
        result = {
          status: 500,
          outcome: 'error',
          reason: 'internal_error',
          body: { code: 'internal_error', message: 'Internal error' },
        };
      }
    }

    const actor = parsed.ok
      ? {
          sub: parsed.principal.sub,
          agencyId: parsed.principal.agencyId,
          roles: parsed.principal.roles as string[],
        }
      : parsed.partial;
    const record: AuditRecord = {
      schema: AUDIT_SCHEMA,
      time: now().toISOString(),
      requestId: req.requestId,
      action: action ?? 'agency.unknown',
      outcome: result.outcome,
      reason: result.reason,
      status: result.status,
      actor: {
        sub: clip(actor.sub),
        agencyId: clip(actor.agencyId),
        roles: actor.roles.slice(0, 20).map((r) => r.slice(0, 64)),
      },
      resource: { route: req.routeKey.slice(0, 128), alertId: scope.alertId, areas: scope.areas },
      source: {
        ip: clip(req.sourceIp),
        forwardedFor: clip(req.forwardedFor),
        userAgent: clip(req.userAgent),
      },
    };

    try {
      await deps.audit.write(record);
    } catch (err) {
      // Fail closed: without its audit record, nothing is served (allowed requests included).
      // The metric filter on this line raises AgencyAuditWriteFailedAlarm.
      log({
        event: 'agency_audit_write_failed',
        error: err instanceof Error ? err.name : 'UnknownError',
        requestId: req.requestId,
      });
      result = {
        status: 503,
        outcome: 'error',
        reason: 'audit_unavailable',
        body: { code: 'unavailable', message: 'Service unavailable' },
      };
    }

    log({
      event: 'agency_request',
      action: record.action,
      outcome: result.outcome,
      reason: result.reason,
      status: result.status,
      requestId: req.requestId,
      ms: Date.now() - started,
    });
    return {
      statusCode: result.status,
      headers: { 'content-type': 'application/json', 'cache-control': 'no-store' },
      body: JSON.stringify(result.body),
    };
  };
}

export type AgencyApi = ReturnType<typeof createAgencyApi>;
