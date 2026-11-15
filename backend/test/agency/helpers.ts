import type { Polygon } from 'geojson';
import { cellForPoint } from '../../src/geo/index.js';
import type { ZoneGeometryProvider } from '../../src/geo/zones.js';
import { ZoneCountyResolver } from '../../src/agency/areas.js';
import { MemoryAuditSink } from '../../src/agency/audit.js';
import {
  createAgencyApi,
  type AgencyRequest,
  type TotalsReader,
} from '../../src/agency/service.js';
import type { CellTotal } from '../../src/scale/checkins/types.js';

export const CLIENT_ID = 'agency-client-id';
export const ISSUER = 'https://cognito-idp.us-west-2.amazonaws.com/us-west-2_TEST';
export const ALERT_ID = '3f1c2a4e-8b7d-4c1a-9e2f-0a1b2c3d4e5f';

/** Santa Clara (06085) and San Mateo (06081) as simple non-overlapping boxes, [lng, lat]. */
const box = (w: number, s: number, e: number, n: number): Polygon => ({
  type: 'Polygon',
  coordinates: [
    [
      [w, s],
      [e, s],
      [e, n],
      [w, n],
      [w, s],
    ],
  ],
});
export const SANTA_CLARA = '06085';
export const SAN_MATEO = '06081';
export const ALAMEDA = '06001';
export const ZONES: Record<string, Polygon> = {
  CAC085: box(-122.1, 36.9, -121.2, 37.4),
  CAC081: box(-122.6, 37.4, -122.15, 37.8),
  CAC001: box(-122.15, 37.45, -121.5, 37.9),
};

export class FakeZones implements ZoneGeometryProvider {
  calls: string[] = [];
  getZoneGeometry(ugc: string): Promise<Polygon | null> {
    this.calls.push(ugc);
    return Promise.resolve(ZONES[ugc] ?? null);
  }
}

/** Cells well inside each county box. */
export const SC_CELLS = [
  cellForPoint(37.3, -121.9),
  cellForPoint(37.2, -121.7),
  cellForPoint(37.0, -121.5),
];
export const SM_CELLS = [cellForPoint(37.6, -122.4), cellForPoint(37.5, -122.3)];

export class FakeTotals implements TotalsReader {
  calls = 0;
  constructor(
    public rows: CellTotal[] = [],
    private readonly pageSize = 500,
  ) {}
  listTotals(alertId: string, cursor?: string) {
    this.calls += 1;
    const start = cursor ? Number(cursor) : 0;
    const mine = this.rows.filter((r) => r.alertId === alertId);
    const page = mine.slice(start, start + this.pageSize);
    const next = start + this.pageSize;
    return Promise.resolve({
      totals: page,
      ...(next < mine.length ? { cursor: String(next) } : {}),
    });
  }
}

export interface ClaimsOptions {
  sub?: string;
  groups?: string[] | string;
  agencyId?: string;
  jurisdiction?: string;
  tokenUse?: string;
  clientId?: string;
  issuer?: string;
}

/** Claims as an HTTP API JWT authorizer passes them (arrays flattened to "[a b]"). */
export function claims(o: ClaimsOptions = {}): Record<string, unknown> {
  const groups = o.groups ?? ['agency-viewer'];
  return {
    sub: o.sub ?? 'user-sub-1',
    'cognito:groups': Array.isArray(groups) ? `[${groups.join(' ')}]` : groups,
    token_use: o.tokenUse ?? 'access',
    client_id: o.clientId ?? CLIENT_ID,
    iss: o.issuer ?? ISSUER,
    scope: 'aws.cognito.signin.user.admin',
    ...(o.agencyId === undefined
      ? { agency_id: 'santa-clara-oes' }
      : o.agencyId === ''
        ? {}
        : { agency_id: o.agencyId }),
    ...(o.jurisdiction === undefined
      ? { jurisdiction: SANTA_CLARA }
      : o.jurisdiction === ''
        ? {}
        : { jurisdiction: o.jurisdiction }),
  };
}

export function request(over: Partial<AgencyRequest> = {}): AgencyRequest {
  return {
    routeKey: 'GET /agency/me',
    pathParameters: {},
    query: {},
    claims: claims(),
    requestId: 'req-1',
    sourceIp: '198.51.100.7',
    forwardedFor: '203.0.113.9',
    userAgent: 'test-agent/1.0',
    originAllowed: true,
    ...over,
  };
}

export const summaryRequest = (over: Partial<AgencyRequest> = {}): AgencyRequest =>
  request({
    routeKey: 'GET /agency/alerts/{alertId}/summary',
    pathParameters: { alertId: ALERT_ID },
    ...over,
  });

export function setup(rows: CellTotal[] = [], minCellCount = 10) {
  const audit = new MemoryAuditSink();
  const totals = new FakeTotals(rows);
  const zones = new FakeZones();
  const logs: Record<string, unknown>[] = [];
  const api = createAgencyApi({
    audit,
    totals,
    areas: new ZoneCountyResolver(zones),
    minCellCount,
    expected: { clientId: CLIENT_ID, issuer: ISSUER },
    now: () => new Date('2026-11-11T12:00:00.000Z'),
    log: (l) => logs.push(l),
  });
  return { api, audit, totals, zones, logs };
}

export const row = (h3Cell: string, checkedIn: number, alertId = ALERT_ID): CellTotal => ({
  alertId,
  h3Cell,
  checkedIn,
});
