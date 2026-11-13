import { PutObjectCommand } from '@aws-sdk/client-s3';
import { describe, expect, it } from 'vitest';
import { ZoneCountyResolver } from '../../src/agency/areas.js';
import {
  AUDIT_SCHEMA,
  auditKey,
  MemoryAuditSink,
  S3AuditSink,
  type AuditRecord,
} from '../../src/agency/audit.js';
import {
  createAgencyHandler,
  runtimeFromEnv,
  toAgencyRequest,
  type AgencyHttpEvent,
} from '../../src/agency/handler.js';
import {
  isCaliforniaCountyFips,
  parseCountyList,
  resolveRequestedAreas,
} from '../../src/agency/jurisdiction.js';
import { agencyClaims, handler as preToken } from '../../src/agency/pre-token.js';
import { parseGroups } from '../../src/agency/principal.js';
import { createAgencyApi } from '../../src/agency/service.js';
import { DEFAULT_MIN_CELL_COUNT, suppressArea } from '../../src/agency/suppression.js';
import { cellForPoint } from '../../src/geo/index.js';
import {
  claims,
  CLIENT_ID,
  FakeTotals,
  FakeZones,
  ISSUER,
  row,
  SAN_MATEO,
  SANTA_CLARA,
  SC_CELLS,
  SM_CELLS,
  summaryRequest,
} from './helpers.js';

describe('suppressArea', () => {
  const c = (h3Cell: string, checkedIn: number) => ({ h3Cell, checkedIn });

  it('defaults to 10 (pending the privacy-floor decision)', () => {
    expect(DEFAULT_MIN_CELL_COUNT).toBe(10);
  });

  it('shows everything when every cell meets the threshold', () => {
    const r = suppressArea([c('b', 10), c('a', 25)], 10);
    expect(r).toEqual({
      checkedIn: 35,
      suppressed: false,
      cells: [
        { h3Cell: 'a', checkedIn: 25, suppressed: false },
        { h3Cell: 'b', checkedIn: 10, suppressed: false },
      ],
    });
  });

  it('a single small cell pulls in the next-smallest visible cell (complementary)', () => {
    const r = suppressArea([c('a', 50), c('b', 2), c('c', 11), c('d', 30)], 10);
    expect(r.checkedIn).toBe(93);
    expect(r.cells.filter((x) => x.suppressed).map((x) => x.h3Cell)).toEqual(['b', 'c']);
  });

  it('several small cells whose sum reaches the threshold need no complement', () => {
    const r = suppressArea([c('a', 50), c('b', 6), c('c', 7)], 10);
    expect(r.cells.filter((x) => x.suppressed).map((x) => x.h3Cell)).toEqual(['b', 'c']);
  });

  it('never lets total minus visible cells reveal a value below the threshold', () => {
    // Exhaustive over small cases.
    for (let a = 1; a < 25; a += 3) {
      for (let b = 1; b < 25; b += 4) {
        for (let d = 1; d < 25; d += 5) {
          const r = suppressArea([c('a', a), c('b', b), c('d', d)], 10);
          if (r.suppressed) continue;
          const visible = r.cells.reduce((s, x) => s + (x.checkedIn ?? 0), 0);
          const hidden = (r.checkedIn as number) - visible;
          expect(hidden === 0 || hidden >= 10, `${a},${b},${d}`).toBe(true);
        }
      }
    }
  });

  it('an empty area is withheld', () => {
    expect(suppressArea([], 10)).toEqual({ checkedIn: null, suppressed: true, cells: [] });
  });

  it.each([0, -1, 1.5])('refuses threshold %s', (t) => {
    expect(() => suppressArea([], t)).toThrow(/threshold/);
  });
});

describe('jurisdiction helpers', () => {
  it('knows the 58 California counties', () => {
    let n = 0;
    for (let i = 0; i < 1000; i++) {
      if (isCaliforniaCountyFips(`06${String(i).padStart(3, '0')}`)) n++;
    }
    expect(n).toBe(58);
    expect(isCaliforniaCountyFips('06085')).toBe(true);
    expect(isCaliforniaCountyFips('06117')).toBe(false);
    expect(isCaliforniaCountyFips('41085')).toBe(false);
  });

  it('parses, sorts and de-duplicates, or refuses the whole list', () => {
    expect(parseCountyList(' 06085,06081 ,06085')).toEqual(['06081', '06085']);
    expect(parseCountyList('06085,bogus')).toBeNull();
    expect(parseCountyList('')).toEqual([]);
  });

  it('defaults to the whole jurisdiction; narrowing only', () => {
    const j = new Set([SANTA_CLARA, SAN_MATEO]);
    expect(resolveRequestedAreas(undefined, j)).toEqual({
      ok: true,
      areas: [SAN_MATEO, SANTA_CLARA],
    });
    expect(resolveRequestedAreas('06001', j)).toEqual({
      ok: false,
      reason: 'area_outside_jurisdiction',
      requested: ['06001'],
    });
  });
});

describe('parseGroups', () => {
  it.each([
    ['[agency-viewer relay-admin]', ['agency-viewer', 'relay-admin']],
    ['agency-viewer', ['agency-viewer']],
    ['[]', []],
    [
      ['a', 1, 'b'],
      ['a', 'b'],
    ],
    [undefined, []],
  ])('%j', (input, out) => {
    expect(parseGroups(input)).toEqual(out);
  });
});

describe('ZoneCountyResolver', () => {
  it('assigns cells by center to exactly one requested county', async () => {
    const zones = new FakeZones();
    const r = new ZoneCountyResolver(zones);
    const outside = cellForPoint(34.05, -118.25); // Los Angeles
    const m = await r.assign([...SC_CELLS, ...SM_CELLS, outside, 'not-a-cell'], [SANTA_CLARA]);
    expect([...m.keys()].sort()).toEqual([...SC_CELLS].sort());
    expect(new Set(m.values())).toEqual(new Set([SANTA_CLARA]));
    expect(zones.calls).toEqual(['CAC085']);
  });
});

describe('pre-token-generation trigger', () => {
  it('copies only the admin attributes into both tokens', async () => {
    const event = {
      version: '2',
      triggerSource: 'TokenGeneration_Authentication',
      userPoolId: 'p',
      userName: 'u',
      request: {
        userAttributes: {
          'custom:agency_id': 'santa-clara-oes',
          'custom:jurisdiction': '06085',
          email: 'a@example.org',
        },
      },
      response: {},
    };
    const out = await preToken(event);
    const expected = { agency_id: 'santa-clara-oes', jurisdiction: '06085' };
    expect(out.response.claimsAndScopeOverrideDetails).toEqual({
      accessTokenGeneration: { claimsToAddOrOverride: expected },
      idTokenGeneration: { claimsToAddOrOverride: expected },
    });
  });

  it('adds nothing for a user without the attributes', () => {
    expect(agencyClaims({ email: 'x' })).toEqual({});
  });
});

const record = (over: Partial<AuditRecord> = {}): AuditRecord => ({
  schema: AUDIT_SCHEMA,
  time: '2026-11-11T07:08:09.123Z',
  requestId: 'Abc=123/../x',
  action: 'agency.me.read',
  outcome: 'allow',
  reason: null,
  status: 200,
  actor: { sub: 's', agencyId: null, roles: [] },
  resource: { route: 'GET /agency/me', alertId: null, areas: [] },
  source: { ip: null, forwardedFor: null, userAgent: null },
  ...over,
});

describe('S3AuditSink', () => {
  it('writes one time-partitioned object per record with a checksum', async () => {
    const sent: PutObjectCommand[] = [];
    const sink = new S3AuditSink(
      {
        send: (cmd: unknown) => (sent.push(cmd as PutObjectCommand), Promise.resolve({})),
      } as never,
      'audit-bucket',
    );
    await sink.write(record());
    expect(sent).toHaveLength(1);
    const input = sent[0]!.input;
    expect(input.Bucket).toBe('audit-bucket');
    expect(input.Key).toBe('agency-audit/v1/2026/11/11/07/2026-11-11T070809123Z-Abc=123____x.json');
    expect(input.ChecksumAlgorithm).toBe('SHA256');
    expect(JSON.parse(String(input.Body))).toEqual(record());
  });

  it('propagates a failed write (so the request fails closed)', async () => {
    const sink = new S3AuditSink(
      { send: () => Promise.reject(new Error('AccessDenied')) } as never,
      'b',
    );
    await expect(sink.write(record())).rejects.toThrow('AccessDenied');
  });

  it('refuses a record with an invalid time', () => {
    expect(() => auditKey(record({ time: 'nope' }))).toThrow(/invalid time/);
  });
});

describe('totals paging', () => {
  it('stops at maxTotalsPages and reports complete: false', async () => {
    const rows = Array.from({ length: 6 }, (_, i) => row(SC_CELLS[i % 3] as string, 20));
    const totals = new FakeTotals(rows, 2);
    const api = createAgencyApi({
      audit: new MemoryAuditSink(),
      totals,
      areas: new ZoneCountyResolver(new FakeZones()),
      minCellCount: 10,
      expected: { clientId: CLIENT_ID, issuer: ISSUER },
      maxTotalsPages: 2,
      log: () => {},
    });
    const res = await api(summaryRequest());
    expect(totals.calls).toBe(2);
    expect(JSON.parse(res.body).complete).toBe(false);
  });
});

const event = (over: Partial<AgencyHttpEvent> = {}): AgencyHttpEvent => ({
  routeKey: 'GET /agency/me',
  headers: { 'x-forwarded-for': '203.0.113.9', 'user-agent': 'ua' },
  requestContext: {
    requestId: 'rid',
    http: { method: 'GET', sourceIp: '198.51.100.1' },
    authorizer: { jwt: { claims: claims() } },
  },
  ...over,
});

describe('Lambda handler', () => {
  it('maps the API Gateway event, claims from the authorizer only', () => {
    const r = toAgencyRequest(event({ queryStringParameters: { areas: '06085' } }), true);
    expect(r).toMatchObject({
      routeKey: 'GET /agency/me',
      requestId: 'rid',
      sourceIp: '198.51.100.1',
      forwardedFor: '203.0.113.9',
      userAgent: 'ua',
      query: { areas: '06085' },
      originAllowed: true,
    });
    expect(r.claims?.['sub']).toBe('user-sub-1');
  });

  it('fails closed (500) when configuration is missing', async () => {
    const h = createAgencyHandler(() => runtimeFromEnv({}));
    const res = await h(event());
    expect(res.statusCode).toBe(500);
  });

  it('names the missing variables and refuses a bad threshold', () => {
    expect(() => runtimeFromEnv({})).toThrow(/AGENCY_AUDIT_BUCKET/);
    const env = {
      AGENCY_AUDIT_BUCKET: 'b',
      CHECKIN_TOTALS_TABLE: 't',
      ZONE_PACK_BUCKET: 'z',
      AGENCY_USER_POOL_CLIENT_ID: CLIENT_ID,
      AGENCY_TOKEN_ISSUER: ISSUER,
    };
    expect(() => runtimeFromEnv({ ...env, AGENCY_MIN_CELL_COUNT: '0' })).toThrow(/positive/);
    expect(() => runtimeFromEnv({ ...env, REQUIRE_ORIGIN_HEADER: 'true' })).toThrow(
      /ORIGIN_VERIFY_SECRET_ARN/,
    );
    expect(runtimeFromEnv(env).originGuard).toBeUndefined();
  });

  it('treats an origin secret that cannot be loaded as unverified (denied, audited)', async () => {
    const audit = new MemoryAuditSink();
    const api = createAgencyApi({
      audit,
      totals: new FakeTotals(),
      areas: new ZoneCountyResolver(new FakeZones()),
      minCellCount: 10,
      log: () => {},
    });
    const h = createAgencyHandler(() => ({
      api,
      originGuard: { allows: () => Promise.reject(new Error('secrets down')) },
    }));
    const res = await h(event());
    expect(res.statusCode).toBe(403);
    expect(audit.records[0]?.reason).toBe('origin');
  });
});
