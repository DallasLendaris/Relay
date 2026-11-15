import { fileURLToPath } from 'node:url';
import SwaggerParser from '@apidevtools/swagger-parser';
import { Ajv2020 } from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { beforeAll, describe, expect, it } from 'vitest';
import { AUDIT_SCHEMA, type AuditRecord } from '../../src/agency/audit.js';
import { AGENCY_ROLES, PERMISSIONS, roleAllows } from '../../src/agency/roles.js';
import {
  ALAMEDA,
  ALERT_ID,
  claims,
  request,
  row,
  SAN_MATEO,
  SANTA_CLARA,
  SC_CELLS,
  setup,
  SM_CELLS,
  summaryRequest,
} from './helpers.js';

const body = (r: { body: string }) => JSON.parse(r.body) as Record<string, any>;

const ROUTES = {
  me: request(),
  summary: summaryRequest(),
} as const;

/** Expected status per role and route: the whole matrix, so a change to roles.ts shows up here. */
const MATRIX: Record<(typeof AGENCY_ROLES)[number], Record<keyof typeof ROUTES, number>> = {
  'agency-viewer': { me: 200, summary: 200 },
  'agency-operator': { me: 200, summary: 200 },
  'relay-admin': { me: 200, summary: 403 },
};

describe('role matrix (AC-3, AC-6)', () => {
  for (const role of AGENCY_ROLES) {
    for (const [route, req] of Object.entries(ROUTES) as [
      keyof typeof ROUTES,
      typeof ROUTES.me,
    ][]) {
      it(`${role} x ${route} -> ${MATRIX[role][route]}`, async () => {
        const { api, audit } = setup([row(SC_CELLS[0] as string, 12)]);
        const res = await api({ ...req, claims: claims({ groups: [role] }) });
        expect(res.statusCode).toBe(MATRIX[role][route]);
        expect(audit.records).toHaveLength(1);
        expect(audit.records[0]?.outcome).toBe(res.statusCode === 200 ? 'allow' : 'deny');
        if (res.statusCode === 403) expect(audit.records[0]?.reason).toBe('role');
      });
    }
  }

  it('a token with no Relay group is denied everywhere', async () => {
    for (const req of Object.values(ROUTES)) {
      const { api, audit } = setup();
      const res = await api({ ...req, claims: claims({ groups: ['some-other-group'] }) });
      expect(res.statusCode).toBe(403);
      expect(audit.records[0]).toMatchObject({ outcome: 'deny', reason: 'no_role' });
    }
  });

  it('unknown actions are denied by default', () => {
    for (const role of AGENCY_ROLES) {
      expect(roleAllows([role], 'agency.alert.send')).toBe(false);
      expect(roleAllows([role], 'constructor')).toBe(false);
    }
    expect(Object.keys(PERMISSIONS).sort()).toEqual([
      'agency.alert.summary.read',
      'agency.me.read',
    ]);
  });

  it('an unknown agency route is a 404 deny, audited', async () => {
    const { api, audit } = setup();
    const res = await api(request({ routeKey: 'ANY /{proxy+}' }));
    expect(res.statusCode).toBe(404);
    expect(audit.records[0]).toMatchObject({
      action: 'agency.unknown',
      outcome: 'deny',
      reason: 'unknown_route',
      resource: { route: 'ANY /{proxy+}' },
    });
  });

  it('POST to a read route is not routed (read-only API)', async () => {
    const { api } = setup();
    const res = await api(request({ routeKey: 'POST /agency/alerts/{alertId}/summary' }));
    expect(res.statusCode).toBe(404);
  });

  it.each(['constructor', '__proto__', 'toString'])('route key %s is not an action', async (k) => {
    const { api, audit } = setup();
    expect((await api(request({ routeKey: k }))).statusCode).toBe(404);
    expect(audit.records[0]?.action).toBe('agency.unknown');
  });
});

describe('token claims are re-checked server side', () => {
  it.each([
    ['ID token', { tokenUse: 'id' }, 'not_access_token'],
    ['other app client', { clientId: 'other' }, 'wrong_client'],
    [
      'other issuer',
      { issuer: 'https://cognito-idp.us-west-2.amazonaws.com/other' },
      'wrong_issuer',
    ],
    ['agency role without agency id', { agencyId: '' }, 'no_agency'],
    ['malformed agency id', { agencyId: 'Bad Agency!' }, 'no_agency'],
    ['malformed jurisdiction', { jurisdiction: '06085,99999' }, 'invalid_jurisdiction'],
    ['non-California county', { jurisdiction: '32003' }, 'invalid_jurisdiction'],
    ['even (non-existent) county code', { jurisdiction: '06084' }, 'invalid_jurisdiction'],
  ] as const)('%s -> 403 %s', async (_label, opts, reason) => {
    const { api, audit } = setup();
    const res = await api(request({ claims: claims(opts) }));
    expect(res.statusCode).toBe(403);
    expect(audit.records[0]).toMatchObject({ outcome: 'deny', reason });
  });

  it('missing claims (no authorizer context) -> 403, audited with a null actor', async () => {
    const { api, audit } = setup();
    const res = await api(request({ claims: undefined }));
    expect(res.statusCode).toBe(403);
    expect(audit.records[0]?.actor).toEqual({ sub: null, agencyId: null, roles: [] });
  });

  it('a request that skipped CloudFront is denied before anything else', async () => {
    const { api, audit, totals } = setup();
    const res = await api(summaryRequest({ originAllowed: false }));
    expect(res.statusCode).toBe(403);
    expect(audit.records[0]).toMatchObject({ outcome: 'deny', reason: 'origin' });
    expect(totals.calls).toBe(0);
  });

  it('accepts groups as a real array too', async () => {
    const { api } = setup();
    const c = claims();
    c['cognito:groups'] = ['agency-viewer'];
    expect((await api(request({ claims: c }))).statusCode).toBe(200);
  });

  it('GET /agency/me returns only server-side identity (no request input echoed)', async () => {
    const { api } = setup();
    const res = await api(
      request({
        claims: claims({ jurisdiction: `${SAN_MATEO},${SANTA_CLARA}` }),
        query: { jurisdiction: '06001' },
      }),
    );
    expect(body(res)).toEqual({
      sub: 'user-sub-1',
      agencyId: 'santa-clara-oes',
      roles: ['agency-viewer'],
      jurisdiction: [SAN_MATEO, SANTA_CLARA],
    });
  });
});

describe('jurisdiction enforcement', () => {
  const rows = [
    row(SC_CELLS[0] as string, 40),
    row(SC_CELLS[1] as string, 30),
    row(SM_CELLS[0] as string, 50),
    row(SM_CELLS[1] as string, 25),
  ];

  it('returns only the counties in the jurisdiction, even with no areas requested', async () => {
    const { api, audit } = setup(rows);
    const res = await api(summaryRequest());
    expect(res.statusCode).toBe(200);
    const b = body(res);
    expect(b['areas'].map((a: any) => a.countyFips)).toEqual([SANTA_CLARA]);
    const cells = b['areas'][0].cells.map((c: any) => c.h3Cell);
    expect(cells.sort()).toEqual([SC_CELLS[0], SC_CELLS[1]].sort());
    expect(JSON.stringify(b)).not.toContain(SM_CELLS[0]);
    expect(audit.records[0]).toMatchObject({
      outcome: 'allow',
      resource: { alertId: ALERT_ID, areas: [SANTA_CLARA] },
    });
  });

  it('a cross-county request is denied as a whole and audited', async () => {
    const { api, audit, totals } = setup(rows);
    const res = await api(summaryRequest({ query: { areas: `${SANTA_CLARA},${SAN_MATEO}` } }));
    expect(res.statusCode).toBe(403);
    expect(body(res)).toEqual({ code: 'area_outside_jurisdiction', message: 'Forbidden' });
    expect(totals.calls).toBe(0);
    expect(audit.records).toHaveLength(1);
    expect(audit.records[0]).toMatchObject({
      action: 'agency.alert.summary.read',
      outcome: 'deny',
      reason: 'area_outside_jurisdiction',
      status: 403,
      actor: { sub: 'user-sub-1', agencyId: 'santa-clara-oes' },
      resource: { alertId: ALERT_ID, areas: [SAN_MATEO, SANTA_CLARA] },
    });
  });

  it('a request may narrow to a subset of its jurisdiction', async () => {
    const { api } = setup(rows);
    const res = await api(
      summaryRequest({
        claims: claims({ jurisdiction: `${SANTA_CLARA},${SAN_MATEO},${ALAMEDA}` }),
        query: { areas: SAN_MATEO },
      }),
    );
    expect(res.statusCode).toBe(200);
    expect(body(res)['areas']).toEqual([
      {
        countyFips: SAN_MATEO,
        checkedIn: 75,
        suppressed: false,
        cells: expect.arrayContaining([
          { h3Cell: SM_CELLS[0], checkedIn: 50, suppressed: false },
          { h3Cell: SM_CELLS[1], checkedIn: 25, suppressed: false },
        ]),
      },
    ]);
  });

  it.each(['abc', '06085;06081', '6085', ',,,'])(
    'malformed areas %j -> 400, audited',
    async (areas) => {
      const { api, audit } = setup(rows);
      const res = await api(summaryRequest({ query: { areas } }));
      expect(res.statusCode).toBe(400);
      expect(audit.records[0]).toMatchObject({ outcome: 'deny', reason: 'invalid_request' });
    },
  );

  it('an agency user with an empty jurisdiction sees nothing', async () => {
    const { api, audit } = setup(rows);
    const res = await api(summaryRequest({ claims: claims({ jurisdiction: '' }) }));
    expect(res.statusCode).toBe(403);
    expect(audit.records[0]?.reason).toBe('no_jurisdiction');
  });

  it('a non-UUID alert id -> 400', async () => {
    const { api } = setup(rows);
    const res = await api(summaryRequest({ pathParameters: { alertId: '../../etc' } }));
    expect(res.statusCode).toBe(400);
  });

  it('a county without a boundary fails closed (503), never unfiltered', async () => {
    const { api, audit } = setup(rows);
    // 06003 (Alpine) is a real county the fake zone data has no boundary for.
    const res = await api(summaryRequest({ claims: claims({ jurisdiction: '06003' }) }));
    expect(res.statusCode).toBe(503);
    expect(audit.records[0]).toMatchObject({ outcome: 'error', reason: 'area_data_unavailable' });
  });

  it('says complete: true when every totals page was read', async () => {
    const many = Array.from({ length: 5 }, (_, i) => row(SC_CELLS[i % 3] as string, 20));
    const { api: full } = setup(many);
    expect(body(await full(summaryRequest()))['complete']).toBe(true);
  });
});

describe('small-cell suppression in responses', () => {
  it('withholds small cells and their complement, default threshold 10', async () => {
    const { api } = setup([
      row(SC_CELLS[0] as string, 40),
      row(SC_CELLS[1] as string, 3),
      row(SC_CELLS[2] as string, 12),
    ]);
    const b = body(await api(summaryRequest()));
    expect(b['minCellCount']).toBe(10);
    const area = b['areas'][0];
    expect(area.checkedIn).toBe(55);
    const byCell = Object.fromEntries(area.cells.map((c: any) => [c.h3Cell, c]));
    expect(byCell[SC_CELLS[1] as string]).toEqual({
      h3Cell: SC_CELLS[1],
      checkedIn: null,
      suppressed: true,
    });
    // 3 alone would be recoverable from 55 - 40 - 12, so the 12 is withheld as well.
    expect(byCell[SC_CELLS[2] as string].suppressed).toBe(true);
    expect(byCell[SC_CELLS[0] as string]).toEqual({
      h3Cell: SC_CELLS[0],
      checkedIn: 40,
      suppressed: false,
    });
    expect(JSON.stringify(b)).not.toMatch(/"checkedIn":3\b/);
  });

  it('withholds a whole area below the threshold', async () => {
    const { api } = setup([row(SC_CELLS[0] as string, 4), row(SC_CELLS[1] as string, 5)]);
    const area = body(await api(summaryRequest()))['areas'][0];
    expect(area.checkedIn).toBeNull();
    expect(area.suppressed).toBe(true);
    expect(area.cells.every((c: any) => c.suppressed && c.checkedIn === null)).toBe(true);
  });

  it('the threshold is configuration', async () => {
    const { api } = setup([row(SC_CELLS[0] as string, 4), row(SC_CELLS[1] as string, 5)], 3);
    const b = body(await api(summaryRequest()));
    expect(b['minCellCount']).toBe(3);
    expect(b['areas'][0].checkedIn).toBe(9);
  });
});

describe('audit record (AU-3)', () => {
  const KEYS = [
    'action',
    'actor',
    'outcome',
    'reason',
    'requestId',
    'resource',
    'schema',
    'source',
    'status',
    'time',
  ];

  it('has exactly the agreed fields and no check-in data', async () => {
    const { api, audit } = setup([row(SC_CELLS[0] as string, 57)]);
    const res = await api(summaryRequest({ requestId: 'abc-123' }));
    expect(res.statusCode).toBe(200);
    const rec = audit.records[0] as AuditRecord;
    expect(Object.keys(rec).sort()).toEqual(KEYS);
    expect(rec).toEqual({
      schema: AUDIT_SCHEMA,
      time: '2026-11-11T12:00:00.000Z',
      requestId: 'abc-123',
      action: 'agency.alert.summary.read',
      outcome: 'allow',
      reason: null,
      status: 200,
      actor: { sub: 'user-sub-1', agencyId: 'santa-clara-oes', roles: ['agency-viewer'] },
      resource: {
        route: 'GET /agency/alerts/{alertId}/summary',
        alertId: ALERT_ID,
        areas: [SANTA_CLARA],
      },
      source: { ip: '198.51.100.7', forwardedFor: '203.0.113.9', userAgent: 'test-agent/1.0' },
    });
    const text = JSON.stringify(rec);
    expect(text).not.toContain('57');
    expect(text).not.toContain(SC_CELLS[0]);
    expect(text).not.toContain('checkedIn');
  });

  it('clips long user agents', async () => {
    const { api, audit } = setup();
    await api(request({ userAgent: 'x'.repeat(5000) }));
    expect(audit.records[0]?.source.userAgent).toHaveLength(512);
  });

  it('one record per request, allowed and denied alike', async () => {
    const { api, audit } = setup();
    await api(request());
    await api(request({ claims: claims({ groups: [] }) }));
    await api(summaryRequest({ query: { areas: SAN_MATEO } }));
    expect(audit.records.map((r) => r.outcome)).toEqual(['allow', 'deny', 'deny']);
  });
});

describe('fail closed when the audit write fails', () => {
  it('an allowed request is refused with 503 and no data', async () => {
    const { api, audit, logs } = setup([row(SC_CELLS[0] as string, 57)]);
    audit.fail = true;
    const res = await api(summaryRequest());
    expect(res.statusCode).toBe(503);
    expect(body(res)).toEqual({ code: 'unavailable', message: 'Service unavailable' });
    expect(res.body).not.toContain(SC_CELLS[0]);
    expect(logs.some((l) => l['event'] === 'agency_audit_write_failed')).toBe(true);
  });

  it('a denied request stays refused', async () => {
    const { api, audit } = setup();
    audit.fail = true;
    const res = await api(summaryRequest({ query: { areas: SAN_MATEO } }));
    expect(res.statusCode).toBe(503);
  });

  it('logs never carry the actor, IP or user agent', async () => {
    const { api, logs } = setup([row(SC_CELLS[0] as string, 57)]);
    await api(summaryRequest());
    const text = JSON.stringify(logs);
    expect(text).not.toContain('user-sub-1');
    expect(text).not.toContain('198.51.100.7');
    expect(text).not.toContain('test-agent');
    expect(logs.at(-1)).toMatchObject({ event: 'agency_request', outcome: 'allow', status: 200 });
  });
});

describe('response headers', () => {
  it('are never cacheable', async () => {
    const { api } = setup();
    expect((await api(request())).headers['cache-control']).toBe('no-store');
  });
});

describe('responses match the OpenAPI contract', () => {
  const SPEC = fileURLToPath(
    new URL('../../../packages/shared/openapi/relay.yaml', import.meta.url),
  );
  let spec: any;
  const ajv = new Ajv2020({ strict: false, allErrors: true });
  addFormats.default(ajv);
  beforeAll(async () => {
    spec = await SwaggerParser.dereference(SPEC);
  });
  const schemaFor = (path: string, status: string) =>
    spec.paths[path].get.responses[status].content['application/json'].schema;
  const check = (schema: object, value: unknown) => {
    const validate = ajv.compile(schema);
    expect(validate(value), JSON.stringify(validate.errors)).toBe(true);
  };

  it('GET /agency/me 200', async () => {
    const { api } = setup();
    check(schemaFor('/agency/me', '200'), body(await api(request())));
  });

  it('GET /agency/alerts/{alertId}/summary 200, with withheld values', async () => {
    const { api } = setup([row(SC_CELLS[0] as string, 40), row(SC_CELLS[1] as string, 3)]);
    check(schemaFor('/agency/alerts/{alertId}/summary', '200'), body(await api(summaryRequest())));
  });

  it.each([
    ['403', { areas: SAN_MATEO }],
    ['400', { areas: 'x' }],
  ] as const)('error %s bodies', async (status, query) => {
    const { api } = setup();
    const res = await api(summaryRequest({ query }));
    expect(String(res.statusCode)).toBe(status);
    check(schemaFor('/agency/alerts/{alertId}/summary', status), body(res));
  });
});
