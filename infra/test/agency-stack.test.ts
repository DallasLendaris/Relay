import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import type { Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';
import {
  AGENCY_GROUPS,
  AGENCY_ROUTES,
  AGENCY_THROTTLE,
  AGENCY_TOKEN_LIFETIMES,
  AUDIT_PROTECTED_ACTIONS,
  DEFAULT_AGENCY_AUDIT_LOCK_DAYS,
  DEFAULT_AGENCY_AUDIT_RETENTION_DAYS,
  auditBreakGlassRoleName,
} from '../lib/agency-stack.js';
import { AGENCY_PATH_PATTERN } from '../lib/edge-stack.js';
import { backendSource, envNamesRead } from './handler-env.js';
import { synthAll, synthApp } from './helpers.js';

vi.setConfig({ testTimeout: 120_000 });

const res = (t: Template, type: string) => Object.entries(t.findResources(type)) as [string, any][];
const one = (t: Template, type: string) => {
  const found = res(t, type);
  expect(found, type).toHaveLength(1);
  return found[0]![1].Properties;
};
const fnByLogicalPrefix = (t: Template, prefix: string) => {
  const f = res(t, 'AWS::Lambda::Function').find(([id]) => id.startsWith(prefix));
  expect(f, prefix).toBeDefined();
  return f![1].Properties;
};

describe.each(['dev', 'prod'] as const)('AgencyStack user pool (%s)', (stage) => {
  const { agency } = synthAll(stage);
  const pool = one(agency, 'AWS::Cognito::UserPool');

  it('admin-created users only (AC-2)', () => {
    expect(pool.AdminCreateUserConfig).toEqual({ AllowAdminCreateUserOnly: true });
  });

  it('MFA required, TOTP only, no SMS (IA-2(1), IA-2(2))', () => {
    expect(pool.MfaConfiguration).toBe('ON');
    expect(pool.EnabledMfas).toEqual(['SOFTWARE_TOKEN_MFA']);
    expect(pool.SmsConfiguration).toBeUndefined();
  });

  it('strong password policy with history (IA-5(1))', () => {
    expect(pool.Policies.PasswordPolicy).toEqual({
      MinimumLength: 14,
      RequireLowercase: true,
      RequireUppercase: true,
      RequireNumbers: true,
      RequireSymbols: true,
      TemporaryPasswordValidityDays: 3,
      PasswordHistorySize: 24,
    });
  });

  it('Plus plan with threat protection: enforced on prod, audit-only on dev (AC-7)', () => {
    expect(pool.UserPoolTier).toBe('PLUS');
    expect(pool.UserPoolAddOns).toEqual({
      AdvancedSecurityMode: stage === 'prod' ? 'ENFORCED' : 'AUDIT',
    });
  });

  it('deletion protection on prod only', () => {
    expect(pool.DeletionProtection).toBe(stage === 'prod' ? 'ACTIVE' : 'INACTIVE');
    const [, r] = res(agency, 'AWS::Cognito::UserPool')[0]!;
    expect(r.DeletionPolicy).toBe(stage === 'prod' ? 'Retain' : 'Delete');
  });

  it('agency id and jurisdiction are custom attributes', () => {
    const names = pool.Schema.map((a: any) => a.Name);
    expect(names).toEqual(expect.arrayContaining(['agency_id', 'jurisdiction']));
  });

  it('pre-token generation V2 trigger copies them into the access token', () => {
    expect(pool.LambdaConfig.PreTokenGenerationConfig.LambdaVersion).toBe('V2_0');
  });

  it('has the three role groups with the backend precedence', () => {
    const groups = res(agency, 'AWS::Cognito::UserPoolGroup').map(([, r]) => [
      r.Properties.GroupName,
      r.Properties.Precedence,
    ]);
    expect(groups.sort()).toEqual(
      [
        ['agency-operator', 10],
        ['agency-viewer', 20],
        ['relay-admin', 0],
      ].sort(),
    );
  });
});

describe('AgencyStack app client (AC-12, IA-2)', () => {
  const { agency } = synthAll('prod');
  const client = one(agency, 'AWS::Cognito::UserPoolClient');

  it('is a public SRP client: no secret, no password or admin flows, no OAuth yet', () => {
    expect(client.GenerateSecret).toBe(false);
    expect(client.ExplicitAuthFlows.sort()).toEqual(
      ['ALLOW_REFRESH_TOKEN_AUTH', 'ALLOW_USER_SRP_AUTH'].sort(),
    );
    expect(client.AllowedOAuthFlowsUserPoolClient).toBe(false);
    expect(client.SupportedIdentityProviders).toEqual(['COGNITO']);
    expect(client.PreventUserExistenceErrors).toBe('ENABLED');
    expect(client.EnableTokenRevocation).toBe(true);
  });

  it('short sessions: 15 min access/ID, 8 h refresh, 3 min to finish sign-in', () => {
    expect(client.AccessTokenValidity).toBe(AGENCY_TOKEN_LIFETIMES.accessMinutes);
    expect(client.IdTokenValidity).toBe(AGENCY_TOKEN_LIFETIMES.idMinutes);
    expect(client.RefreshTokenValidity).toBe(AGENCY_TOKEN_LIFETIMES.refreshHours * 60);
    expect(client.TokenValidityUnits).toEqual({
      AccessToken: 'minutes',
      IdToken: 'minutes',
      RefreshToken: 'minutes',
    });
    expect(client.AuthSessionValidity).toBe(3);
  });

  it('users cannot write their agency, jurisdiction or email; they can read them', () => {
    expect(client.WriteAttributes.sort()).toEqual(['locale', 'zoneinfo']);
    expect(client.ReadAttributes).toEqual(
      expect.arrayContaining(['custom:agency_id', 'custom:jurisdiction']),
    );
  });
});

describe('AgencyStack API and authorizer', () => {
  const { agency } = synthAll('dev');

  it('every route uses the Cognito JWT authorizer for this pool and client', () => {
    const auth = one(agency, 'AWS::ApiGatewayV2::Authorizer');
    expect(auth.AuthorizerType).toBe('JWT');
    expect(auth.IdentitySource).toEqual(['$request.header.Authorization']);
    expect(JSON.stringify(auth.JwtConfiguration.Issuer)).toContain('cognito-idp.us-west-2');
    expect(JSON.stringify(auth.JwtConfiguration.Audience)).toContain('AgencyWebClient');
    const routes = res(agency, 'AWS::ApiGatewayV2::Route').map(([, r]) => r.Properties);
    expect(routes.map((r) => r.RouteKey).sort()).toEqual(
      ['ANY /{proxy+}', 'GET /agency/alerts/{alertId}/summary', 'GET /agency/me'].sort(),
    );
    for (const r of routes) expect(r.AuthorizationType, r.RouteKey).toBe('JWT');
  });

  it('is read-only: no route but GET (and the audited catch-all)', () => {
    expect(AGENCY_ROUTES.every((r) => r.method === 'GET')).toBe(true);
  });

  it('logs every request (incl. authorizer refusals) and throttles at 20 rps', () => {
    const stage = one(agency, 'AWS::ApiGatewayV2::Stage');
    expect(stage.DefaultRouteSettings).toEqual({
      ThrottlingRateLimit: AGENCY_THROTTLE.rateLimit,
      ThrottlingBurstLimit: AGENCY_THROTTLE.burstLimit,
    });
    const format = JSON.parse(stage.AccessLogSettings.Format.replace('$context.status', '0'));
    expect(Object.keys(format)).toEqual(
      expect.arrayContaining(['requestId', 'ip', 'status', 'routeKey', 'authorizerError', 'sub']),
    );
  });

  it('routes match the /agency/* paths of the OpenAPI spec', () => {
    const spec = readFileSync(
      fileURLToPath(new URL('../../packages/shared/openapi/relay.yaml', import.meta.url)),
      'utf8',
    );
    const specPaths = [...spec.matchAll(/^ {2}(\/agency\/[^:]+):$/gm)].map((m) => m[1]);
    expect(specPaths.sort()).toEqual(AGENCY_ROUTES.map((r) => r.path).sort());
  });

  it('groups match the backend roles', () => {
    const src = backendSource('agency/roles.ts');
    for (const g of AGENCY_GROUPS) {
      expect(src).toContain(`'${g.name}': ${g.precedence},`);
    }
  });
});

describe('AgencyStack function permissions (AC-6)', () => {
  const { agency } = synthAll('dev');
  const fn = fnByLogicalPrefix(agency, 'AgencyFunction');
  const policy = res(agency, 'AWS::IAM::Policy').find(([id]) =>
    id.startsWith('AgencyFunctionServiceRoleDefaultPolicy'),
  )![1].Properties.PolicyDocument.Statement as any[];

  it('may only write audit objects, query the totals index, read the zone pack and the origin secret', () => {
    const actions = policy.flatMap((s) => [s.Action].flat()).sort();
    expect(actions).toEqual(
      ['dynamodb:Query', 's3:GetObject', 's3:PutObject', 'secretsmanager:GetSecretValue'].sort(),
    );
    const query = policy.find((s) => s.Action === 'dynamodb:Query');
    expect(JSON.stringify(query.Resource)).toContain('/index/byAlert');
    const put = policy.find((s) => s.Action === 's3:PutObject');
    expect(JSON.stringify(put.Resource)).toContain('/agency-audit/v1/*');
  });

  it('env contract: sets exactly what the handler reads', () => {
    const read = envNamesRead('agency/handler.ts');
    const set = Object.keys(fn.Environment.Variables);
    expect(read.length).toBeGreaterThan(0);
    expect(set).toEqual(expect.arrayContaining(read.filter((n) => n !== 'AGENCY_AUDIT_PREFIX')));
    expect(set.filter((n) => !read.includes(n))).toEqual([]);
  });

  it('the pre-token trigger has no permissions beyond logging', () => {
    const p = res(agency, 'AWS::IAM::Policy').find(([id]) =>
      id.startsWith('AgencyPreTokenFunction'),
    );
    expect(p).toBeUndefined();
  });

  it('origin header is required on prod, not on dev', () => {
    expect(fn.Environment.Variables.REQUIRE_ORIGIN_HEADER).toBe('false');
    const prod = fnByLogicalPrefix(synthAll('prod').agency, 'AgencyFunction');
    expect(prod.Environment.Variables.REQUIRE_ORIGIN_HEADER).toBe('true');
    expect(prod.Environment.Variables.AGENCY_MIN_CELL_COUNT).toBe('10');
  });
});

describe.each(['dev', 'prod'] as const)('AgencyStack audit bucket (%s)', (stage) => {
  const { agency } = synthAll(stage);
  const [, bucket] = res(agency, 'AWS::S3::Bucket').find(([id]) =>
    id.startsWith('AgencyAuditBucket'),
  )!;
  const p = bucket.Properties;

  it('Object Lock governance, versioned, retained, logged (AU-9)', () => {
    expect(p.ObjectLockEnabled).toBe(true);
    expect(p.ObjectLockConfiguration.Rule.DefaultRetention).toEqual({
      Mode: 'GOVERNANCE',
      Days: DEFAULT_AGENCY_AUDIT_LOCK_DAYS[stage],
    });
    expect(p.VersioningConfiguration).toEqual({ Status: 'Enabled' });
    expect(bucket.DeletionPolicy).toBe('Retain');
    expect(p.LoggingConfiguration.LogFilePrefix).toBe('agency-audit/');
  });

  it('retention defaults: 365 days prod, 30 dev (AU-11)', () => {
    expect(p.LifecycleConfiguration.Rules[0]).toMatchObject({
      ExpirationInDays: DEFAULT_AGENCY_AUDIT_RETENTION_DAYS[stage],
      NoncurrentVersionExpiration: { NoncurrentDays: DEFAULT_AGENCY_AUDIT_RETENTION_DAYS[stage] },
    });
  });

  it('SSE-KMS (the logs key, see encryption.test.ts)', () => {
    const rule = p.BucketEncryption.ServerSideEncryptionConfiguration[0];
    expect(rule.ServerSideEncryptionByDefault.SSEAlgorithm).toBe('aws:kms');
  });

  it('policy: TLS only, no tampering except break-glass, writes only by the agency function', () => {
    const [, pol] = res(agency, 'AWS::S3::BucketPolicy').find(([, r]) =>
      JSON.stringify(r.Properties.Bucket).includes('AgencyAuditBucket'),
    )!;
    const st = pol.Properties.PolicyDocument.Statement as any[];
    expect(st.some((s) => s.Condition?.Bool?.['aws:SecureTransport'] === 'false')).toBe(true);
    const tamper = st.find((s) => s.Sid === 'DenyAuditTamperExceptBreakGlass');
    expect(tamper.Effect).toBe('Deny');
    expect(tamper.Action).toEqual([...AUDIT_PROTECTED_ACTIONS]);
    expect(JSON.stringify(tamper.Condition.ArnNotLike)).toContain(auditBreakGlassRoleName(stage));
    const write = st.find((s) => s.Sid === 'DenyAuditWriteExceptAgencyFunction');
    expect(write.Effect).toBe('Deny');
    expect(write.Action).toBe('s3:PutObject');
    expect(JSON.stringify(write.Condition.ArnNotEquals)).toContain('AgencyFunctionServiceRole');
  });
});

describe('AgencyStack audit settings are configurable and validated', () => {
  it('takes retention and lock days from options', () => {
    const { agency } = synthAll('dev', {
      agency: { auditRetentionDays: 90, auditObjectLockDays: 7, minCellCount: 20 },
    });
    const [, b] = res(agency, 'AWS::S3::Bucket').find(([id]) =>
      id.startsWith('AgencyAuditBucket'),
    )!;
    expect(b.Properties.ObjectLockConfiguration.Rule.DefaultRetention.Days).toBe(7);
    expect(b.Properties.LifecycleConfiguration.Rules[0].ExpirationInDays).toBe(90);
    const fn = fnByLogicalPrefix(agency, 'AgencyFunction');
    expect(fn.Environment.Variables.AGENCY_MIN_CELL_COUNT).toBe('20');
  });

  it.each([
    [{ auditRetentionDays: 0 }, /agencyAuditRetentionDays/],
    [{ auditObjectLockDays: 1.5 }, /agencyAuditObjectLockDays/],
    [{ auditRetentionDays: 10, auditObjectLockDays: 20 }, /exceeds/],
    [{ minCellCount: 0 }, /agencyMinCellCount/],
    [{ deniedAlarmThreshold: -1 }, /agencyDeniedAlarmThreshold/],
  ])('refuses %j', (agencyOpts, err) => {
    expect(() => synthApp('dev', { agency: agencyOpts })).toThrow(err);
  });
});

describe('AgencyStack alarms and reported changes (AU-6, SI-4)', () => {
  const { agency } = synthAll('dev');
  const alarms = res(agency, 'AWS::CloudWatch::Alarm').map(([id, r]) => [id, r.Properties]);

  it('denied-request spike and audit-write failure alarm the alerts topic', () => {
    const ids = alarms.map(([id]) => id as string);
    expect(ids.some((i) => i.startsWith('AgencyDeniedRequestsAlarm'))).toBe(true);
    expect(ids.some((i) => i.startsWith('AgencyAuditWriteFailedAlarm'))).toBe(true);
    for (const [id, a] of alarms) {
      expect(JSON.stringify(a.AlarmActions), id as string).toContain('AlertsTopic');
    }
  });

  it('counts 401 and 403 from the access log', () => {
    const filters = res(agency, 'AWS::Logs::MetricFilter').map(([, r]) => r.Properties);
    const denied = filters.find((f) => f.MetricTransformations[0].MetricName.startsWith('Denied'));
    expect(denied.FilterPattern).toContain('$.status = 401');
    expect(denied.FilterPattern).toContain('$.status = 403');
    const failed = filters.find((f) => f.MetricTransformations[0].MetricName.startsWith('Audit'));
    expect(failed.FilterPattern).toContain('agency_audit_write_failed');
    // The backend logs exactly that event name.
    expect(backendSource('agency/service.ts')).toContain("event: 'agency_audit_write_failed'");
  });

  it('reports audit bucket and user pool security changes from CloudTrail', () => {
    const rules = res(agency, 'AWS::Events::Rule').map(([, r]) => r.Properties.EventPattern);
    expect(rules.map((r) => r.source[0]).sort()).toEqual(['aws.cognito-idp', 'aws.s3']);
    const pool = rules.find((r) => r.source[0] === 'aws.cognito-idp');
    expect(pool.detail.eventName).toContain('SetUserPoolMfaConfig');
  });
});

describe('AgencyStack at the edge', () => {
  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: /agency/* goes through the same distribution (WAF) with the origin header`, () => {
      const { edge } = synthAll(stage);
      const dist = one(edge, 'AWS::CloudFront::Distribution').DistributionConfig;
      expect(dist.WebACLId).toBeDefined();
      const b = dist.CacheBehaviors.find((x: any) => x.PathPattern === AGENCY_PATH_PATTERN);
      expect(b, 'agency behavior').toBeDefined();
      expect(b.ViewerProtocolPolicy).toBe('https-only');
      expect(b.AllowedMethods).toContain('GET');
      // Never cached (CachingDisabled managed policy).
      expect(b.CachePolicyId).toBe('4135ea2d-6df8-44a3-9df3-4b5a84be39ad');
      const origin = dist.Origins.find((o: any) => o.Id === b.TargetOriginId);
      expect(origin.OriginCustomHeaders?.[0]?.HeaderName).toBe('x-relay-origin-verify');
      expect(origin.CustomOriginConfig.OriginSSLProtocols).toEqual(['TLSv1.2']);
    });
  }

  it('also wires with an API origin domain (agency still crosses regions)', () => {
    const { stacks } = synthApp('dev', {
      apiOriginDomain: {
        domainName: 'api-origin.example.org',
        certificateArn: 'arn:aws:acm:us-west-2:111111111111:certificate/abc',
      },
    });
    expect(stacks.edge.dependencies).toContain(stacks.agency);
  });

  it('the agency stack sits between api/ingest and edge', () => {
    const { stacks } = synthApp('dev');
    expect(stacks.agency.dependencies).toEqual(
      expect.arrayContaining([stacks.api, stacks.ingest, stacks.monitoring]),
    );
  });
});

describe('Cognito at-rest encryption', () => {
  it('the user pool has no customer-managed key property (AWS-owned key; documented)', () => {
    const pool = one(synthAll('prod').agency, 'AWS::Cognito::UserPool');
    expect(Object.keys(pool).filter((k) => /kms/i.test(k))).toEqual([]);
  });
});
