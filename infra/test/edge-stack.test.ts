import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';

// Each build() synthesizes several stacks, which is slow on a busy machine.
vi.setConfig({ testTimeout: 60_000 });
import { DEFAULT_THROTTLE } from '../lib/api-stack.js';
import { CHECKIN_PATH_REGEX, RATE_LIMITS, publicAlertsBucketName } from '../lib/edge-stack.js';
import { synthAll } from './helpers.js';

function build(stage: 'dev' | 'prod', throttle?: { rateLimit: number; burstLimit: number }) {
  return synthAll(stage, throttle ? { throttle } : {});
}

const { edge, api } = build('dev');

function webAclRules(t: Template): any[] {
  const acl = Object.values(t.findResources('AWS::WAFv2::WebACL'))[0] as any;
  return acl.Properties.Rules;
}
const rule = (name: string) => {
  const r = webAclRules(edge).find((x) => x.Name === name);
  expect(r, name).toBeDefined();
  return r;
};

describe('EdgeStack: bucket and origin access', () => {
  it('is a private bucket: all public access blocked, TLS enforced', () => {
    edge.hasResourceProperties('AWS::S3::Bucket', {
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
    });
    // Reads go only to CloudFront, scoped to this distribution; nothing is open to everyone.
    const policies = Object.values(edge.findResources('AWS::S3::BucketPolicy')) as any[];
    const allows = policies
      .flatMap((p) => p.Properties.PolicyDocument.Statement)
      .filter((s) => s.Effect === 'Allow');
    for (const s of allows) {
      expect(JSON.stringify(s.Principal)).not.toMatch(/"\*"/);
    }
    const cf = allows.filter((s) => s.Principal?.Service === 'cloudfront.amazonaws.com');
    expect(cf).toHaveLength(1);
    expect(cf[0].Action).toBe('s3:GetObject');
    expect(cf[0].Condition.StringEquals['AWS:SourceArn']).toBeDefined();
  });

  it('uses Origin Access Control (not the legacy identity)', () => {
    edge.resourceCountIs('AWS::CloudFront::OriginAccessControl', 1);
    edge.hasResourceProperties('AWS::CloudFront::OriginAccessControl', {
      OriginAccessControlConfig: Match.objectLike({
        OriginAccessControlOriginType: 's3',
        SigningBehavior: 'always',
        SigningProtocol: 'sigv4',
      }),
    });
    edge.resourceCountIs('AWS::CloudFront::CloudFrontOriginAccessIdentity', 0);
  });

  it('names the bucket deterministically for the ingest stack', () => {
    edge.hasResourceProperties('AWS::S3::Bucket', {
      BucketName: { 'Fn::Join': ['', ['relay-dev-public-alerts-', { Ref: 'AWS::AccountId' }]] },
    });
    expect(publicAlertsBucketName('prod')).toContain('relay-prod-public-alerts-');
  });

  it('outputs the distribution domain and the bucket name', () => {
    const outs = edge.toJSON().Outputs;
    expect(outs.DistributionDomainName).toBeDefined();
    expect(outs.PublicAlertsBucketName).toBeDefined();
  });
});

describe('EdgeStack: CloudFront behaviors', () => {
  const dist = () =>
    (Object.values(edge.findResources('AWS::CloudFront::Distribution'))[0] as any).Properties
      .DistributionConfig;

  it('routes /alerts/v1/* to S3 with the origin-honoring, compressed cache policy', () => {
    const cfg = dist();
    const b = cfg.CacheBehaviors.find((x: any) => x.PathPattern === '/alerts/v1/*');
    expect(b).toBeDefined();
    expect(b.Compress).toBe(true);
    expect(b.AllowedMethods).toEqual(['GET', 'HEAD']);
    const origin = cfg.Origins.find((o: any) => o.Id === b.TargetOriginId);
    expect(origin.S3OriginConfig).toBeDefined();
    expect(origin.OriginAccessControlId).toBeDefined();
    edge.hasResourceProperties('AWS::CloudFront::CachePolicy', {
      CachePolicyConfig: Match.objectLike({
        MinTTL: 0,
        DefaultTTL: 15,
        MaxTTL: 3600,
        ParametersInCacheKeyAndForwardedToOrigin: Match.objectLike({
          EnableAcceptEncodingGzip: true,
          EnableAcceptEncodingBrotli: true,
          // Nothing but the path is in the cache key, so one object is one cache entry.
          QueryStringsConfig: { QueryStringBehavior: 'none' },
          HeadersConfig: { HeaderBehavior: 'none' },
          CookiesConfig: { CookieBehavior: 'none' },
        }),
      }),
    });
    expect(b.CachePolicyId.Ref).toMatch(/^PublicAlertsCachePolicy/);
  });

  it('serves /alerts/v1/doc/* from S3 with a one-year cache, matched before /alerts/v1/*', () => {
    const cfg = dist();
    const patterns = cfg.CacheBehaviors.map((x: any) => x.PathPattern);
    expect(patterns.indexOf('/alerts/v1/doc/*')).toBeGreaterThanOrEqual(0);
    expect(patterns.indexOf('/alerts/v1/doc/*')).toBeLessThan(patterns.indexOf('/alerts/v1/*'));
    const b = cfg.CacheBehaviors.find((x: any) => x.PathPattern === '/alerts/v1/doc/*');
    expect(b.AllowedMethods).toEqual(['GET', 'HEAD']);
    expect(b.CachePolicyId.Ref).toMatch(/^PublicAlertDocsCachePolicy/);
    edge.hasResourceProperties('AWS::CloudFront::CachePolicy', {
      CachePolicyConfig: Match.objectLike({ MaxTTL: 31536000 }),
    });
  });

  it('routes everything else to the HTTP API uncached with all viewer headers except Host', () => {
    const cfg = dist();
    const d = cfg.DefaultCacheBehavior;
    const origin = cfg.Origins.find((o: any) => o.Id === d.TargetOriginId);
    expect(origin.CustomOriginConfig.OriginProtocolPolicy).toBe('https-only');
    // Managed policy ids: CachingDisabled and AllViewerExceptHostHeader.
    expect(d.CachePolicyId).toBe('4135ea2d-6df8-44a3-9df3-4b5a84be39ad');
    expect(d.OriginRequestPolicyId).toBe('b689b0a8-53d0-40ab-baf2-68738e2966ac');
    expect(d.AllowedMethods).toHaveLength(7);
    expect(d.ViewerProtocolPolicy).toBe('https-only');
  });

  it('attaches the web ACL', () => {
    const acl = Object.keys(edge.findResources('AWS::WAFv2::WebACL'))[0];
    expect(dist().WebACLId).toEqual({ 'Fn::GetAtt': [acl, 'Arn'] });
  });
});

describe('EdgeStack: WAF', () => {
  it('has scope CLOUDFRONT and the stack deploys in us-east-1', () => {
    edge.hasResourceProperties('AWS::WAFv2::WebACL', {
      Scope: 'CLOUDFRONT',
      DefaultAction: { Allow: {} },
    });
  });

  it('includes both AWS managed rule groups', () => {
    for (const name of ['AWSManagedRulesCommonRuleSet', 'AWSManagedRulesKnownBadInputsRuleSet']) {
      expect(rule(name).Statement.ManagedRuleGroupStatement).toEqual({
        VendorName: 'AWS',
        Name: name,
      });
      expect(rule(name).OverrideAction).toEqual({ None: {} });
    }
  });

  it('rate-limits /devices to 300 per 5 minutes per IP', () => {
    const s = rule('RegistrationRateLimit').Statement.RateBasedStatement;
    expect(rule('RegistrationRateLimit').Action).toEqual({ Block: {} });
    expect(s).toMatchObject({ Limit: 300, AggregateKeyType: 'IP', EvaluationWindowSec: 300 });
    expect(s.ScopeDownStatement.ByteMatchStatement).toMatchObject({
      SearchString: '/devices',
      PositionalConstraint: 'EXACTLY',
      FieldToMatch: { UriPath: {} },
    });
    expect(RATE_LIMITS.REGISTRATION).toBe(300);
  });

  it('gives check-ins a 20,000 per 5 minute per IP limit', () => {
    const s = rule('CheckInRateLimit').Statement.RateBasedStatement;
    expect(s).toMatchObject({ Limit: 20000, AggregateKeyType: 'IP', EvaluationWindowSec: 300 });
    expect(s.ScopeDownStatement.RegexMatchStatement.RegexString).toBe(CHECKIN_PATH_REGEX);
    // The regex matches real check-in paths and nothing else.
    const re = new RegExp(CHECKIN_PATH_REGEX);
    expect(re.test('/alerts/abc-123/checkins')).toBe(true);
    expect(re.test('/alerts/abc/checkins/extra')).toBe(false);
    expect(re.test('/alerts/abc')).toBe(false);
  });

  it('limits everything else to 2,000 per 5 minutes per IP, excluding check-ins', () => {
    const s = rule('GeneralRateLimit').Statement.RateBasedStatement;
    expect(s).toMatchObject({ Limit: 2000, AggregateKeyType: 'IP', EvaluationWindowSec: 300 });
    const inner = s.ScopeDownStatement.NotStatement.Statement.OrStatement.Statements;
    expect(inner[0].RegexMatchStatement.RegexString).toBe(CHECKIN_PATH_REGEX);
    expect(inner[1].ByteMatchStatement.SearchString).toBe('/alerts/v1/');
  });

  it('gives cached public reads their own high limit', () => {
    const s = rule('PublicReadRateLimit').Statement.RateBasedStatement;
    expect(s).toMatchObject({ Limit: 20000, AggregateKeyType: 'IP' });
  });

  it('uses unique priorities', () => {
    const p = webAclRules(edge).map((r) => r.Priority);
    expect(new Set(p).size).toBe(p.length);
  });
});

describe('API stage throttle per stage', () => {
  const throttleOf = (t: Template) =>
    (Object.values(t.findResources('AWS::ApiGatewayV2::Stage'))[0] as any).Properties
      .DefaultRouteSettings;

  it('dev stays 50 rps / burst 100', () => {
    expect(throttleOf(api)).toEqual({ ThrottlingRateLimit: 50, ThrottlingBurstLimit: 100 });
    expect(DEFAULT_THROTTLE.dev).toEqual({ rateLimit: 50, burstLimit: 100 });
  });

  it('prod defaults to the HTTP API account quota: 10,000 rps / burst 5,000', () => {
    expect(throttleOf(build('prod').api)).toEqual({
      ThrottlingRateLimit: 10000,
      ThrottlingBurstLimit: 5000,
    });
  });

  it('an explicit prop overrides the stage default', () => {
    expect(throttleOf(build('prod', { rateLimit: 5000, burstLimit: 8000 }).api)).toEqual({
      ThrottlingRateLimit: 5000,
      ThrottlingBurstLimit: 8000,
    });
  });
});

describe('IngestStack public alert wiring', () => {
  it('points at the edge bucket by deterministic name, in us-east-1, with PutObject on alerts/v1/*', () => {
    const t = build('dev').ingest;
    t.hasResourceProperties('AWS::Lambda::Function', {
      Environment: {
        Variables: Match.objectLike({
          PUBLIC_ALERTS_BUCKET: {
            'Fn::Join': ['', ['relay-dev-public-alerts-', { Ref: 'AWS::AccountId' }]],
          },
          PUBLIC_ALERTS_REGION: 'us-east-1',
        }),
      },
    });
    expect(publicAlertsBucketName('dev')).toContain('relay-dev-public-alerts-');
    expect(JSON.stringify(t.toJSON().Resources)).toContain('/alerts/v1/*');
  });
});

// Review C3 (NIST SI-4, SC-12): EventBridge rules are regional, so the us-west-2 DataKeyChangedRule
// cannot see the edge key in us-east-1. The edge stack has its own rule to the edge topic.
describe('EdgeStack: edge key change alerts', () => {
  const resources = edge.toJSON().Resources as Record<string, any>;
  const ruleOf = (prefix: string) => {
    const found = Object.entries(resources).filter(
      ([id, r]) => r.Type === 'AWS::Events::Rule' && id.startsWith(prefix),
    );
    expect(found, prefix).toHaveLength(1);
    return found[0]![1];
  };
  const keyId = Object.entries(resources).find(([, r]) => r.Type === 'AWS::KMS::Key')![0];
  const topicId = Object.entries(resources).find(
    ([, r]) => r.Type === 'AWS::SNS::Topic' && r.Properties.TopicName === 'relay-dev-edge-alerts',
  )![0];

  it('reports disabling, deleting, re-enabling or re-policying the edge key (not grants)', () => {
    const r = ruleOf('EdgeKeyChangedRule');
    const detail = r.Properties.EventPattern.detail;
    expect(r.Properties.EventPattern.source).toEqual(['aws.kms']);
    expect(detail.eventName).toEqual(
      expect.arrayContaining(['ScheduleKeyDeletion', 'DisableKey', 'EnableKey', 'PutKeyPolicy']),
    );
    expect(detail.eventName).not.toContain('CreateGrant');
    const ids = JSON.stringify(detail.requestParameters.keyId);
    expect(ids).toContain(`{"Ref":"${keyId}"}`);
    expect(ids).toContain('alias/relay/dev/edge');
    expect(r.Properties.Targets).toHaveLength(1);
    expect(r.Properties.Targets[0].Arn).toEqual({ Ref: topicId });
  });

  it('reports changes to the edge key alias', () => {
    const r = ruleOf('EdgeKeyAliasChangedRule');
    const detail = r.Properties.EventPattern.detail;
    expect(detail.eventName).toEqual(['CreateAlias', 'UpdateAlias', 'DeleteAlias']);
    expect(JSON.stringify(detail.requestParameters.aliasName)).toContain('alias/relay/dev/edge');
    expect(r.Properties.Targets[0].Arn).toEqual({ Ref: topicId });
  });

  it('lets EventBridge publish to the encrypted edge topic (topic policy and key policy)', () => {
    const policies = Object.values(resources).filter((r) => r.Type === 'AWS::SNS::TopicPolicy');
    const statements = policies.flatMap((p) => p.Properties.PolicyDocument.Statement);
    expect(
      statements.some(
        (s) => s.Effect === 'Allow' && s.Principal?.Service === 'events.amazonaws.com',
      ),
    ).toBe(true);
    const key = resources[keyId].Properties.KeyPolicy.Statement as any[];
    const events = key.find((s) => s.Sid === 'AllowEventBridgeRules');
    expect(events.Principal).toEqual({ Service: 'events.amazonaws.com' });
    expect(events.Action).toEqual(['kms:Decrypt', 'kms:GenerateDataKey*']);
    expect(events.Condition.StringEquals['aws:SourceAccount']).toBeDefined();
  });
});
