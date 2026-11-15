import { Annotations, Match } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';
import { synthApp } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

// NIST SC-7 / SC-7(3) (review-2026-11 / SEC-C-2): the API's default execute-api endpoint lets a
// caller skip CloudFront and the WAF. It can only be turned off when CloudFront reaches the API
// by a custom domain, so that is behind -c apiOriginDomainName / -c apiOriginCertificateArn.

const ORIGIN = {
  domainName: 'api-origin.example.org',
  certificateArn: 'arn:aws:acm:us-west-2:111111111111:certificate/abc',
};

const apiOrigin = (edge: ReturnType<typeof synthApp>['templates']['edge']) => {
  const dist = Object.values(edge.findResources('AWS::CloudFront::Distribution'))[0] as any;
  const cfg = dist.Properties.DistributionConfig;
  // The public API is the default behavior; /agency/* is a second API origin (b2-agency).
  const api = (cfg.Origins as any[]).filter(
    (o) => o.CustomOriginConfig && o.Id === cfg.DefaultCacheBehavior.TargetOriginId,
  );
  expect(api).toHaveLength(1);
  return api[0];
};

describe('API origin without a custom domain (default)', () => {
  const { templates, stacks } = synthApp('dev');

  it('keeps the default endpoint (CloudFront needs it)', () => {
    const apis = Object.values(templates.api.findResources('AWS::ApiGatewayV2::Api')) as any[];
    expect(apis).toHaveLength(1);
    expect(apis[0].Properties.DisableExecuteApiEndpoint).toBe(false);
    templates.api.resourceCountIs('AWS::ApiGatewayV2::DomainName', 0);
  });

  it('gives CloudFront the bare host name, not the https:// URL of ApiEndpoint', () => {
    // CloudFront rejects an origin domain with a scheme. ApiEndpoint crosses regions through the
    // export reader and is split in the edge stack.
    const domain = apiOrigin(templates.edge).DomainName;
    expect(domain['Fn::Select'][0]).toBe(2);
    const [sep, value] = domain['Fn::Select'][1]['Fn::Split'];
    expect(sep).toBe('/');
    expect(value['Fn::GetAtt'][1]).toMatch(/HttpApi[0-9A-F]{8}ApiEndpoint/);
    expect(stacks.api.apiDomainName).toBeDefined();
  });

  it('warns on prod that the WAF can be bypassed', () => {
    const prod = synthApp('prod');
    Annotations.fromStack(prod.stacks.api).hasWarning(
      '*',
      Match.stringLikeRegexp('default execute-api endpoint'),
    );
  });
});

describe('API origin with -c apiOriginDomainName', () => {
  const { templates, stacks } = synthApp('prod', { apiOriginDomain: ORIGIN });

  it('disables the default execute-api endpoint', () => {
    templates.api.hasResourceProperties('AWS::ApiGatewayV2::Api', {
      DisableExecuteApiEndpoint: true,
    });
  });

  it('serves the API on the custom domain with TLS 1.2 and maps the $default stage', () => {
    templates.api.hasResourceProperties('AWS::ApiGatewayV2::DomainName', {
      DomainName: ORIGIN.domainName,
      DomainNameConfigurations: [
        Match.objectLike({
          CertificateArn: ORIGIN.certificateArn,
          EndpointType: 'REGIONAL',
          SecurityPolicy: 'TLS_1_2',
        }),
      ],
    });
    templates.api.hasResourceProperties('AWS::ApiGatewayV2::ApiMapping', {
      DomainName: Match.anyValue(),
      Stage: Match.anyValue(),
    });
    templates.api.hasOutput('ApiOriginTarget', {
      Value: { 'Fn::GetAtt': [Match.anyValue(), 'RegionalDomainName'] },
    });
  });

  it('points CloudFront at the custom domain, still with the origin-verify header', () => {
    const origin = apiOrigin(templates.edge);
    expect(origin.DomainName).toBe(ORIGIN.domainName);
    expect(origin.OriginCustomHeaders[0].HeaderName).toBe('x-relay-origin-verify');
    // No cross-region reference is left for the API host name.
    templates.api.resourceCountIs('Custom::CrossRegionExportWriter', 0);
  });

  it('does not warn about the default endpoint', () => {
    const warnings = Annotations.fromStack(stacks.api).findWarning(
      '*',
      Match.stringLikeRegexp('default execute-api endpoint'),
    );
    expect(warnings).toHaveLength(0);
  });
});
