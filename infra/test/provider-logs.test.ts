import { App, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { describe, expect, it, vi } from 'vitest';
import { DEFAULT_LOG_RETENTION_DAYS } from '../lib/monitoring-stack.js';
import { CROSS_REGION_WRITER, declareProviderLogs } from '../lib/provider-logs.js';
import { synthApp } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

// NIST AU-11 / SC-28 (review-2026-11 A5, C6): CDK's provider Lambdas (VPC default-SG cleanup,
// dev bucket auto-delete, cross-region exports) used to create their log groups at run time,
// with no retention and no customer-managed key.

type Res = { Type: string; Properties?: any; DeletionPolicy?: string };

function lambdaLogGroups(templates: Record<string, Template>) {
  const out: Array<{ stack: string; fn: string; group: Res | undefined }> = [];
  for (const [stack, t] of Object.entries(templates)) {
    const resources = t.toJSON().Resources as Record<string, Res>;
    for (const [id, r] of Object.entries(resources)) {
      if (r.Type !== 'AWS::Lambda::Function') continue;
      const ref = r.Properties?.LoggingConfig?.LogGroup?.Ref;
      out.push({ stack, fn: id, group: ref ? resources[ref] : undefined });
    }
  }
  return out;
}

describe.each(['dev', 'prod'] as const)('provider Lambda log groups (%s)', (stage) => {
  const { templates } = synthApp(stage);
  const all = lambdaLogGroups(templates);
  const providers = all.filter((x) => x.fn.startsWith('Custom'));

  it('finds the CDK provider Lambdas of this stage', () => {
    const names = providers.map((p) => `${p.stack}/${p.fn.replace(/[0-9A-F]{8}$/, '')}`).sort();
    const expected = [
      'api/CustomCrossRegionExportWriterCustomResourceProviderHandler',
      'data/CustomVpcRestrictDefaultSGCustomResourceProviderHandler',
      'edge/CustomCrossRegionExportReaderCustomResourceProviderHandler',
      'sender/CustomVpcRestrictDefaultSGCustomResourceProviderHandler',
      // Bucket auto-delete exists on dev only.
      ...(stage === 'dev'
        ? [
            'edge/CustomS3AutoDeleteObjectsCustomResourceProviderHandler',
            'ingest/CustomS3AutoDeleteObjectsCustomResourceProviderHandler',
            'monitoring/CustomS3AutoDeleteObjectsCustomResourceProviderHandler',
          ]
        : []),
    ].sort();
    expect(names).toEqual(expected);
  });

  it('every Lambda, CDK providers included, logs to a declared group with retention and a CMK', () => {
    for (const { stack, fn, group } of all) {
      expect(group?.Type, `${stack}/${fn}`).toBe('AWS::Logs::LogGroup');
      expect(group?.Properties.RetentionInDays, `${stack}/${fn}`).toBe(
        DEFAULT_LOG_RETENTION_DAYS[stage],
      );
      expect(group?.Properties.KmsKeyId, `${stack}/${fn}`).toBeDefined();
    }
  });

  it('keeps provider logs on prod and deletes them with a dev stack', () => {
    for (const { group } of providers) {
      expect(group?.DeletionPolicy).toBe(stage === 'prod' ? 'Retain' : 'Delete');
    }
  });
});

describe('provider log groups with an API origin domain', () => {
  it('declares no export-provider groups when nothing crosses regions', () => {
    const { templates } = synthApp('prod', {
      apiOriginDomain: {
        domainName: 'api-origin.example.org',
        certificateArn: 'arn:aws:acm:us-west-2:111111111111:certificate/abc',
      },
    });
    const ids = Object.keys(templates.api.toJSON().Resources).concat(
      Object.keys(templates.edge.toJSON().Resources),
    );
    expect(ids.filter((id) => id.includes('CrossRegion'))).toEqual([]);
  });
});

describe('declareProviderLogs keeps itself honest', () => {
  const stack = () => new Stack(new App(), 'S', { env: { account: '1', region: 'us-west-2' } });

  it('fails synth when a provider Lambda appears with no declared log group', () => {
    const s = stack();
    declareProviderLogs(s, { isProd: false });
    // Created after the call, like the cross-region providers: nothing declared it.
    new Bucket(s, 'B', { removalPolicy: RemovalPolicy.DESTROY, autoDeleteObjects: true });
    expect(() => Template.fromStack(s)).toThrow(/without a declared log group/);
  });

  it('fails synth when a declared late provider never appears', () => {
    const s = stack();
    declareProviderLogs(s, { isProd: false, late: [CROSS_REGION_WRITER] });
    expect(() => Template.fromStack(s)).toThrow(/never creates/);
  });

  it('attaches providers that exist at call time', () => {
    const s = stack();
    new Bucket(s, 'B', { removalPolicy: RemovalPolicy.DESTROY, autoDeleteObjects: true });
    declareProviderLogs(s, { isProd: false });
    const t = Template.fromStack(s);
    t.resourceCountIs('AWS::Logs::LogGroup', 1);
    t.hasResourceProperties('AWS::Lambda::Function', {
      LoggingConfig: { LogGroup: { Ref: Object.keys(t.findResources('AWS::Logs::LogGroup'))[0] } },
    });
  });
});
