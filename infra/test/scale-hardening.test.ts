import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';
import { synthAll, synthApp } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

const dev = synthAll('dev');
const prod = synthAll('prod');

type Fn = { Properties: Record<string, any> };
const fns = (t: Template) => Object.values(t.findResources('AWS::Lambda::Function')) as Fn[];
const fnWithEnv = (t: Template, name: string): Fn => {
  const f = fns(t).find((x) => x.Properties['Environment']?.Variables?.[name] !== undefined);
  expect(f, name).toBeDefined();
  return f as Fn;
};
const statements = (t: Template): any[] =>
  Object.values(t.findResources('AWS::IAM::Policy')).flatMap(
    (p: any) => p.Properties.PolicyDocument.Statement,
  );
const actions = (t: Template): string[] => statements(t).flatMap((s) => [s.Action].flat());

describe('origin verification (WAF bypass)', () => {
  it('config generates a secret and replicates it to the edge region', () => {
    for (const [stage, t] of [
      ['dev', dev.config],
      ['prod', prod.config],
    ] as const) {
      t.hasResourceProperties('AWS::SecretsManager::Secret', {
        Name: `relay/${stage}/origin-verify`,
        GenerateSecretString: Match.objectLike({ PasswordLength: 48, ExcludePunctuation: true }),
        ReplicaRegions: [{ Region: 'us-east-1' }],
      });
    }
  });

  it('CloudFront adds x-relay-origin-verify to the API origin from a deploy-time dynamic reference', () => {
    for (const [stage, t] of [
      ['dev', dev.edge],
      ['prod', prod.edge],
    ] as const) {
      const dist = Object.values(t.findResources('AWS::CloudFront::Distribution'))[0] as any;
      const origins = dist.Properties.DistributionConfig.Origins as any[];
      const apiOrigin = origins.find((o) => o.CustomOriginConfig);
      expect(apiOrigin.OriginCustomHeaders).toEqual([
        {
          HeaderName: 'x-relay-origin-verify',
          HeaderValue: `{{resolve:secretsmanager:relay/${stage}/origin-verify:SecretString:::}}`,
        },
      ]);
      // The S3 origin must not get the header.
      expect(origins.find((o) => o.S3OriginConfig)?.OriginCustomHeaders).toBeUndefined();
    }
  });

  it('the API requires the header on prod only, and can read the secret', () => {
    const vars = fnWithEnv(prod.api, 'REQUIRE_ORIGIN_HEADER').Properties['Environment'].Variables;
    expect(vars.REQUIRE_ORIGIN_HEADER).toBe('true');
    expect(vars.ORIGIN_VERIFY_SECRET_ARN).toBeDefined();
    expect(
      fnWithEnv(dev.api, 'REQUIRE_ORIGIN_HEADER').Properties['Environment'].Variables
        .REQUIRE_ORIGIN_HEADER,
    ).toBe('false');
    expect(actions(prod.api)).toContain('secretsmanager:GetSecretValue');
  });

  it('edge deploys after config (the replica must exist first)', () => {
    const { stacks } = synthApp('prod');
    expect(stacks.edge.dependencies).toContain(stacks.config);
  });
});

describe('cell-index repair outbox', () => {
  const t = prod.api;

  it('has a repair queue with a DLQ (5 receives) and the API gets its URL', () => {
    t.hasResourceProperties('AWS::SQS::Queue', {
      RedrivePolicy: Match.objectLike({ maxReceiveCount: 5 }),
      VisibilityTimeout: 360,
    });
    expect(fnWithEnv(t, 'INDEX_REPAIR_QUEUE_URL')).toBeDefined();
    expect(actions(t)).toContain('sqs:SendMessage');
  });

  it('runs the repair Lambda from the queue with per-item failure reporting', () => {
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 10,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
      EventSourceArn: { 'Fn::GetAtt': [Match.stringLikeRegexp('^IndexRepairQueue'), 'Arn'] },
    });
    const repair = fns(t).find(
      (f) =>
        f.Properties['Environment']?.Variables?.CELL_INDEX_TABLE !== undefined &&
        f.Properties['Environment']?.Variables?.CHECKINS_TABLE === undefined,
    );
    expect(repair).toBeDefined();
    expect(Object.keys(repair?.Properties['Environment'].Variables).sort()).toEqual([
      'CELL_INDEX_TABLE',
      'DB_CLUSTER_ARN',
      'DB_NAME',
      'DB_SECRET_ARN',
    ]);
  });

  it('alarms when anything lands in the repair DLQ', () => {
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Threshold: 0,
      ComparisonOperator: 'GreaterThanThreshold',
      Dimensions: [
        {
          Name: 'QueueName',
          Value: { 'Fn::GetAtt': [Match.stringLikeRegexp('^IndexRepairDlq'), 'QueueName'] },
        },
      ],
    });
  });
});

describe('late joiners', () => {
  it('the API has both send queue URLs and may send to them', () => {
    const vars = fnWithEnv(dev.api, 'STANDARD_QUEUE_URL').Properties['Environment'].Variables;
    expect(vars).toHaveProperty('URGENT_QUEUE_URL');
    const send = statements(dev.api).filter((s) => [s.Action].flat().includes('sqs:SendMessage'));
    const resources = send.flatMap((s) => [s.Resource].flat());
    expect(resources.length).toBeGreaterThanOrEqual(3); // repair, urgent, standard
  });
});

describe('capacity', () => {
  it('Aurora max ACU: prod 16, dev 2, configurable', () => {
    prod.data.hasResourceProperties('AWS::RDS::DBCluster', {
      ServerlessV2ScalingConfiguration: Match.objectLike({ MaxCapacity: 16 }),
    });
    dev.data.hasResourceProperties('AWS::RDS::DBCluster', {
      ServerlessV2ScalingConfiguration: Match.objectLike({ MaxCapacity: 2 }),
    });
    synthAll('prod', { capacity: { auroraMaxAcu: 32 } }).data.hasResourceProperties(
      'AWS::RDS::DBCluster',
      { ServerlessV2ScalingConfiguration: Match.objectLike({ MaxCapacity: 32 }) },
    );
  });

  it('ingest has 1024 MB in both stages', () => {
    for (const t of [dev.ingest, prod.ingest]) {
      t.hasResourceProperties('AWS::Lambda::Function', { MemorySize: 1024, Timeout: 70 });
    }
  });

  it('check-ins table: prod warm throughput 40,000 writes/s, dev none', () => {
    const checkins = (t: Template) =>
      (Object.values(t.findResources('AWS::DynamoDB::Table')) as any[]).find(
        (x) => x.Properties.StreamSpecification,
      );
    expect(checkins(prod.api).Properties.WarmThroughput).toEqual({ WriteUnitsPerSecond: 40000 });
    expect(checkins(dev.api).Properties.WarmThroughput).toBeUndefined();
    const warmed = (Object.values(prod.api.findResources('AWS::DynamoDB::Table')) as any[]).filter(
      (x) => x.Properties.WarmThroughput,
    );
    expect(warmed).toHaveLength(1); // the totals table is written in small batches
    const custom = synthAll('prod', { capacity: { checkinsWarmWritesPerSecond: 10000 } });
    expect(checkins(custom.api).Properties.WarmThroughput).toEqual({ WriteUnitsPerSecond: 10000 });
  });

  it('reserved concurrency is unset by default (a fresh account deploys) and settable', () => {
    for (const t of [dev.api, prod.api]) {
      expect(
        fns(t).filter((f) => f.Properties['ReservedConcurrentExecutions'] !== undefined),
      ).toEqual([]);
    }
    const t = synthAll('prod', {
      capacity: {
        apiReservedConcurrency: 3000,
        aggregatorReservedConcurrency: 20,
        repairReservedConcurrency: 5,
      },
    }).api;
    const values = fns(t)
      .map((f) => f.Properties['ReservedConcurrentExecutions'])
      .filter((v) => v !== undefined)
      .sort((a, b) => a - b);
    expect(values).toEqual([5, 20, 3000]);
  });

  it('the totals table keeps TTL on the ttl attribute (the aggregator now sets it)', () => {
    prod.api.hasResourceProperties('AWS::DynamoDB::Table', {
      TimeToLiveSpecification: { AttributeName: 'ttl', Enabled: true },
    });
  });
});
