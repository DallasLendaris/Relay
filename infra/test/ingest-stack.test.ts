import { Match } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { synthAll } from './helpers.js';

function synth(stage: 'dev' | 'prod') {
  return synthAll(stage).ingest;
}

describe('IngestStack', () => {
  const t = synth('dev');

  it('blocks all public access on the raw bucket', () => {
    t.hasResourceProperties('AWS::S3::Bucket', {
      PublicAccessBlockConfiguration: {
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      },
    });
  });

  it('limits the function to one concurrent run with the right env vars', () => {
    t.hasResourceProperties('AWS::Lambda::Function', {
      ReservedConcurrentExecutions: 1,
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Timeout: 70,
      MemorySize: 1024,
      Environment: {
        Variables: Match.objectLike({
          NWS_USER_AGENT: 'Relay capstone (contact via GitHub DallasLendaris/Relay)',
          POLL_INTERVAL_SECONDS: '12',
          POLLS_PER_INVOCATION: '5',
          RAW_BUCKET: Match.anyValue(),
          STATE_TABLE: Match.anyValue(),
        }),
      },
    });
  });

  it('runs every minute with no retries', () => {
    t.hasResourceProperties('AWS::Events::Rule', {
      ScheduleExpression: 'rate(1 minute)',
      Targets: [Match.objectLike({ RetryPolicy: { MaximumRetryAttempts: 0 } })],
    });
  });

  it('does not grant s3:* or dynamodb:* (or any wildcard action)', () => {
    const policies = t.findResources('AWS::IAM::Policy');
    const actions: string[] = Object.values(policies)
      .flatMap((p: any) => p.Properties.PolicyDocument.Statement)
      .flatMap((s: any) => (Array.isArray(s.Action) ? s.Action : [s.Action]));
    expect(actions.length).toBeGreaterThan(0);
    expect(actions).not.toContain('s3:*');
    expect(actions).not.toContain('dynamodb:*');
    expect(actions.filter((a) => a === '*' || a.endsWith(':*'))).toEqual([]);
  });

  it('creates an errors alarm (>= 3 in 5 minutes)', () => {
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'Errors',
      Namespace: 'AWS/Lambda',
      Threshold: 3,
      Period: 300,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
    });
  });

  it('alarms on the custom Relay/Ingest metrics, which keep working during an outage', () => {
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      Namespace: 'Relay/Ingest',
      MetricName: 'Errors',
      Statistic: 'Sum',
      Threshold: 3,
      Period: 300,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
      TreatMissingData: 'notBreaching',
    });
    // FetchFailed is emitted every invocation, so missing data means ingest stopped: breaching.
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      Namespace: 'Relay/Ingest',
      MetricName: 'FetchFailed',
      Statistic: 'Sum',
      Threshold: 5,
      Period: 300,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
      TreatMissingData: 'breaching',
    });
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      Namespace: 'Relay/Ingest',
      MetricName: 'Quarantined',
      Statistic: 'Sum',
      Threshold: 1,
      Period: 300,
      EvaluationPeriods: 1,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
      TreatMissingData: 'notBreaching',
    });
    t.resourceCountIs('AWS::CloudWatch::Alarm', 4); // the original Lambda Errors alarm stays
  });

  it('retains data and enables PITR on prod, destroys on dev', () => {
    const prod = synth('prod');
    prod.hasResource('AWS::S3::Bucket', { DeletionPolicy: 'Retain' });
    prod.hasResourceProperties('AWS::DynamoDB::Table', {
      PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
    });
    t.hasResource('AWS::S3::Bucket', { DeletionPolicy: 'Delete' });
  });

  it('moves raw objects to Infrequent Access after 30 days', () => {
    t.hasResourceProperties('AWS::S3::Bucket', {
      LifecycleConfiguration: {
        Rules: [
          Match.objectLike({
            Transitions: [{ StorageClass: 'STANDARD_IA', TransitionInDays: 30 }],
          }),
        ],
      },
    });
  });
});
