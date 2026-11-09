import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { IngestStack } from '../lib/ingest-stack.js';

function synth(stage: 'dev' | 'prod') {
  const app = new App();
  const stack = new IngestStack(app, `test-${stage}`, { stage, env: { region: 'us-west-2' } });
  return Template.fromStack(stack);
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
      MemorySize: 256,
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

  it('retains data and enables PITR on prod, destroys on dev', () => {
    const prod = synth('prod');
    prod.hasResource('AWS::S3::Bucket', { DeletionPolicy: 'Retain' });
    prod.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'alerts-state',
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
