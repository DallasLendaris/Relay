import { App } from 'aws-cdk-lib';
import { Match } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { buildRelayApp } from '../lib/relay-app.js';
import { SKIP_IMAGE_CONTEXT } from '../lib/sender-stack.js';
import { synthAll } from './helpers.js';

const entries = (t: any, type: string) =>
  Object.entries(t.findResources(type)) as Array<[string, any]>;
const values = (t: any, type: string) => entries(t, type).map(([, v]) => v);

/** Auto scaling's own alarms act on a scaling policy; they are the only alarms exempt from the topic. */
const isAutoScalingAlarm = (t: any, alarm: any) => {
  const policies = Object.keys(t.findResources('AWS::ApplicationAutoScaling::ScalingPolicy'));
  const actions = JSON.stringify(alarm.Properties.AlarmActions ?? []);
  return policies.some((id) => actions.includes(`"${id}"`));
};

describe('alerts topic (OPS-1 / SEC-C-3)', () => {
  it('one topic per stage named relay-<stage>-alerts, with the email subscription', () => {
    for (const stage of ['dev', 'prod'] as const) {
      const t = synthAll(stage, { monitoring: { alarmEmail: 'oncall@example.com' } }).monitoring;
      t.hasResourceProperties('AWS::SNS::Topic', { TopicName: `relay-${stage}-alerts` });
      t.resourceCountIs('AWS::SNS::Topic', 1);
      t.hasResourceProperties('AWS::SNS::Subscription', {
        Protocol: 'email',
        Endpoint: 'oncall@example.com',
      });
    }
  });

  it('prod fails synth without alarmEmail unless allowNoAlarmEmail=true; dev does not need one', () => {
    const build = (stage: 'dev' | 'prod', monitoring: Record<string, unknown>) => {
      const app = new App({ context: { [SKIP_IMAGE_CONTEXT]: 'true' } });
      return buildRelayApp(app, {
        stage,
        env: { account: '111111111111', region: 'us-west-2' },
        edgeEnv: { account: '111111111111', region: 'us-east-1' },
        prefix: `x-${stage}`,
        publicAlertBaseUrl: 'https://alerts.example.org',
        monitoring,
      });
    };
    expect(() => build('prod', {})).toThrow(/alarm email/);
    expect(() => build('prod', { alarmEmail: '  ' })).toThrow(/alarm email/);
    expect(() => build('prod', { allowNoAlarmEmail: true })).not.toThrow();
    expect(() => build('dev', {})).not.toThrow();
  });

  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: every non-autoscaling alarm in every stack has the topic as ALARM and OK action`, () => {
      const all = synthAll(stage);
      let checked = 0;
      let skipped = 0;
      for (const [name, t] of Object.entries(all)) {
        for (const [id, alarm] of entries(t, 'AWS::CloudWatch::Alarm')) {
          if (isAutoScalingAlarm(t, alarm)) {
            skipped++;
            continue;
          }
          checked++;
          for (const key of ['AlarmActions', 'OKActions']) {
            const actions = JSON.stringify(alarm.Properties[key] ?? []);
            // Same stack: a Ref to the topic. Other stacks: an import of the monitoring stack's export.
            expect(
              actions.includes('AlertsTopic') || actions.includes(`${stage}-monitoring`),
              `${name}/${id} ${key} -> ${actions}`,
            ).toBe(true);
          }
        }
      }
      // Not vacuous: the stacks hold many alarms, and the scale-in/out alarms were exempted.
      expect(checked).toBeGreaterThanOrEqual(20);
      if (stage === 'prod') expect(skipped).toBeGreaterThan(0);
    });
  }

  it('the topic policy lets CloudWatch, Budgets, Cost Anomaly Detection and EventBridge publish', () => {
    const policy = JSON.stringify(
      values(synthAll('prod').monitoring, 'AWS::SNS::TopicPolicy').map((p) => p.Properties),
    );
    for (const p of [
      'cloudwatch.amazonaws.com',
      'budgets.amazonaws.com',
      'costalerts.amazonaws.com',
      'events.amazonaws.com',
    ]) {
      expect(policy).toContain(p);
    }
  });
});

describe('new alarms (OPS-2)', () => {
  const prod = synthAll('prod');
  const find = (t: any, idPrefix: string) => {
    const hit = entries(t, 'AWS::CloudWatch::Alarm').find(([k]) => k.startsWith(idPrefix));
    expect(hit, idPrefix).toBeDefined();
    return hit?.[1].Properties;
  };

  it('planner and shard Lambda Errors >= 1', () => {
    for (const [id, fn] of [
      ['PlannerErrorsAlarm', 'PlannerFunction'],
      ['ShardErrorsAlarm', 'ShardFunction'],
    ] as const) {
      const a = find(prod.fanout, id);
      expect(a.MetricName).toBe('Errors');
      expect(a.Namespace).toBe('AWS/Lambda');
      expect(a.Threshold).toBe(1);
      expect(JSON.stringify(a.Dimensions)).toContain(fn);
    }
  });

  it('plan and standard queue age > 120 s', () => {
    for (const id of ['PlanQueueAgeAlarm', 'StandardQueueAgeAlarm']) {
      const a = find(prod.fanout, id);
      expect(a.MetricName).toBe('ApproximateAgeOfOldestMessage');
      expect(a.Threshold).toBe(120);
      expect(a.ComparisonOperator).toBe('GreaterThanThreshold');
    }
    // The plan queue's name is taken from its ARN, not guessed.
    expect(JSON.stringify(find(prod.fanout, 'PlanQueueAgeAlarm').Dimensions)).toContain(
      'Fn::Split',
    );
  });

  it('aggregator IteratorAge > 60 s (the metric is in milliseconds)', () => {
    const a = find(prod.api, 'AggregatorIteratorAgeAlarm');
    expect(a.MetricName).toBe('IteratorAge');
    expect(a.Threshold).toBe(60_000);
    expect(JSON.stringify(a.Dimensions)).toContain('AggregatorFunction');
  });

  it('signature_failed log metric filters and alarms, in fanout (planner + shard) and delivery', () => {
    const fanoutFilters = values(prod.fanout, 'AWS::Logs::MetricFilter').map((f) => f.Properties);
    expect(fanoutFilters).toHaveLength(2);
    for (const f of fanoutFilters) {
      expect(f.FilterPattern).toContain('fanout.signature_failed');
      expect(f.MetricTransformations[0]).toMatchObject({
        MetricNamespace: 'Relay/Fanout',
        MetricName: 'SignatureFailed',
      });
    }
    const [d] = values(prod.delivery, 'AWS::Logs::MetricFilter').map((f) => f.Properties);
    expect(d.FilterPattern).toContain('delivery.signature_failed');
    for (const [t, id, ns] of [
      [prod.fanout, 'FanoutSignatureFailedAlarm', 'Relay/Fanout'],
      [prod.delivery, 'DeliverySignatureFailedAlarm', 'Relay/Delivery'],
    ] as const) {
      const a = find(t, id);
      expect(a.Threshold).toBe(1);
      expect(a.Namespace).toBe(ns);
      expect(a.MetricName).toBe('SignatureFailed');
    }
  });
});

describe('audit trail (SEC-C-4)', () => {
  const t = synthAll('prod').monitoring;

  it('a multi-region trail with log file validation into an encrypted, versioned, 1-year bucket', () => {
    t.hasResourceProperties('AWS::CloudTrail::Trail', {
      IsMultiRegionTrail: true,
      EnableLogFileValidation: true,
      IncludeGlobalServiceEvents: true,
      IsLogging: true,
    });
    t.hasResourceProperties('AWS::S3::Bucket', {
      VersioningConfiguration: { Status: 'Enabled' },
      BucketEncryption: Match.objectLike({
        ServerSideEncryptionConfiguration: Match.anyValue(),
      }),
      PublicAccessBlockConfiguration: Match.objectLike({ BlockPublicAcls: true }),
      LifecycleConfiguration: {
        Rules: [
          Match.objectLike({
            ExpirationInDays: 365,
            NoncurrentVersionExpiration: { NoncurrentDays: 365 },
            Status: 'Enabled',
          }),
        ],
      },
    });
    const bucket = values(t, 'AWS::S3::Bucket')[0];
    expect(bucket.DeletionPolicy).toBe('Retain');
  });

  it('kill-switch writes and relay secret changes are routed to the topic by EventBridge', () => {
    const rules = values(t, 'AWS::Events::Rule').map((r) => r.Properties);
    const ssm = rules.filter((r) => r.EventPattern.source?.includes('aws.ssm'));
    expect(ssm.length).toBeGreaterThanOrEqual(2);
    expect(ssm[0].EventPattern.detail.eventName).toContain('PutParameter');
    // The name is the config stack's kill-switch parameter, imported (not retyped).
    expect(JSON.stringify(ssm[0].EventPattern.detail.requestParameters)).toContain(
      'DeliveryKillSwitch',
    );
    const sm = rules.find((r) => r.EventPattern.source?.includes('aws.secretsmanager'));
    expect(sm.EventPattern.detail.eventName).toContain('PutSecretValue');
    expect(JSON.stringify(sm.EventPattern.detail.requestParameters)).toContain('relay/prod/');
    for (const r of [...ssm, sm]) expect(JSON.stringify(r.Targets)).toContain('AlertsTopic');
  });

  it('IAM policy changes (global events, so read from the trail log group) alarm to the topic', () => {
    const f = values(t, 'AWS::Logs::MetricFilter').find((x) =>
      x.Properties.FilterPattern.includes('iam.amazonaws.com'),
    );
    for (const e of [
      'PutRolePolicy',
      'AttachRolePolicy',
      'CreatePolicyVersion',
      'UpdateAssumeRolePolicy',
    ]) {
      expect(f.Properties.FilterPattern).toContain(e);
    }
    expect(f.Properties.FilterPattern).toContain('cloudformation.amazonaws.com');
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      Namespace: 'Relay/Security',
      MetricName: 'IamPolicyChange',
      AlarmActions: [{ Ref: Match.stringLikeRegexp('AlertsTopic') }],
    });
  });

  it('GuardDuty: on by default on prod with high findings to the topic; off on dev; switchable', () => {
    t.resourceCountIs('AWS::GuardDuty::Detector', 1);
    const rule = values(t, 'AWS::Events::Rule').find((r) =>
      r.Properties.EventPattern.source?.includes('aws.guardduty'),
    );
    expect(rule.Properties.EventPattern.detail.severity).toEqual([{ numeric: ['>=', 7] }]);
    expect(JSON.stringify(rule.Properties.Targets)).toContain('AlertsTopic');
    synthAll('dev').monitoring.resourceCountIs('AWS::GuardDuty::Detector', 0);
    synthAll('dev', { monitoring: { enableGuardDuty: true } }).monitoring.resourceCountIs(
      'AWS::GuardDuty::Detector',
      1,
    );
    synthAll('prod', {
      monitoring: { alarmEmail: 'a@b.co', enableGuardDuty: false },
    }).monitoring.resourceCountIs('AWS::GuardDuty::Detector', 0);
  });

  it('log retention: 365 days on every prod log group, 30 on dev, overridable', () => {
    const retentions = (all: Record<string, any>) =>
      Object.entries(all).flatMap(([name, tpl]) =>
        values(tpl, 'AWS::Logs::LogGroup').map((g) => [name, g.Properties.RetentionInDays]),
      );
    const prodLogs = retentions(synthAll('prod'));
    expect(prodLogs.length).toBeGreaterThan(8);
    for (const [name, days] of prodLogs) expect(days, String(name)).toBe(365);
    for (const [name, days] of retentions(synthAll('dev'))) expect(days, String(name)).toBe(30);
    const custom = synthAll('prod', {
      monitoring: { alarmEmail: 'a@b.co', logRetentionDays: 90 },
    });
    for (const [, days] of retentions(custom)) expect(days).toBe(90);
    expect(() => synthAll('dev', { monitoring: { logRetentionDays: 45 } })).toThrow(/retention/i);
  });
});

describe('cost (OPS-14)', () => {
  const budget = (t: any) => values(t, 'AWS::Budgets::Budget')[0].Properties;

  it('monthly budget per stage with 80% and 100% notifications to the topic', () => {
    for (const [stage, usd] of [
      ['prod', 600],
      ['dev', 50],
    ] as const) {
      const b = budget(synthAll(stage).monitoring);
      expect(b.Budget.BudgetName).toBe(`relay-${stage}-monthly`);
      expect(b.Budget.BudgetLimit).toEqual({ Amount: usd, Unit: 'USD' });
      expect(
        b.NotificationsWithSubscribers.map((n: any) => n.Notification.Threshold).sort(),
      ).toEqual([100, 80]);
      for (const n of b.NotificationsWithSubscribers) {
        expect(n.Subscribers).toHaveLength(1);
        expect(n.Subscribers[0].SubscriptionType).toBe('SNS');
        expect(JSON.stringify(n.Subscribers[0].Address)).toContain('AlertsTopic');
      }
    }
  });

  it('the amount comes from monthlyBudgetUsd', () => {
    const t = synthAll('prod', { monitoring: { alarmEmail: 'a@b.co', monthlyBudgetUsd: 1234 } });
    expect(budget(t.monitoring).Budget.BudgetLimit.Amount).toBe(1234);
  });

  it('prod also has a cost anomaly monitor and an immediate SNS subscription', () => {
    const t = synthAll('prod').monitoring;
    t.hasResourceProperties('AWS::CE::AnomalyMonitor', { MonitorDimension: 'SERVICE' });
    t.hasResourceProperties('AWS::CE::AnomalySubscription', {
      Frequency: 'IMMEDIATE',
      Subscribers: [{ Type: 'SNS', Address: Match.anyValue() }],
    });
    synthAll('dev').monitoring.resourceCountIs('AWS::CE::AnomalyMonitor', 0);
  });
});

describe('backups and warm throughput (OPS-5, OPS-15)', () => {
  it('Aurora keeps automated backups 14 days on prod, 7 on dev, and copies tags to snapshots', () => {
    for (const [stage, days] of [
      ['prod', 14],
      ['dev', 7],
    ] as const) {
      synthAll(stage).data.hasResourceProperties('AWS::RDS::DBCluster', {
        BackupRetentionPeriod: days,
        CopyTagsToSnapshot: true,
      });
    }
  });

  it('idempotency table: warm throughput 20,000 writes/s on prod, none on dev', () => {
    const table = (t: any) => values(t, 'AWS::DynamoDB::Table')[0].Properties;
    expect(table(synthAll('prod').delivery).WarmThroughput).toEqual({
      WriteUnitsPerSecond: 20000,
    });
    expect(table(synthAll('dev').delivery).WarmThroughput).toBeUndefined();
  });
});
