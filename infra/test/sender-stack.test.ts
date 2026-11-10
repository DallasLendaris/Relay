import { App, Aws } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { DB_ROLES, type DbRefs } from '../lib/grants.js';
import {
  SENDER_IDLE_MINUTES_BEFORE_SCALE_IN,
  SENDER_MAX_TASKS,
  SKIP_IMAGE_CONTEXT,
  SenderStack,
} from '../lib/sender-stack.js';

const ALERT_KEY_ARN = 'arn:aws:kms:us-west-2:111111111111:key/alert-signing';

const queue = (name: string) => ({
  name,
  arn: `arn:aws:sqs:us-west-2:111111111111:${name}`,
  url: `https://sqs.us-west-2.amazonaws.com/111111111111/${name}`,
});

function synth(stage: 'dev' | 'prod', context: Record<string, string> = {}) {
  // The context flag keeps the Docker image asset out of the test (no hashing the repo, no Docker).
  const app = new App({ context: { [SKIP_IMAGE_CONTEXT]: 'true', ...context } });
  const stack = new SenderStack(app, `sender-${stage}`, {
    env: { region: 'us-west-2' },
    stage,
    db: {
      clusterArn: 'arn:aws:rds:us-west-2:111111111111:cluster:db',
      roleSecretArns: Object.fromEntries(
        Object.keys(DB_ROLES).map((r) => [
          r,
          `arn:aws:secretsmanager:us-west-2:111111111111:secret:db-${r}-AbCdEf`,
        ]),
      ) as DbRefs['roleSecretArns'],
      databaseName: 'relay',
    },
    urgentQueue: queue('send-urgent'),
    standardQueue: queue('send-standard'),
    idempotencyTableName: 'idem',
    idempotencyTableArn: 'arn:aws:dynamodb:us-west-2:111111111111:table/idem',
    cellIndexTableName: 'cells',
    cellIndexTableArn: 'arn:aws:dynamodb:us-west-2:111111111111:table/cells',
    alertSigning: {
      current: { kid: 'x-alert-v1', keyArn: ALERT_KEY_ARN },
      verify: [{ kid: 'x-alert-v1', keyArn: ALERT_KEY_ARN }],
    },
    killSwitchParamName: '/relay/x/kill',
    killSwitchParamArn: 'arn:aws:ssm:us-west-2:111111111111:parameter/relay/x/kill',
  });
  return Template.fromStack(stack);
}

const dev = synth('dev');
const prod = synth('prod');
// Fleet enabled (context senderEnabled=true): the scaling and DesiredCount tests use these.
const ON = { senderEnabled: 'true' };
const devOn = synth('dev', ON);
const prodOn = synth('prod', ON);

function statements(t: Template): any[] {
  const policies = Object.values(t.findResources('AWS::IAM::Policy')) as any[];
  return policies.flatMap((p) => p.Properties.PolicyDocument.Statement);
}
const actionsOf = (s: any): string[] => (Array.isArray(s.Action) ? s.Action : [s.Action]);
const resourcesOf = (s: any): any[] => (Array.isArray(s.Resource) ? s.Resource : [s.Resource]);
/** Actions granted on a resource whose JSON contains `needle`. */
function grantedOn(t: Template, needle: string): string[] {
  return statements(t)
    .filter((s) => s.Effect === 'Allow' && JSON.stringify(resourcesOf(s)).includes(needle))
    .flatMap(actionsOf)
    .sort();
}

describe('SenderStack network', () => {
  it('has no NAT gateway and only public subnets', () => {
    dev.resourceCountIs('AWS::EC2::NatGateway', 0);
    const subnets = Object.values(dev.findResources('AWS::EC2::Subnet')) as any[];
    expect(subnets.length).toBeGreaterThan(0);
    for (const s of subnets) expect(s.Properties.MapPublicIpOnLaunch).toBe(true);
  });

  it('task security group: egress 443 only, no ingress', () => {
    const groups = Object.values(dev.findResources('AWS::EC2::SecurityGroup')) as any[];
    expect(groups).toHaveLength(1);
    const props = groups[0].Properties;
    expect(props.SecurityGroupIngress ?? []).toHaveLength(0);
    expect(props.SecurityGroupEgress).toHaveLength(1);
    expect(props.SecurityGroupEgress[0]).toMatchObject({
      IpProtocol: 'tcp',
      FromPort: 443,
      ToPort: 443,
      CidrIp: '0.0.0.0/0',
    });
    dev.resourceCountIs('AWS::EC2::SecurityGroupIngress', 0);
  });

  it('runs tasks with a public IP', () => {
    dev.hasResourceProperties('AWS::ECS::Service', {
      LaunchType: 'FARGATE',
      NetworkConfiguration: { AwsvpcConfiguration: { AssignPublicIp: 'ENABLED' } },
    });
  });
});

describe('SenderStack service', () => {
  it('is ARM64 Fargate, 1 vCPU / 2 GB, with a 30 s stop timeout and container insights', () => {
    dev.hasResourceProperties('AWS::ECS::TaskDefinition', {
      Cpu: '1024',
      Memory: '2048',
      RuntimePlatform: { CpuArchitecture: 'ARM64', OperatingSystemFamily: 'LINUX' },
      ContainerDefinitions: [Match.objectLike({ StopTimeout: 30, User: Match.absent() })],
    });
    dev.hasResourceProperties('AWS::ECS::Cluster', {
      ClusterSettings: [{ Name: 'containerInsights', Value: 'enabled' }],
    });
  });

  it('passes the environment main.ts needs', () => {
    const [td] = Object.values(dev.findResources('AWS::ECS::TaskDefinition')) as any[];
    const env = Object.fromEntries(
      td.Properties.ContainerDefinitions[0].Environment.map((e: any) => [e.Name, e.Value]),
    );
    for (const k of [
      'URGENT_QUEUE_URL',
      'STANDARD_QUEUE_URL',
      'IDEMPOTENCY_TABLE',
      'CELL_INDEX_TABLE',
      'DB_CLUSTER_ARN',
      'DB_SECRET_ARN',
      'DB_NAME',
      'ALERT_VERIFY_KEYS',
      'KILL_SWITCH_PARAM',
      'APNS_KEY_SECRET_ARN',
      'FCM_SERVICE_ACCOUNT_SECRET_ARN',
    ]) {
      expect(env, k).toHaveProperty(k);
    }
    expect(env['URGENT_QUEUE_URL']).toContain('send-urgent');
  });

  it('spreads Android over the Firebase projects named by the fcmProjectIds context (PERF-1)', () => {
    const envOf = (t: Template) => {
      const [td] = Object.values(t.findResources('AWS::ECS::TaskDefinition')) as any[];
      return Object.fromEntries(
        td.Properties.ContainerDefinitions[0].Environment.map((e: any) => [e.Name, e.Value]),
      );
    };
    // Default: one project, nothing to configure, the secret stays one placeholder object.
    expect(envOf(dev)).not.toHaveProperty('FCM_PROJECT_IDS');
    const three = synth('dev', { fcmProjectIds: 'relay-a, relay-b,relay-c' });
    expect(envOf(three)['FCM_PROJECT_IDS']).toBe('relay-a,relay-b,relay-c');
    const secret = Object.values(three.findResources('AWS::SecretsManager::Secret')).find(
      (r: any) => JSON.stringify(r).includes('fcm-service-account'),
    ) as any;
    const placeholder = JSON.parse(secret.Properties.SecretString);
    expect(placeholder.map((a: any) => a.project_id)).toEqual(['relay-a', 'relay-b', 'relay-c']);
    expect(() => synth('dev', { fcmProjectIds: 'relay-a,relay-a' })).toThrow(/duplicate/);
    expect(() => synth('dev', { fcmProjectIds: 'Bad_Id' })).toThrow(/invalid project id/);
  });

  it('keeps logs for one month', () => {
    dev.hasResourceProperties('AWS::Logs::LogGroup', { RetentionInDays: 30 });
  });

  it('creates the APNs and FCM secrets with placeholder values', () => {
    const secrets = Object.values(dev.findResources('AWS::SecretsManager::Secret')) as any[];
    const byName = Object.fromEntries(secrets.map((s) => [s.Properties.Name, s.Properties]));
    const apns = JSON.parse(byName['relay/dev/apns-key'].SecretString);
    expect(Object.keys(apns).sort()).toEqual(['bundleId', 'keyId', 'p8', 'sandbox', 'teamId']);
    expect(apns.p8).toBe('REPLACE_ME');
    const fcm = JSON.parse(byName['relay/dev/fcm-service-account'].SecretString);
    expect(fcm.project_id).toBe('REPLACE_ME');
  });
});

const STOP = { senderEnabled: 'false', iUnderstandSenderStops: 'true' };
const prodOff = synth('prod', STOP);

describe('SenderStack container hardening (SEC-C-14)', () => {
  it.each([
    ['dev', dev],
    ['prod', prod],
  ])(
    '%s: read-only root filesystem, no writable volumes, user left to the image (USER node)',
    (_n, t) => {
      const defs = Object.values(t.findResources('AWS::ECS::TaskDefinition')) as any[];
      expect(defs).toHaveLength(1);
      const containers = defs[0].Properties.ContainerDefinitions as any[];
      expect(containers).toHaveLength(1);
      expect(containers[0].ReadonlyRootFilesystem).toBe(true);
      expect(containers[0].User).toBeUndefined();
      // The process writes no files; a volume would be a reason to re-check this.
      expect(defs[0].Properties.Volumes ?? []).toEqual([]);
      expect(containers[0].MountPoints ?? []).toEqual([]);
    },
  );
});

describe('SenderStack gating (placeholder secrets must not crash-loop)', () => {
  it('dev defaults to off: desiredCount 0 and no auto scaling at all', () => {
    dev.hasResourceProperties('AWS::ECS::Service', { DesiredCount: 0 });
    dev.resourceCountIs('AWS::ApplicationAutoScaling::ScalableTarget', 0);
    dev.resourceCountIs('AWS::ApplicationAutoScaling::ScalingPolicy', 0);
  });

  it('OPS-4: prod defaults to ON, so a deploy without flags never scales the fleet to 0', () => {
    const [svc] = Object.values(prod.findResources('AWS::ECS::Service')) as any[];
    expect(svc.Properties.DesiredCount).toBeUndefined();
    prod.hasResourceProperties('AWS::ApplicationAutoScaling::ScalableTarget', { MinCapacity: 10 });
  });

  it('OPS-4: turning prod off needs senderEnabled=false AND iUnderstandSenderStops=true', () => {
    expect(() => synth('prod', { senderEnabled: 'false' })).toThrow(/iUnderstandSenderStops/);
    expect(() =>
      synth('prod', { senderEnabled: 'false', iUnderstandSenderStops: 'false' }),
    ).toThrow(/Refusing/);
    prodOff.hasResourceProperties('AWS::ECS::Service', { DesiredCount: 0 });
    prodOff.resourceCountIs('AWS::ApplicationAutoScaling::ScalableTarget', 0);
    prodOff.resourceCountIs('AWS::ApplicationAutoScaling::ScalingPolicy', 0);
  });

  it('with senderEnabled=true: desiredCount is left to auto scaling', () => {
    for (const t of [devOn, prodOn]) {
      const [svc] = Object.values(t.findResources('AWS::ECS::Service')) as any[];
      expect(svc.Properties.DesiredCount).toBeUndefined();
      t.resourceCountIs('AWS::ApplicationAutoScaling::ScalableTarget', 1);
    }
  });

  it('OPS-4: a bad image rolls back: circuit breaker plus a running-tasks deployment alarm', () => {
    for (const t of [devOn, prod]) {
      const [svc] = Object.values(t.findResources('AWS::ECS::Service')) as any[];
      const cfg = svc.Properties.DeploymentConfiguration;
      expect(cfg.DeploymentCircuitBreaker).toEqual({ Enable: true, Rollback: true });
      expect(cfg.Alarms.Enable).toBe(true);
      expect(cfg.Alarms.Rollback).toBe(true);
      const alarmLogicalId = Object.keys(
        t.findResources('AWS::CloudWatch::Alarm', {
          Properties: { AlarmDescription: Match.stringLikeRegexp('below desired') },
        }),
      )[0];
      expect(alarmLogicalId).toBeDefined();
      expect(JSON.stringify(cfg.Alarms.AlarmNames)).toContain(alarmLogicalId);
    }
  });

  it('a disabled fleet has no deployment alarm to wait on', () => {
    for (const t of [dev, prodOff]) {
      const [svc] = Object.values(t.findResources('AWS::ECS::Service')) as any[];
      expect(svc.Properties.DeploymentConfiguration.Alarms?.Enable ?? false).toBe(false);
    }
  });

  it('uses stage-derived cluster and service names (the planner builds its ARNs from them)', () => {
    prod.hasResourceProperties('AWS::ECS::Cluster', { ClusterName: 'relay-prod-sender' });
    prod.hasResourceProperties('AWS::ECS::Service', { ServiceName: 'relay-prod-sender' });
    dev.hasResourceProperties('AWS::ECS::Service', { ServiceName: 'relay-dev-sender' });
  });
});

describe('SenderStack scaling', () => {
  it('prod scales between 10 and 100 tasks, dev between 0 and 10', () => {
    expect(SENDER_MAX_TASKS).toBe(100);
    prodOn.hasResourceProperties('AWS::ApplicationAutoScaling::ScalableTarget', {
      MinCapacity: 10,
      MaxCapacity: 100,
      ServiceNamespace: 'ecs',
    });
    devOn.hasResourceProperties('AWS::ApplicationAutoScaling::ScalableTarget', {
      MinCapacity: 0,
      MaxCapacity: 10,
    });
  });

  it('prod minimum is configurable with context senderMinTasks, and validated', () => {
    synth('prod', { ...ON, senderMinTasks: '25' }).hasResourceProperties(
      'AWS::ApplicationAutoScaling::ScalableTarget',
      { MinCapacity: 25, MaxCapacity: 100 },
    );
    expect(() => synth('prod', { ...ON, senderMinTasks: '500' })).toThrow('senderMinTasks');
    expect(() => synth('prod', { ...ON, senderMinTasks: 'x' })).toThrow('senderMinTasks');
  });

  it('has step scaling on queue depth and age (out) and an idle policy (in)', () => {
    const policies = Object.values(
      prodOn.findResources('AWS::ApplicationAutoScaling::ScalingPolicy'),
    ) as any[];
    expect(policies).toHaveLength(3); // out on depth, out on age, in when idle
    for (const p of policies) expect(p.Properties.PolicyType).toBe('StepScaling');
    const steps = (p: any) => p.Properties.StepScalingPolicyConfiguration;
    const outs = policies.filter((p) =>
      steps(p).StepAdjustments.some((a: any) => a.ScalingAdjustment > 0),
    );
    const ins = policies.filter((p) =>
      steps(p).StepAdjustments.some((a: any) => a.ScalingAdjustment < 0),
    );
    expect(outs).toHaveLength(2);
    expect(ins).toHaveLength(1);
    // Out is fast (short cooldown); in is slow (long cooldown, many evaluation periods).
    for (const p of outs) expect(steps(p).Cooldown).toBeLessThanOrEqual(60);
    expect(steps(ins[0]).Cooldown).toBeGreaterThanOrEqual(300);
    // Aggressive: the depth policy reaches +50 tasks in one step.
    const biggest = Math.max(
      ...outs.flatMap((p) => steps(p).StepAdjustments.map((a: any) => a.ScalingAdjustment)),
    );
    expect(biggest).toBeGreaterThanOrEqual(50);
  });

  it('alarms use 1-minute periods; scale-out reacts after 1 period, scale-in after 15 idle minutes', () => {
    const alarms = Object.values(prodOn.findResources('AWS::CloudWatch::Alarm')) as any[];
    const text = JSON.stringify(alarms);
    expect(text).toContain('ApproximateNumberOfMessagesVisible');
    expect(text).toContain('ApproximateAgeOfOldestMessage');
    expect(text).toContain('ApproximateNumberOfMessagesNotVisible');
    expect(SENDER_IDLE_MINUTES_BEFORE_SCALE_IN).toBe(15);
    const inAlarm = alarms.find((a) => a.Properties.EvaluationPeriods === 15);
    expect(inAlarm).toBeDefined();
    expect(alarms.some((a) => a.Properties.EvaluationPeriods === 10)).toBe(false);
    const outAlarms = alarms.filter((a) => a.Properties.EvaluationPeriods === 1);
    expect(outAlarms.length).toBeGreaterThanOrEqual(2);
    // Every metric in the scaling alarms is sampled once a minute.
    const scalingAlarms = [inAlarm, ...outAlarms].filter((a) => a.Properties.Metrics);
    expect(scalingAlarms.length).toBeGreaterThanOrEqual(2);
    for (const a of scalingAlarms) {
      for (const m of a.Properties.Metrics) {
        if (m.MetricStat) expect(m.MetricStat.Period).toBe(60);
      }
    }
  });
});

describe('SenderStack grants', () => {
  const sqsActions = [
    'sqs:ChangeMessageVisibility',
    'sqs:DeleteMessage',
    'sqs:GetQueueAttributes',
    'sqs:ReceiveMessage',
    'sqs:SendMessage',
  ];

  it('SQS receive/delete/change-visibility/send on both queues', () => {
    expect(grantedOn(dev, 'send-urgent')).toEqual(sqsActions);
    expect(grantedOn(dev, 'send-standard')).toEqual(sqsActions);
  });

  it('DynamoDB: full claim cycle on idempotency, only UpdateItem/DeleteItem on the cell index', () => {
    expect(grantedOn(dev, 'table/idem')).toEqual([
      'dynamodb:DeleteItem',
      'dynamodb:GetItem',
      'dynamodb:PutItem',
      'dynamodb:UpdateItem',
    ]);
    expect(grantedOn(dev, 'table/cells')).toEqual(['dynamodb:DeleteItem', 'dynamodb:UpdateItem']);
  });

  it('rds-data on the cluster, secrets, ssm', () => {
    expect(grantedOn(dev, 'cluster:db')).toEqual([
      'rds-data:BeginTransaction',
      'rds-data:CommitTransaction',
      'rds-data:ExecuteStatement',
      'rds-data:RollbackTransaction',
    ]);
    const json = JSON.stringify(
      statements(dev)
        .filter((s) => actionsOf(s).includes('secretsmanager:GetSecretValue'))
        .map((s) => resourcesOf(s)),
    );
    // Its own database role secret only, not the other roles' or the admin one.
    expect(json).toContain('secret:db-sender-AbCdEf');
    expect(json).not.toContain('secret:db-api-');
    expect(json).not.toContain('secret:db-ops-');
    // No alert key in Secrets Manager: the sender verifies with the KMS public key only.
    expect(json).not.toContain('alert-keys');
    expect(grantedOn(dev, ALERT_KEY_ARN)).toEqual(['kms:GetPublicKey']);
    expect(json).toContain('ApnsKey');
    expect(json).toContain('FcmServiceAccount');
    expect(grantedOn(dev, 'parameter/relay/x/kill')).toEqual(['ssm:GetParameter']);
  });

  it('no wildcard actions, and no wildcard resources on the task role', () => {
    for (const s of statements(dev)) {
      for (const a of actionsOf(s)) expect(a).not.toMatch(/\*/);
      if (
        JSON.stringify(s.Action).includes('sqs:') ||
        JSON.stringify(s.Action).includes('dynamodb:')
      ) {
        expect(resourcesOf(s)).not.toContain('*');
      }
    }
  });
});

it('Aws pseudo parameters can stand in for placeholder ARNs (bin/relay.ts keeps compiling)', () => {
  expect(typeof Aws.ACCOUNT_ID).toBe('string');
});

describe('SenderStack FCM fleet rate', () => {
  const envOf = (t: Template, name: string): any => {
    const defs = Object.values(t.findResources('AWS::ECS::TaskDefinition')) as any[];
    const env = defs[0].Properties.ContainerDefinitions[0].Environment as any[];
    return env.find((e) => e.Name === name)?.Value;
  };

  it('tells the task its own cluster and service and the per-project fleet budget (default 9000)', () => {
    expect(envOf(prod, 'ECS_CLUSTER')).toBe('relay-prod-sender');
    expect(envOf(prod, 'ECS_SERVICE')).toBe('relay-prod-sender');
    expect(envOf(prod, 'FCM_PROJECT_FLEET_RATE_PER_SEC')).toBe('9000');
    expect(envOf(prod, 'FCM_FLEET_SIZE_FALLBACK')).toBe(String(SENDER_MAX_TASKS));
    expect(envOf(prod, 'FCM_PROJECT_RATE_PER_SEC')).toBeUndefined();
  });

  it('context fcmProjectFleetRatePerSec overrides the budget and is validated', () => {
    expect(
      envOf(synth('dev', { fcmProjectFleetRatePerSec: '4500' }), 'FCM_PROJECT_FLEET_RATE_PER_SEC'),
    ).toBe('4500');
    expect(() => synth('dev', { fcmProjectFleetRatePerSec: '0' })).toThrow(/positive/);
  });

  it('grants only ecs:DescribeServices, on the sender service ARN', () => {
    expect(grantedOn(prod, ':service/')).toEqual(['ecs:DescribeServices']);
    const s = statements(prod).find((x) => actionsOf(x).includes('ecs:DescribeServices'));
    expect(JSON.stringify(s.Resource)).toContain('service/');
    expect(JSON.stringify(s.Resource)).not.toContain('*');
  });
});

describe('SenderStack alarms (OPS-2)', () => {
  const alarm = (t: Template, re: string) =>
    Object.values(
      t.findResources('AWS::CloudWatch::Alarm', {
        Properties: { AlarmDescription: Match.stringLikeRegexp(re) },
      }),
    ) as any[];

  it('running tasks below desired for 3 minutes, from Container Insights', () => {
    const [a] = alarm(prod, 'below desired');
    expect(a.Properties.EvaluationPeriods).toBe(3);
    expect(a.Properties.DatapointsToAlarm).toBe(3);
    const json = JSON.stringify(a.Properties.Metrics);
    expect(json).toContain('ECS/ContainerInsights');
    expect(json).toContain('RunningTaskCount');
    expect(json).toContain('DesiredTaskCount');
    expect(json).toContain('relay-prod-sender');
  });

  it('PermanentBreakerOpen, GaveUp and DeadTargetDropped alarm at >= 1 on Relay/Sender', () => {
    for (const [id, metric] of [
      ['PermanentBreakerOpenAlarm', 'PermanentBreakerOpen'],
      ['GaveUpAlarm', 'GaveUp'],
      ['DeadTargetDroppedAlarm', 'DeadTargetDropped'],
    ] as const) {
      const found = Object.entries(prod.findResources('AWS::CloudWatch::Alarm')).find(([k]) =>
        k.startsWith(id),
      );
      expect(found, id).toBeDefined();
      const props = (found?.[1] as any).Properties;
      expect(props.Threshold).toBe(1);
      expect(props.ComparisonOperator).toBe('GreaterThanOrEqualToThreshold');
      const json = JSON.stringify(props);
      expect(json).toContain('Relay/Sender');
      expect(json).toContain(metric);
    }
  });
});
