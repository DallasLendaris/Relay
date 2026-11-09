import { App, Aws } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { SENDER_MAX_TASKS, SKIP_IMAGE_CONTEXT, SenderStack } from '../lib/sender-stack.js';

const queue = (name: string) => ({
  name,
  arn: `arn:aws:sqs:us-west-2:111111111111:${name}`,
  url: `https://sqs.us-west-2.amazonaws.com/111111111111/${name}`,
});

function synth(stage: 'dev' | 'prod') {
  // The context flag keeps the Docker image asset out of the test (no hashing the repo, no Docker).
  const app = new App({ context: { [SKIP_IMAGE_CONTEXT]: 'true' } });
  const stack = new SenderStack(app, `sender-${stage}`, {
    env: { region: 'us-west-2' },
    stage,
    db: {
      clusterArn: 'arn:aws:rds:us-west-2:111111111111:cluster:db',
      secretArn: 'arn:aws:secretsmanager:us-west-2:111111111111:secret:db-AbCdEf',
      databaseName: 'relay',
    },
    urgentQueue: queue('send-urgent'),
    standardQueue: queue('send-standard'),
    idempotencyTableName: 'idem',
    idempotencyTableArn: 'arn:aws:dynamodb:us-west-2:111111111111:table/idem',
    cellIndexTableName: 'cells',
    cellIndexTableArn: 'arn:aws:dynamodb:us-west-2:111111111111:table/cells',
    alertKeysSecretArn: 'arn:aws:secretsmanager:us-west-2:111111111111:secret:alert-keys-AbCdEf',
    killSwitchParamName: '/relay/x/kill',
    killSwitchParamArn: 'arn:aws:ssm:us-west-2:111111111111:parameter/relay/x/kill',
  });
  return Template.fromStack(stack);
}

const dev = synth('dev');
const prod = synth('prod');

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
      'ALERT_KEYS_SECRET_ARN',
      'KILL_SWITCH_PARAM',
      'APNS_KEY_SECRET_ARN',
      'FCM_SERVICE_ACCOUNT_SECRET_ARN',
    ]) {
      expect(env, k).toHaveProperty(k);
    }
    expect(env['URGENT_QUEUE_URL']).toContain('send-urgent');
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

describe('SenderStack scaling', () => {
  it('scales between 1 (prod) / 0 (dev) and 50 tasks', () => {
    expect(SENDER_MAX_TASKS).toBe(50);
    prod.hasResourceProperties('AWS::ApplicationAutoScaling::ScalableTarget', {
      MinCapacity: 1,
      MaxCapacity: 50,
      ServiceNamespace: 'ecs',
    });
    dev.hasResourceProperties('AWS::ApplicationAutoScaling::ScalableTarget', {
      MinCapacity: 0,
      MaxCapacity: 50,
    });
  });

  it('has step scaling on queue depth and age (out) and an idle policy (in)', () => {
    const policies = Object.values(
      prod.findResources('AWS::ApplicationAutoScaling::ScalingPolicy'),
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
    // Biggest out step reaches +30 tasks.
    const biggest = Math.max(
      ...outs.flatMap((p) => steps(p).StepAdjustments.map((a: any) => a.ScalingAdjustment)),
    );
    expect(biggest).toBeGreaterThanOrEqual(30);
  });

  it('alarms watch SQS depth, urgent age, and idleness', () => {
    const alarms = Object.values(prod.findResources('AWS::CloudWatch::Alarm')) as any[];
    const text = JSON.stringify(alarms);
    expect(text).toContain('ApproximateNumberOfMessagesVisible');
    expect(text).toContain('ApproximateAgeOfOldestMessage');
    expect(text).toContain('ApproximateNumberOfMessagesNotVisible');
    const inAlarm = alarms.find((a) => a.Properties.EvaluationPeriods === 10);
    expect(inAlarm).toBeDefined();
    const outAlarms = alarms.filter((a) => a.Properties.EvaluationPeriods === 1);
    expect(outAlarms.length).toBeGreaterThanOrEqual(2);
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
    const secretStatement = statements(dev).find((s) =>
      actionsOf(s).includes('secretsmanager:GetSecretValue'),
    );
    const json = JSON.stringify(resourcesOf(secretStatement));
    expect(json).toContain('secret:db-AbCdEf');
    expect(json).toContain('alert-keys');
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
