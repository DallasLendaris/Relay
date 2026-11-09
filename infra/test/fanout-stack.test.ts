import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { ConfigStack } from '../lib/config-stack.js';
import { DataStack } from '../lib/data-stack.js';
import { FanoutStack } from '../lib/fanout-stack.js';

function synth(stage: 'dev' | 'prod', shardReservedConcurrency?: number) {
  const app = new App();
  const env = { region: 'us-west-2' };
  const data = new DataStack(app, `data-${stage}`, { env, stage });
  const config = new ConfigStack(app, `config-${stage}`, { env, stage });
  const stack = new FanoutStack(app, `fanout-${stage}`, {
    env,
    stage,
    db: {
      clusterArn: data.clusterArn,
      secretArn: data.secretArn,
      databaseName: data.databaseName,
    },
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    alertKeysSecretArn: config.alertKeysSecretArn,
    killSwitchParamName: config.killSwitchParamName,
    killSwitchParamArn: config.killSwitchParamArn,
    ...(shardReservedConcurrency ? { shardReservedConcurrency } : {}),
  });
  return { stack, t: Template.fromStack(stack) };
}

type Res = { Properties: Record<string, any> };
const fnByHandler = (t: Template, handler: string): Res => {
  const hit = Object.values(t.findResources('AWS::Lambda::Function')).filter((f) =>
    (f as Res).Properties.Handler?.endsWith(`.${handler}`),
  );
  expect(hit).toHaveLength(1);
  return hit[0] as Res;
};

/** Statements of the policy attached to the role of the function with this handler. */
function statementsFor(t: Template, handler: string): Array<Record<string, any>> {
  const fn = fnByHandler(t, handler);
  const roleRef = (fn.Properties.Role['Fn::GetAtt'] as string[])[0];
  const policies = Object.values(t.findResources('AWS::IAM::Policy')) as Res[];
  return policies
    .filter((p) => JSON.stringify(p.Properties.Roles).includes(String(roleRef)))
    .flatMap((p) => p.Properties.PolicyDocument.Statement);
}
const actionsOf = (st: Array<Record<string, any>>) =>
  st.flatMap((s) => (Array.isArray(s.Action) ? s.Action : [s.Action])) as string[];

/** Env var names a handler's wiring reads, from the source (PLANNER_ENV / SHARD_ENV lists). */
function envList(name: string): string[] {
  const src = readFileSync(
    fileURLToPath(new URL('../../backend/src/scale/fanout/handlers.ts', import.meta.url)),
    'utf8',
  );
  const m = new RegExp(`const ${name} = \\[([^\\]]*)\\]`).exec(src);
  if (!m) throw new Error(`${name} not found`);
  return [...(m[1] as string).matchAll(/'([A-Z0-9_]+)'/g)].map((x) => x[1] as string);
}

describe('FanoutStack', () => {
  const { t, stack } = synth('dev');

  it('has plan, urgent and standard queues with DLQs and SSE, plus the shard DLQ', () => {
    t.resourceCountIs('AWS::SQS::Queue', 7);
    const queues = Object.values(t.findResources('AWS::SQS::Queue')) as Res[];
    expect(queues.every((q) => q.Properties.SqsManagedSseEnabled === true)).toBe(true);
    // Send queues: 30 s visibility.
    expect(queues.filter((q) => q.Properties.VisibilityTimeout === 30)).toHaveLength(2);
    // plan, urgent and standard have a RedrivePolicy; the four DLQs have none.
    expect(queues.filter((q) => q.Properties.RedrivePolicy)).toHaveLength(3);
    t.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 30,
      RedrivePolicy: { maxReceiveCount: 5, deadLetterTargetArn: Match.anyValue() },
    });
  });

  it('shard worker: Node 22, ARM64, 60 s, 1024 MB, reserved concurrency 500', () => {
    expect(fnByHandler(t, 'shardHandler').Properties).toMatchObject({
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Timeout: 60,
      MemorySize: 1024,
      ReservedConcurrentExecutions: 500,
    });
  });

  it('shard concurrency is configurable', () => {
    const f = fnByHandler(synth('dev', 1200).t, 'shardHandler').Properties;
    expect(f.ReservedConcurrentExecutions).toBe(1200);
  }, 60_000);

  it('shard async invoke: max age 1 h, 2 retries, failures to the shard DLQ', () => {
    t.hasResourceProperties('AWS::Lambda::EventInvokeConfig', {
      MaximumEventAgeInSeconds: 3600,
      MaximumRetryAttempts: 2,
      DestinationConfig: { OnFailure: { Destination: Match.anyValue() } },
    });
  });

  it('planner is SQS-triggered (batch 1, item failures); the shard worker has no SQS trigger', () => {
    t.resourceCountIs('AWS::Lambda::EventSourceMapping', 1);
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
    });
    expect(fnByHandler(t, 'plannerHandler').Properties).toMatchObject({
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
    });
  });

  it('cell index table: h3Cell + sk, on demand; PITR on prod only', () => {
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      KeySchema: [
        { AttributeName: 'h3Cell', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
      AttributeDefinitions: [
        { AttributeName: 'h3Cell', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
    });
    synth('prod').t.hasResourceProperties('AWS::DynamoDB::Table', {
      PointInTimeRecoverySpecification: { PointInTimeRecoveryEnabled: true },
    });
    const dev = Object.values(t.findResources('AWS::DynamoDB::Table')) as Res[];
    expect(dev[0]?.Properties.PointInTimeRecoverySpecification?.PointInTimeRecoveryEnabled).toBe(
      false,
    );
  }, 60_000);

  it('planner may invoke the shard function only, and cannot touch the cell index or send queues', () => {
    const st = statementsFor(t, 'plannerHandler');
    const invoke = st.filter((s) => actionsOf([s]).includes('lambda:InvokeFunction'));
    expect(invoke).toHaveLength(1);
    expect(JSON.stringify(invoke[0]?.Resource)).toContain('ShardFunction');
    const actions = actionsOf(st);
    for (const a of [
      'rds-data:ExecuteStatement',
      'secretsmanager:GetSecretValue',
      'ssm:GetParameter',
    ]) {
      expect(actions).toContain(a);
    }
    expect(actions).not.toContain('dynamodb:Query');
    expect(actions).not.toContain('sqs:SendMessage');
  });

  it('shard worker: Query on the cell index, send to the two send queues, nothing wider', () => {
    const st = statementsFor(t, 'shardHandler');
    const actions = actionsOf(st);
    for (const a of [
      'dynamodb:Query',
      'sqs:SendMessage',
      'rds-data:ExecuteStatement',
      'secretsmanager:GetSecretValue',
      'ssm:GetParameter',
    ]) {
      expect(actions).toContain(a);
    }
    for (const a of [
      'dynamodb:PutItem',
      'dynamodb:Scan',
      'dynamodb:GetItem',
      'lambda:InvokeFunction',
    ]) {
      expect(actions).not.toContain(a);
    }
    const send = st.filter((s) => actionsOf([s]).includes('sqs:SendMessage'));
    const resources = JSON.stringify(send.map((s) => s.Resource));
    expect(resources).toContain('UrgentQueue');
    expect(resources).toContain('StandardQueue');
    expect(resources).not.toContain('PlanQueue');
    const query = st.find((s) => actionsOf([s]).includes('dynamodb:Query'));
    expect(JSON.stringify(query?.Resource)).toContain('CellIndexTable');
  });

  it('no wildcard actions anywhere', () => {
    for (const handler of ['plannerHandler', 'shardHandler', 'redriveHandler']) {
      for (const a of actionsOf(statementsFor(t, handler))) expect(a.endsWith('*')).toBe(false);
    }
  });

  it('sets exactly the env vars the handler wiring reads', () => {
    const planner = Object.keys(fnByHandler(t, 'plannerHandler').Properties.Environment.Variables);
    const shard = Object.keys(fnByHandler(t, 'shardHandler').Properties.Environment.Variables);
    expect(planner.sort()).toEqual([...envList('PLANNER_ENV'), 'SHARD_FUNCTION_NAME'].sort());
    expect(shard.sort()).toEqual([...envList('SHARD_ENV')].sort());
  });

  it('redrive function can read and delete the shard DLQ and invoke the shard function', () => {
    const actions = actionsOf(statementsFor(t, 'redriveHandler'));
    for (const a of ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'lambda:InvokeFunction']) {
      expect(actions).toContain(a);
    }
  });

  it('alarms on each of the four DLQs', () => {
    t.resourceCountIs('AWS::CloudWatch::Alarm', 4);
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Namespace: 'AWS/SQS',
      Threshold: 0,
      ComparisonOperator: 'GreaterThanThreshold',
    });
  });

  it('exposes send queue ARNs and URLs and the cell index table for other stacks', () => {
    for (const k of [
      'urgentQueueArn',
      'urgentQueueUrl',
      'standardQueueArn',
      'standardQueueUrl',
      'planQueueUrl',
      'cellIndexTableArn',
      'cellIndexTableName',
    ] as const) {
      expect(stack[k]).toBeDefined();
    }
  });
});
