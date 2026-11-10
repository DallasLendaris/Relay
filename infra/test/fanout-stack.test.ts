import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { ConfigStack } from '../lib/config-stack.js';
import { DataStack } from '../lib/data-stack.js';
import { FanoutStack, SHARD_MEMORY_MB } from '../lib/fanout-stack.js';

const IDEM_ARN = 'arn:aws:dynamodb:us-west-2:111111111111:table/idem';

function synth(
  stage: 'dev' | 'prod',
  shardReservedConcurrency?: number,
  context: Record<string, string> = {},
) {
  const app = new App({ context });
  const env = { region: 'us-west-2' };
  const data = new DataStack(app, `data-${stage}`, { env, stage });
  const config = new ConfigStack(app, `config-${stage}`, { env, stage });
  const stack = new FanoutStack(app, `fanout-${stage}`, {
    env,
    stage,
    db: {
      clusterArn: data.clusterArn,
      roleSecretArns: data.roleSecretArns,
      databaseName: data.databaseName,
    },
    tokenKeysSecretArn: config.tokenKeysSecretArn,
    alertSigning: config.alertSigning,
    killSwitchParamName: config.killSwitchParamName,
    killSwitchParamArn: config.killSwitchParamArn,
    idempotencyTable: { name: 'idem', arn: IDEM_ARN },
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

/** The pre-scale env names the planner reads (SCALER_ENV in fleet-scaler.ts). */
function scalerEnv(): string[] {
  const src = readFileSync(
    fileURLToPath(new URL('../../backend/src/scale/fanout/fleet-scaler.ts', import.meta.url)),
    'utf8',
  );
  const m = /SCALER_ENV = \[([^\]]*)\]/.exec(src);
  if (!m) throw new Error('SCALER_ENV not found');
  return [...(m[1] as string).matchAll(/'([A-Z0-9_]+)'/g)].map((x) => x[1] as string);
}

describe('FanoutStack', () => {
  const { t, stack } = synth('dev');

  it('has plan, urgent and standard queues with DLQs and SSE, plus the shard DLQ and the paused queue with its DLQ', () => {
    t.resourceCountIs('AWS::SQS::Queue', 9);
    const queues = Object.values(t.findResources('AWS::SQS::Queue')) as Res[];
    expect(queues.every((q) => q.Properties.SqsManagedSseEnabled === true)).toBe(true);
    // Send queues: 30 s visibility.
    expect(queues.filter((q) => q.Properties.VisibilityTimeout === 30)).toHaveLength(2);
    // plan, urgent, standard and paused have a RedrivePolicy; the five DLQs have none.
    expect(queues.filter((q) => q.Properties.RedrivePolicy)).toHaveLength(4);
    t.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 30,
      RedrivePolicy: { maxReceiveCount: 100, deadLetterTargetArn: Match.anyValue() },
    });
  });

  it('send queues allow 100 receives before the DLQ (pause and leasing cause receives); none use 5', () => {
    const queues = Object.values(t.findResources('AWS::SQS::Queue')) as Res[];
    const send = queues.filter((q) => q.Properties.VisibilityTimeout === 30);
    expect(send).toHaveLength(2);
    for (const q of send) expect(q.Properties.RedrivePolicy.maxReceiveCount).toBe(100);
    expect(queues.some((q) => q.Properties.RedrivePolicy?.maxReceiveCount === 5)).toBe(false);
  });

  it('shard worker: Node 22, ARM64, 60 s, 2048 MB, reserved concurrency 500', () => {
    expect(fnByHandler(t, 'shardHandler').Properties).toMatchObject({
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Timeout: 60,
      MemorySize: 2048,
      ReservedConcurrentExecutions: 500,
    });
    expect(SHARD_MEMORY_MB).toBe(2048);
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
    // The planner (plan queue) and the paused consumer (paused queue).
    t.resourceCountIs('AWS::Lambda::EventSourceMapping', 2);
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
    // It may send to the paused queue (parking a paused AlertReady) and nowhere else.
    const sends = st.filter((s) => actionsOf([s]).includes('sqs:SendMessage'));
    expect(sends).toHaveLength(1);
    expect(JSON.stringify(sends[0]?.Resource)).toContain('ShardPausedQueue');
    expect(JSON.stringify(sends[0]?.Resource)).not.toMatch(/Urgent|Standard|PlanQueue/);
  });

  it('planner may only UpdateService/DescribeServices, on the sender service, nothing wider', () => {
    const st = statementsFor(t, 'plannerHandler');
    const ecs = st.filter((s) => actionsOf([s]).some((a) => a.startsWith('ecs:')));
    expect(ecs).toHaveLength(1);
    expect([...actionsOf(ecs)].sort()).toEqual(['ecs:DescribeServices', 'ecs:UpdateService']);
    const resource = JSON.stringify(ecs[0]?.Resource);
    expect(resource).toContain('service/relay-dev-sender/relay-dev-sender');
    expect(resource).not.toContain('"*"');
    expect(ecs[0]?.Resource).not.toBe('*');
    // The shard worker and redrive functions get no ECS access.
    for (const h of ['shardHandler', 'redriveHandler', 'sendRedriveHandler']) {
      expect(actionsOf(statementsFor(t, h)).some((a) => a.startsWith('ecs:'))).toBe(false);
    }
  });

  it('planner pre-scale env: off (max 0) until senderEnabled, on with the stage limits after', () => {
    const env = (tt: Template) =>
      fnByHandler(tt, 'plannerHandler').Properties.Environment.Variables as Record<string, any>;
    const off = env(t);
    expect(off.SENDER_MAX_TASKS).toBe('0');
    expect(off.SENDER_SERVICE_NAME).toBe('relay-dev-sender');
    expect(JSON.stringify(off.SENDER_CLUSTER_ARN)).toContain('cluster/relay-dev-sender');
    expect(Number(off.SENDER_SCALE_PER_SHARD)).toBeGreaterThan(0);
    expect(Number(off.SENDER_RECIPIENTS_PER_TASK_PER_MIN)).toBeGreaterThan(0);
    const devOn = env(synth('dev', undefined, { senderEnabled: 'true' }).t);
    expect(devOn.SENDER_MAX_TASKS).toBe('10');
    const prodOn = env(synth('prod', undefined, { senderEnabled: 'true' }).t);
    expect(prodOn.SENDER_MAX_TASKS).toBe('100');
    expect(prodOn.SENDER_SERVICE_NAME).toBe('relay-prod-sender');
  }, 60_000);

  it('shard worker may invoke itself (continuations), by explicit name, not by its own ARN token', () => {
    const st = statementsFor(t, 'shardHandler');
    const invoke = st.filter((s) => actionsOf([s]).includes('lambda:InvokeFunction'));
    expect(invoke).toHaveLength(1);
    expect(JSON.stringify(invoke[0]?.Resource)).toContain('function:relay-dev-shard-worker');
    expect(fnByHandler(t, 'shardHandler').Properties.FunctionName).toBe('relay-dev-shard-worker');
  });

  it('send redrive function: native DLQ move for urgent and standard only', () => {
    const f = fnByHandler(t, 'sendRedriveHandler').Properties;
    expect(Object.keys(f.Environment.Variables).sort()).toEqual([
      'STANDARD_DLQ_ARN',
      'URGENT_DLQ_ARN',
    ]);
    const st = statementsFor(t, 'sendRedriveHandler');
    const actions = actionsOf(st);
    for (const a of [
      'sqs:StartMessageMoveTask',
      'sqs:ReceiveMessage',
      'sqs:DeleteMessage',
      'sqs:SendMessage',
    ]) {
      expect(actions).toContain(a);
    }
    const move = st.find((s) => actionsOf([s]).includes('sqs:StartMessageMoveTask'));
    const res = JSON.stringify(move?.Resource);
    expect(res).toContain('UrgentDlq');
    expect(res).toContain('StandardDlq');
    expect(res).not.toContain('PlanDlq');
    expect(res).not.toContain('ShardDlq');
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
    expect(actions).not.toContain('dynamodb:Scan');
    expect(actions).not.toContain('dynamodb:DeleteItem');
    // Item reads and writes only on the idempotency table (dense-cell tombstones), never the index.
    for (const s of st.filter((x) =>
      actionsOf([x]).some((a) => ['dynamodb:PutItem', 'dynamodb:GetItem'].includes(a)),
    )) {
      expect(s.Resource).toBe(IDEM_ARN);
    }
    expect(actions).toEqual(
      expect.arrayContaining(['dynamodb:PutItem', 'dynamodb:GetItem', 'dynamodb:UpdateItem']),
    );
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
    expect(planner.sort()).toEqual(
      [...envList('PLANNER_ENV'), 'SHARD_FUNCTION_NAME', ...scalerEnv()].sort(),
    );
    expect(shard.sort()).toEqual([...envList('SHARD_ENV')].sort());
  });

  it('redrive function can read and delete the shard DLQ and invoke the shard function', () => {
    const actions = actionsOf(statementsFor(t, 'redriveHandler'));
    for (const a of ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'lambda:InvokeFunction']) {
      expect(actions).toContain(a);
    }
  });

  it('alarms on each of the five DLQs, including urgent and standard send and the paused DLQ', () => {
    // 5 DLQ alarms + the outbox-stuck alarm + the drainer-not-running alarm.
    t.resourceCountIs('AWS::CloudWatch::Alarm', 7);
    const alarms = Object.values(t.findResources('AWS::CloudWatch::Alarm')) as Res[];
    const watched = alarms.map((a) => JSON.stringify(a.Properties.Dimensions ?? ''));
    for (const dlq of ['UrgentDlq', 'StandardDlq', 'PlanDlq', 'ShardDlq', 'ShardPausedDlq']) {
      expect(watched.some((w) => w.includes(dlq))).toBe(true);
    }
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Namespace: 'AWS/SQS',
      Threshold: 0,
      ComparisonOperator: 'GreaterThanThreshold',
    });
  });

  describe('REL-2: paused queue and consumer', () => {
    it('names the queue relay-<stage>-shard-paused, with a DLQ after 3 receives (receives are rare: it re-parks as a new message)', () => {
      t.hasResourceProperties('AWS::SQS::Queue', {
        QueueName: 'relay-dev-shard-paused',
        RedrivePolicy: { maxReceiveCount: 3, deadLetterTargetArn: Match.anyValue() },
      });
      expect(synth('prod').stack.pausedQueueUrl).toBeDefined();
    }, 60_000);

    it('the shard worker and planner get PAUSED_QUEUE_URL; the shard worker may send to it', () => {
      for (const h of ['shardHandler', 'plannerHandler']) {
        expect(fnByHandler(t, h).Properties.Environment.Variables).toHaveProperty(
          'PAUSED_QUEUE_URL',
        );
      }
      const sends = statementsFor(t, 'shardHandler').filter((s) =>
        JSON.stringify(s.Resource).includes('ShardPausedQueue'),
      );
      expect(actionsOf(sends)).toContain('sqs:SendMessage');
    });

    it('the consumer: reads the paused queue (batch 10, item failures), checks the switch, re-dispatches', () => {
      t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
        BatchSize: 10,
        FunctionResponseTypes: ['ReportBatchItemFailures'],
      });
      const consumer = Object.values(t.findResources('AWS::Lambda::Function')).find((f) =>
        JSON.stringify((f as Res).Properties.Environment?.Variables ?? {}).includes(
          'PLAN_QUEUE_URL',
        ),
      ) as Res;
      expect(Object.keys(consumer.Properties.Environment.Variables).sort()).toEqual([
        'KILL_SWITCH_PARAM',
        'PAUSED_QUEUE_URL',
        'PLAN_QUEUE_URL',
        'SHARD_FUNCTION_NAME',
      ]);
    });
  });

  describe('REL-3: outbox drainer', () => {
    const drainerFn = () =>
      Object.values(t.findResources('AWS::Lambda::Function')).find(
        (f) =>
          JSON.stringify((f as Res).Properties.Environment?.Variables ?? {}).includes(
            'CELL_INDEX_TABLE',
          ) && !(f as Res).Properties.Handler.includes('shardHandler'),
      ) as Res;

    it('runs every 5 minutes on dev and every minute on prod (PERF-5)', () => {
      t.hasResourceProperties('AWS::Events::Rule', { ScheduleExpression: 'rate(5 minutes)' });
      synth('prod').t.hasResourceProperties('AWS::Events::Rule', {
        ScheduleExpression: 'rate(1 minute)',
      });
    });

    it('has DB access and cell-index write access', () => {
      const vars = drainerFn().Properties.Environment.Variables;
      expect(Object.keys(vars).sort()).toEqual([
        'CELL_INDEX_TABLE',
        'DB_CLUSTER_ARN',
        'DB_NAME',
        'DB_SECRET_ARN',
        'LATEJOIN_QUEUE_URL',
      ]);
      const roleRef = (drainerFn().Properties.Role['Fn::GetAtt'] as string[])[0];
      const st = (Object.values(t.findResources('AWS::IAM::Policy')) as Res[])
        .filter((p) => JSON.stringify(p.Properties.Roles).includes(String(roleRef)))
        .flatMap((p) => p.Properties.PolicyDocument.Statement as Array<Record<string, any>>);
      const actions = actionsOf(st);
      for (const a of ['rds-data:ExecuteStatement', 'dynamodb:PutItem', 'dynamodb:DeleteItem']) {
        expect(actions).toContain(a);
      }
      // It may send to the late-join queue (and nothing else) to ask for a late join.
      const sends = st.filter((x) => actionsOf([x]).includes('sqs:SendMessage'));
      expect(sends).toHaveLength(1);
      expect(JSON.stringify(sends[0]?.['Resource'])).toContain('-latejoin');
    });

    it('alarms (a plain alarm) when an outbox row reaches 10 attempts', () => {
      t.hasResourceProperties('AWS::CloudWatch::Alarm', {
        Namespace: 'Relay/CellIndex',
        MetricName: 'OutboxMaxAttempts',
        Statistic: 'Maximum',
        Threshold: 9,
        ComparisonOperator: 'GreaterThanThreshold',
      });
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
