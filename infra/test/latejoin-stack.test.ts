import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it, vi } from 'vitest';
import { envNamesRead } from './handler-env.js';
import { synthAll, synthApp } from './helpers.js';

vi.setConfig({ testTimeout: 60_000 });

const dev = synthAll('dev');
const prod = synthAll('prod');
const stacks = { dev: synthApp('dev').stacks, prod: synthApp('prod').stacks };

type Statement = { Action: string | string[]; Resource: unknown; Effect: string };
const statements = (t: Template): Statement[] =>
  Object.values(t.findResources('AWS::IAM::Policy')).flatMap(
    (p: any) => p.Properties.PolicyDocument.Statement as Statement[],
  );
const actionsOf = (s: Statement): string[] => [s.Action].flat();
const text = (v: unknown): string => JSON.stringify(v);
const sends = (t: Template) =>
  statements(t).filter((s) => actionsOf(s).includes('sqs:SendMessage'));

describe('SEC-C-1: the API cannot send to the urgent or standard send queues', () => {
  for (const [stage, t] of [
    ['dev', dev],
    ['prod', prod],
  ] as const) {
    it(`${stage}: no API statement grants any SQS action on a send queue`, () => {
      const api = statements(t.api);
      for (const s of api) {
        const touchesSqs = actionsOf(s).some((a) => a.startsWith('sqs:'));
        if (!touchesSqs) continue;
        expect(text(s.Resource), text(s)).not.toMatch(/Urgent|Standard/);
      }
      // It may send to the repair queue and the late-join intent queue. (The aggregator Lambda
      // in this stack also sends to its own DLQ.) Never to a send queue.
      const resources = text(sends(t.api).flatMap((s) => [s.Resource].flat()));
      expect(resources).toContain('IndexRepairQueue');
      expect(resources).toContain('LateJoinQueue');
      expect(resources).not.toMatch(/Urgent|Standard/);
    });

    it(`${stage}: the API template does not mention the send queues at all`, () => {
      const json = text(t.api.toJSON());
      expect(json).not.toMatch(/URGENT_QUEUE_URL|STANDARD_QUEUE_URL/);
      expect(json).not.toMatch(/UrgentQueue|StandardQueue/);
    });

    it(`${stage}: no wildcard SQS grant anywhere in the API stack`, () => {
      for (const s of statements(t.api)) {
        if (actionsOf(s).some((a) => a === 'sqs:*' || a === '*')) {
          throw new Error(`wildcard action: ${text(s)}`);
        }
        if (actionsOf(s).some((a) => a.startsWith('sqs:'))) {
          expect(text(s.Resource)).not.toContain('"*"');
        }
      }
    });
  }

  it('the API keeps token-key read access for verification, and nothing sends but the intent', () => {
    const grants = statements(dev.api).filter((s) =>
      actionsOf(s).includes('secretsmanager:GetSecretValue'),
    );
    expect(grants.length).toBeGreaterThan(0);
    expect(envNamesRead('api/handler.ts')).toContain('TOKEN_KEYS_SECRET_ARN');
  });

  it('only the API can send to the late-join queue', () => {
    for (const [name, t] of Object.entries(dev)) {
      if (name === 'api' || name === 'latejoin') continue; // latejoin: its own queue, asserted below
      for (const s of sends(t)) {
        expect(text(s.Resource), `${name}: ${text(s)}`).not.toContain('LateJoinQueue');
      }
    }
    // The outbox drainer (fanout stack) sends to it by its fixed name, and nothing else there does.
    const drainerSends = sends(dev.fanout).filter((s) => text(s.Resource).includes('-latejoin'));
    expect(drainerSends).toHaveLength(1);
    // The late-join Lambda re-enqueues (parks) paused intents to its own queue and no other.
    const own = statements(dev.latejoin).filter(
      (s) => text(s.Resource).includes('LateJoinQueue') && actionsOf(s).includes('sqs:SendMessage'),
    );
    expect(own).toHaveLength(1);
  });
});

describe('LateJoinStack', () => {
  const t = dev.latejoin;
  const fnEntries = () =>
    Object.values(t.findResources('AWS::Lambda::Function')).filter(
      (f: any) => f.Properties.Environment?.Variables?.IDEMPOTENCY_TABLE !== undefined,
    ) as any[];

  it('creates the late-join queue with a DLQ and an alarm on it', () => {
    // The repo lets CDK generate queue names (wiring.test.ts), which carry the stack name
    // relay-<stage>-latejoin, so the queue is relay-<stage>-latejoin-LateJoinQueue-<hash>.
    t.hasResourceProperties('AWS::SQS::Queue', {
      KmsMasterKeyId: Match.anyValue(), // customer-managed key (encryption.ts)
      RedrivePolicy: { maxReceiveCount: 20, deadLetterTargetArn: Match.anyValue() },
    });
    expect(stacks.dev.latejoin.stackName).toBe('dev-latejoin');
    expect(stacks.prod.latejoin.stackName).toBe('prod-latejoin');
    t.resourceCountIs('AWS::SQS::Queue', 2);
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Threshold: 0,
    });
  });

  it('has one Lambda, the only consumer of the queue, with partial batch failures and a cap', () => {
    expect(fnEntries()).toHaveLength(1);
    t.resourceCountIs('AWS::Lambda::EventSourceMapping', 1);
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      FunctionResponseTypes: ['ReportBatchItemFailures'],
      ScalingConfig: { MaximumConcurrency: 20 },
    });
  });

  it('reads exactly the env vars the handler needs', () => {
    const set = Object.keys(fnEntries()[0].Properties.Environment.Variables).sort();
    const read = envNamesRead('scale/latejoin/handler.ts');
    expect(read.length).toBeGreaterThan(0);
    expect(set).toEqual(expect.arrayContaining(read));
    expect(set.filter((n) => !read.includes(n))).toEqual([]);
  });

  it('has IAM limited to: intent queue (consume), send queues, rds-data, token keyring, alert public key, kill switch, idempotency', () => {
    const all = statements(t);
    const allActions = [...new Set(all.flatMap(actionsOf))].sort();
    expect(allActions).toEqual(
      [
        // consume the late-join queue (event source)
        'sqs:ReceiveMessage',
        'sqs:ChangeMessageVisibility',
        'sqs:GetQueueUrl',
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes',
        // send to the two send queues
        'sqs:SendMessage',
        // Aurora through the Data API (and its secret) plus the token keyring
        'rds-data:ExecuteStatement',
        'rds-data:BeginTransaction',
        'rds-data:CommitTransaction',
        'rds-data:RollbackTransaction',
        'secretsmanager:GetSecretValue',
        // the alert public key (verify only, never kms:Sign)
        'kms:GetPublicKey',
        // the kill switch
        'ssm:GetParameter',
        // idempotency claims
        'dynamodb:PutItem',
        'dynamodb:GetItem',
        'dynamodb:UpdateItem',
        'dynamodb:DeleteItem',
      ].sort(),
    );
    // SendMessage only on the urgent and standard queues and its own.
    const send = all.filter((s) => actionsOf(s).includes('sqs:SendMessage'));
    expect(send).toHaveLength(1);
    const resources = [send[0]?.Resource].flat();
    // The urgent and standard queues, plus its own queue (to park intents while paused).
    expect(resources).toHaveLength(3);
    expect(text(resources)).toMatch(/Urgent/);
    expect(text(resources)).toMatch(/Standard/);
    expect(text(resources)).toMatch(/LateJoinQueue/);
    // Two secrets: the database credentials and the token keyring (no alert key secret).
    const secret = all.find((s) => actionsOf(s).includes('secretsmanager:GetSecretValue'));
    expect(secret).toBeDefined();
    expect(
      all
        .filter((s) => actionsOf(s).includes('secretsmanager:GetSecretValue'))
        .flatMap((s) => [s.Resource].flat()),
    ).toHaveLength(2);
    // No wildcard resources.
    for (const s of all) expect(text(s.Resource)).not.toContain('"*"');
  });

  it('is wired to the app: the API depends on it, and it points at the real send queues', () => {
    const json = text(t.toJSON());
    expect(json).toMatch(/URGENT_QUEUE_URL/);
    expect(json).not.toContain('relay-dev-send-urgent'); // no placeholder names
    expect(text(dev.api.toJSON())).toMatch(/LATEJOIN_QUEUE_URL/);
  });
});
