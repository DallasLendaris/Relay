import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { MAX_RECEIVE_COUNT } from '../lib/delivery-stack.js';
import { envNamesRead } from './handler-env.js';
import { synthAll } from './helpers.js';

const all = synthAll('dev');

/** Finds the one Lambda whose env contains the given variable name. */
function envOf(t: Template, marker: string): string[] {
  const fns = Object.values(t.findResources('AWS::Lambda::Function')) as any[];
  const hit = fns.filter((f) => marker in (f.Properties.Environment?.Variables ?? {}));
  expect(hit).toHaveLength(1);
  return Object.keys(hit[0].Properties.Environment.Variables).sort();
}

function allActions(t: Template): string[] {
  const policies = Object.values(t.findResources('AWS::IAM::Policy')) as any[];
  return policies
    .flatMap((p) => p.Properties.PolicyDocument.Statement)
    .flatMap((s) => (Array.isArray(s.Action) ? s.Action : [s.Action]));
}

/**
 * Reads an exported numeric constant (like `export const LEASE_MS = 45_000;`) from a backend
 * source file. The infra project cannot import backend code (separate tsconfig rootDir).
 */
function backendConstant(backendPath: string, name: string): number {
  const src = readFileSync(
    fileURLToPath(new URL(`../../backend/src/${backendPath}`, import.meta.url)),
    'utf8',
  );
  const m = new RegExp(`export const ${name} = ([\\d_]+);`).exec(src);
  if (!m) throw new Error(`${name} not found in ${backendPath}`);
  return Number((m[1] as string).replaceAll('_', ''));
}

/** DynamoDB document-client commands -> the IAM action each one needs. */
const DYNAMO_ACTION: Record<string, string> = {
  GetCommand: 'dynamodb:GetItem',
  PutCommand: 'dynamodb:PutItem',
  UpdateCommand: 'dynamodb:UpdateItem',
  DeleteCommand: 'dynamodb:DeleteItem',
  QueryCommand: 'dynamodb:Query',
  ScanCommand: 'dynamodb:Scan',
};

/** The IAM actions a source file needs, read from the SDK commands it constructs. */
function dynamoActionsUsed(backendPath: string): string[] {
  const src = readFileSync(
    fileURLToPath(new URL(`../../backend/src/${backendPath}`, import.meta.url)),
    'utf8',
  );
  const commands = [...src.matchAll(/new (\w+Command)\(/g)].map((m) => m[1] as string);
  return [...new Set(commands.map((c) => DYNAMO_ACTION[c] ?? `UNMAPPED:${c}`))].sort();
}

/**
 * Env contract, derived from the handler source (see handler-env.ts) so it cannot drift:
 *  - every name the handler reads must be set by the stack, or the Lambda fails at runtime;
 *  - every name the stack sets must be read by the handler, or it is dead config.
 * `setButUnread` lists names the stack sets that the handler does not read (yet).
 * `reverse: false` skips the second check for a handler that is being rewritten.
 */
function checkEnvContract(
  label: string,
  t: Template,
  marker: string,
  handlerPath: string,
  options: { setButUnread?: string[]; reverse?: boolean } = {},
) {
  const { setButUnread = [], reverse = true } = options;
  const read = envNamesRead(handlerPath);
  const set = envOf(t, marker);
  describe(`${label} env contract`, () => {
    it('handler source yields env names', () => {
      expect(read).toContain('DB_CLUSTER_ARN');
    });
    it('stack sets every name the handler reads', () => {
      expect(set).toEqual(expect.arrayContaining(read));
    });
    if (reverse) {
      it('handler reads every name the stack sets', () => {
        expect(set.filter((n) => !setButUnread.includes(n))).toEqual(read);
      });
    } else {
      it.todo(`${label}: handler reads every name the stack sets (check-in handler not merged)`);
    }
  });
}

describe('DeliveryStack', () => {
  const t = all.delivery;

  it('wires the queue to a DLQ after 100 receives, with SSE and a 30 s visibility timeout', () => {
    t.resourceCountIs('AWS::SQS::Queue', 2);
    t.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 30,
      SqsManagedSseEnabled: true,
      RedrivePolicy: { maxReceiveCount: 100, deadLetterTargetArn: Match.anyValue() },
    });
    // 100 receives x 30 s is about 50 minutes of kill-switch pause before the DLQ.
    expect(MAX_RECEIVE_COUNT * 30).toBe(3000);
  });

  it('keeps the worker lease between the worker timeout and 2x the visibility timeout', () => {
    const LEASE_MS = backendConstant('delivery/worker.ts', 'LEASE_MS');
    const WORKER_TIMEOUT_MS = backendConstant('delivery/worker.ts', 'WORKER_TIMEOUT_MS');
    const VISIBILITY_TIMEOUT_MS = backendConstant('delivery/worker.ts', 'VISIBILITY_TIMEOUT_MS');
    // The numbers in the stack and the worker must agree...
    t.hasResourceProperties('AWS::Lambda::Function', { Timeout: WORKER_TIMEOUT_MS / 1000 });
    t.hasResourceProperties('AWS::SQS::Queue', { VisibilityTimeout: VISIBILITY_TIMEOUT_MS / 1000 });
    // ...so a live attempt is never doubled and a crashed one is retried at the second redelivery.
    expect(LEASE_MS).toBeGreaterThan(WORKER_TIMEOUT_MS);
    expect(LEASE_MS).toBeLessThan(2 * VISIBILITY_TIMEOUT_MS);
  });

  it('alarms when the DLQ has any visible message', () => {
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Namespace: 'AWS/SQS',
      Threshold: 0,
      ComparisonOperator: 'GreaterThanThreshold',
    });
  });

  it('reports batch item failures with batch size 10', () => {
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 10,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
    });
  });

  it('configures the worker: Node 22, ARM64, 20 s, reserved concurrency 10', () => {
    t.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Timeout: 20,
      ReservedConcurrentExecutions: 10,
    });
  });

  it('has an on-demand idempotency table with TTL on "ttl"', () => {
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
      TimeToLiveSpecification: { AttributeName: 'ttl', Enabled: true },
    });
  });

  checkEnvContract('worker', t, 'EXPO_ACCESS_TOKEN_SECRET_ARN', 'delivery/handler.ts');

  it('worker can use the idempotency table, alert keys and kill switch (and nothing wider)', () => {
    const actions = allActions(t);
    // Derived from the SDK commands in the store, so a new call without a grant fails here.
    const used = dynamoActionsUsed('adapters/aws/dynamo-idempotency-store.ts');
    expect(used.filter((a) => a.startsWith('UNMAPPED'))).toEqual([]);
    expect(used).toEqual(
      expect.arrayContaining([
        'dynamodb:DeleteItem',
        'dynamodb:GetItem',
        'dynamodb:PutItem',
        'dynamodb:UpdateItem',
      ]),
    );
    for (const a of used) expect(actions).toContain(a);
    for (const a of [
      'dynamodb:UpdateItem', // markSent
      'ssm:GetParameter',
      'secretsmanager:GetSecretValue',
      'sqs:ReceiveMessage',
    ]) {
      expect(actions).toContain(a);
    }
    expect(actions).not.toContain('dynamodb:Query');
  });
});

describe('IngestStack wiring', () => {
  const t = all.ingest;

  it('state table has TTL on expiresAt', () => {
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      KeySchema: [{ AttributeName: 'sourceId', KeyType: 'HASH' }],
      TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
    });
  });

  it('keeps reserved concurrency 1', () => {
    t.hasResourceProperties('AWS::Lambda::Function', { ReservedConcurrentExecutions: 1 });
  });

  checkEnvContract('ingest', t, 'RAW_BUCKET', 'ingest/handler.ts');

  it('grants the specific actions it needs', () => {
    const actions = allActions(t);
    for (const a of [
      'rds-data:ExecuteStatement',
      'secretsmanager:GetSecretValue',
      'sqs:SendMessage',
      'ssm:GetParameter',
    ]) {
      expect(actions).toContain(a);
    }
  });

  it('no longer touches the idempotency table', () => {
    expect(JSON.stringify(t.toJSON())).not.toContain('Idempotency');
  });
});

describe('ApiStack', () => {
  const t = all.api;

  // Until the check-in handler is merged, the handler reads fewer names than the stack sets,
  // so only the "stack sets what the handler reads" half runs; the other half is a todo.
  checkEnvContract('api', t, 'PLACE_INDEX_NAME', 'api/handler.ts', {
    reverse: envNamesRead('api/handler.ts').includes('CHECKINS_TABLE'),
  });

  it('sets the check-in env names regardless of the handler', () => {
    expect(envOf(t, 'PLACE_INDEX_NAME')).toEqual(
      expect.arrayContaining(['CHECKINS_TABLE', 'CHECKIN_TOTALS_TABLE', 'TOKEN_KEYS_SECRET_ARN']),
    );
  });

  it('has check-in tables with the agreed keys, on demand', () => {
    t.resourceCountIs('AWS::DynamoDB::Table', 2);
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [
        { AttributeName: 'alertId', KeyType: 'HASH' },
        { AttributeName: 'deviceId', KeyType: 'RANGE' },
      ],
    });
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [
        { AttributeName: 'alertId', KeyType: 'HASH' },
        { AttributeName: 'h3Cell', KeyType: 'RANGE' },
      ],
    });
  });

  it('keeps check-in tables with PITR on prod only', () => {
    const prod = synthAll('prod').api;
    for (const table of Object.values(prod.findResources('AWS::DynamoDB::Table')) as any[]) {
      expect(table.DeletionPolicy).toBe('Retain');
      expect(table.Properties.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled).toBe(
        true,
      );
    }
    for (const table of Object.values(t.findResources('AWS::DynamoDB::Table')) as any[]) {
      expect(table.DeletionPolicy).toBe('Delete');
    }
  });

  it('can read the token keys and put/get/update check-ins, but not delete or scan', () => {
    const actions = allActions(t);
    for (const a of [
      'dynamodb:PutItem',
      'dynamodb:GetItem',
      'dynamodb:UpdateItem',
      'secretsmanager:GetSecretValue',
    ]) {
      expect(actions).toContain(a);
    }
    for (const a of ['dynamodb:DeleteItem', 'dynamodb:Scan', 'dynamodb:Query']) {
      expect(actions).not.toContain(a);
    }
  });

  it('has a route for every non-agency path in the OpenAPI spec', () => {
    const keys = Object.values(t.findResources('AWS::ApiGatewayV2::Route')).map(
      (r: any) => r.Properties.RouteKey as string,
    );
    expect(keys.sort()).toEqual(
      [
        'POST /devices',
        'PUT /devices/me/push-token',
        'POST /places',
        'GET /places',
        'DELETE /places/{placeId}',
        'GET /alerts',
        'GET /alerts/{alertId}',
        'POST /alerts/{alertId}/checkins',
      ].sort(),
    );
    expect(keys.some((k) => k.includes('/agency'))).toBe(false);
  });

  it('throttles the $default stage at 50 rps, burst 100', () => {
    t.hasResourceProperties('AWS::ApiGatewayV2::Stage', {
      StageName: '$default',
      DefaultRouteSettings: { ThrottlingRateLimit: 50, ThrottlingBurstLimit: 100 },
    });
  });

  it('creates the Esri single-use place index and allows searching it', () => {
    t.hasResourceProperties('AWS::Location::PlaceIndex', {
      IndexName: 'relay-dev-places',
      DataSource: 'Esri',
      DataSourceConfiguration: { IntendedUse: 'SingleUse' },
    });
    expect(allActions(t)).toContain('geo:SearchPlaceIndexForText');
  });

  it('outputs the API URL', () => {
    expect(Object.keys(t.toJSON().Outputs)).toContain('ApiUrl');
  });
});

describe('ConfigStack', () => {
  const t = all.config;

  it('has the delivery kill switch defaulting to "running"', () => {
    t.hasResourceProperties('AWS::SSM::Parameter', {
      Name: '/relay/dev/delivery-kill-switch',
      Type: 'String',
      Value: 'running',
    });
  });

  it('has keyring-shaped key secrets and an Expo secret', () => {
    t.resourceCountIs('AWS::SecretsManager::Secret', 3);
    t.hasResourceProperties('AWS::SecretsManager::Secret', {
      Name: 'relay/dev/token-keys',
      SecretString: Match.stringLikeRegexp('"current":"k1".*"keys"'),
    });
    t.hasResourceProperties('AWS::SecretsManager::Secret', { Name: 'relay/dev/alert-keys' });
    t.hasResourceProperties('AWS::SecretsManager::Secret', {
      Name: 'relay/dev/expo-access-token',
    });
  });
});

describe('resource names', () => {
  // Properties that hold a physical name. If a stack sets one explicitly, it must carry
  // the stage, or dev and prod collide in one account. Tables, buckets and queues must not
  // set a name at all (CDK generates a unique one and passes it to the Lambda through env).
  const NAME_KEYS = [
    'TableName',
    'BucketName',
    'QueueName',
    'FunctionName',
    'IndexName',
    'LogGroupName',
    'AlarmName',
    'RoleName',
    'Name', // Secrets Manager and SSM
  ];
  const GENERATED_ONLY = ['AWS::DynamoDB::Table', 'AWS::S3::Bucket', 'AWS::SQS::Queue'];

  for (const stage of ['dev', 'prod'] as const) {
    it(`every explicit physical name contains "${stage}"`, () => {
      for (const [stack, t] of Object.entries(synthAll(stage))) {
        const resources = Object.entries(t.toJSON().Resources ?? {}) as [string, any][];
        for (const [id, r] of resources) {
          for (const key of NAME_KEYS) {
            const value = r.Properties?.[key];
            if (typeof value !== 'string') continue;
            expect(value, `${stack}/${id}.${key}`).toContain(stage);
            expect(GENERATED_ONLY, `${stack}/${id} must not set ${key}`).not.toContain(r.Type);
          }
        }
      }
    });
  }
});

describe('all stacks', () => {
  it('have no IAM action of "*" or a service wildcard', () => {
    for (const [name, t] of Object.entries(synthAll('prod'))) {
      const actions = allActions(t);
      expect(
        actions.filter((a) => a === '*' || a.endsWith(':*')),
        name,
      ).toEqual([]);
    }
  });
});
