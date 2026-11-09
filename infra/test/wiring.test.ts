import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { MAX_RECEIVE_COUNT } from '../lib/delivery-stack.js';
import { backendSource, envNamesRead } from './handler-env.js';
import { synthAll, synthApp } from './helpers.js';

const all = synthAll('dev');

/** Finds the one Lambda whose env contains the given variable name. */
function envOf(t: Template, marker: string): string[] {
  const fns = Object.values(t.findResources('AWS::Lambda::Function')) as any[];
  const hit = fns.filter((f) => marker in (f.Properties.Environment?.Variables ?? {}));
  expect(hit).toHaveLength(1);
  return Object.keys(hit[0].Properties.Environment.Variables).sort();
}

/** The one Lambda whose Handler ends with `.${handler}` (for files with several handlers). */
function envOfHandler(t: Template, handler: string): string[] {
  const fns = Object.values(t.findResources('AWS::Lambda::Function')) as any[];
  const hit = fns.filter((f) => String(f.Properties.Handler).endsWith(`.${handler}`));
  expect(hit).toHaveLength(1);
  return Object.keys(hit[0].Properties.Environment?.Variables ?? {}).sort();
}

/** Env of the sender container (the only ECS container definition in the SenderStack). */
function senderEnv(t: Template): string[] {
  const defs = Object.values(t.findResources('AWS::ECS::TaskDefinition')) as any[];
  expect(defs).toHaveLength(1);
  const containers = defs[0].Properties.ContainerDefinitions as any[];
  expect(containers).toHaveLength(1);
  return containers[0].Environment.map((e: any) => e.Name as string).sort();
}

function allActions(t: Template): string[] {
  const policies = Object.values(t.findResources('AWS::IAM::Policy')) as any[];
  return policies
    .flatMap((p) => p.Properties.PolicyDocument.Statement)
    .flatMap((s) => (Array.isArray(s.Action) ? s.Action : [s.Action]));
}

function statements(t: Template): any[] {
  const policies = Object.values(t.findResources('AWS::IAM::Policy')) as any[];
  return policies.flatMap((p) => p.Properties.PolicyDocument.Statement);
}
const actionsOf = (st: any): string[] => (Array.isArray(st.Action) ? st.Action : [st.Action]);
const resourcesOf = (st: any): any[] => (Array.isArray(st.Resource) ? st.Resource : [st.Resource]);

/** Actions granted on any resource whose JSON contains `needle`, sorted and de-duplicated. */
function grantedOnResource(t: Template, needle: string): string[] {
  return [
    ...new Set(
      statements(t)
        .filter((st) => {
          const resources = JSON.stringify(resourcesOf(st));
          // Stream grants name the stream ARN, not the table: keep them apart.
          const isStream = resources.includes('StreamArn') && !needle.includes('StreamArn');
          return st.Effect === 'Allow' && resources.includes(needle) && !isStream;
        })
        .flatMap(actionsOf),
    ),
  ].sort();
}

/** The resources an action is granted on. */
function grantedResources(t: Template, action: string): any[] {
  return statements(t)
    .filter((st) => actionsOf(st).includes(action))
    .flatMap(resourcesOf);
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
 * Env contract, derived from the source (see handler-env.ts) so it cannot drift:
 *  - every name the code reads must be set by the stack (unless `optional`), or it fails at runtime;
 *  - every name the stack sets must be read by the code, or it is dead config.
 * `pending` lists names the integration contract requires but the code does not read yet (the
 * backend wiring lands in a parallel change); re-run this suite after merging it, then the list
 * can shrink to nothing. `optional` lists names the code reads only in an alternate mode.
 */
function checkEnvContract(
  label: string,
  set: string[],
  read: string[],
  options: { pending?: string[]; optional?: string[]; contract?: string[] } = {},
) {
  const { pending = [], optional = [], contract = [] } = options;
  describe(`${label} env contract`, () => {
    it('source yields env names', () => {
      expect(read.length).toBeGreaterThan(0);
    });
    it('stack sets every name the code reads', () => {
      expect(set).toEqual(expect.arrayContaining(read.filter((n) => !optional.includes(n))));
    });
    it('code reads every name the stack sets', () => {
      expect(set.filter((n) => !pending.includes(n) && !read.includes(n))).toEqual([]);
    });
    if (contract.length > 0) {
      it('sets the names the integration contract lists', () => {
        expect(set).toEqual(expect.arrayContaining(contract));
      });
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

  checkEnvContract(
    'worker',
    envOf(t, 'EXPO_ACCESS_TOKEN_SECRET_ARN'),
    envNamesRead('delivery/handler.ts'),
  );

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

  const INGEST_CONTRACT = [
    'FANOUT_MODE',
    'PLAN_QUEUE_URL',
    'PUBLIC_ALERTS_BUCKET',
    'PUBLIC_ALERTS_REGION',
    'ZONE_PACK_BUCKET',
  ];
  checkEnvContract(
    'ingest',
    envOf(t, 'RAW_BUCKET'),
    [...envNamesRead('ingest/handler.ts'), ...envNamesRead('ingest/pipeline.ts')],
    { contract: INGEST_CONTRACT },
  );

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

  checkEnvContract('api', envOf(t, 'PLACE_INDEX_NAME'), envNamesRead('api/handler.ts'), {
    // Optional: unset means the default (30 days); counsel decides the real period.
    // FCM_PROJECT_IDS: set only when the fcmProjectIds context names more than the default project.
    optional: ['CHECKIN_RETENTION_DAYS', 'FCM_PROJECT_IDS'],
    contract: [
      'DB_CLUSTER_ARN',
      'DB_SECRET_ARN',
      'DB_NAME',
      'PLACE_INDEX_NAME',
      'CHECKINS_TABLE',
      'TOKEN_KEYS_SECRET_ARN',
      'DEVICE_CREDENTIAL_KEYS_SECRET_ARN',
      'CELL_INDEX_TABLE',
    ],
  });

  it('does not set CHECKIN_TOTALS_TABLE on the API', () => {
    expect(envOf(t, 'PLACE_INDEX_NAME')).not.toContain('CHECKIN_TOTALS_TABLE');
  });

  it('has check-in tables with the agreed keys, on demand', () => {
    t.resourceCountIs('AWS::DynamoDB::Table', 2);
    // checkins: pk only, stream of new images feeding the aggregator.
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      StreamSpecification: { StreamViewType: 'NEW_IMAGE' },
    });
    // totals: pk, GSI byAlert (alertId, projection ALL), TTL on "ttl".
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      TimeToLiveSpecification: { AttributeName: 'ttl', Enabled: true },
      GlobalSecondaryIndexes: [
        {
          IndexName: 'byAlert',
          KeySchema: [{ AttributeName: 'alertId', KeyType: 'HASH' }],
          Projection: { ProjectionType: 'ALL' },
        },
      ],
    });
    const tables = Object.values(t.findResources('AWS::DynamoDB::Table')) as any[];
    expect(tables.filter((x) => x.Properties.StreamSpecification)).toHaveLength(1);
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

  it('grants exactly the cell-index, check-in and keyring actions the API needs', () => {
    const on = (needle: string) => grantedOnResource(t, needle);
    expect(on('CheckinsTable')).toEqual([
      'dynamodb:BatchWriteItem',
      'dynamodb:GetItem',
      'dynamodb:PutItem',
      'dynamodb:Query',
    ]);
    // The cell index is a prop (FanoutStack), so its ARN appears as a cross-stack import.
    expect(on('CellIndexTable')).toEqual(
      [
        'dynamodb:BatchWriteItem',
        'dynamodb:DeleteItem',
        'dynamodb:GetItem',
        'dynamodb:PutItem',
        'dynamodb:Query',
        'dynamodb:UpdateItem',
      ].sort(),
    );
    // Totals are written only by the aggregator (Put + Update, which TransactWriteItems needs).
    expect(on('CheckinTotalsTable')).toEqual(['dynamodb:PutItem', 'dynamodb:UpdateItem']);
    expect(allActions(t)).toContain('secretsmanager:GetSecretValue');
    expect(allActions(t)).not.toContain('dynamodb:Scan');
  });

  it('can read both the token keyring and the device-credential keyring', () => {
    const secrets = grantedResources(t, 'secretsmanager:GetSecretValue').map((r) =>
      JSON.stringify(r),
    );
    // The two keyring ARNs reach the API as plain strings, so they resolve to different imports.
    expect(new Set(secrets).size).toBeGreaterThanOrEqual(3); // db, token keys, credential keys
  });

  it('has a route for every non-agency path in the OpenAPI spec', () => {
    const keys = Object.values(t.findResources('AWS::ApiGatewayV2::Route')).map(
      (r: any) => r.Properties.RouteKey as string,
    );
    expect(keys.sort()).toEqual(
      [
        'POST /devices',
        'PUT /devices/me/push-token',
        'DELETE /devices/me',
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
    t.resourceCountIs('AWS::SecretsManager::Secret', 4); // plus the origin-verify secret
    t.hasResourceProperties('AWS::SecretsManager::Secret', {
      Name: 'relay/dev/token-keys',
      SecretString: Match.stringLikeRegexp('"current":"k1".*"keys"'),
    });
    t.hasResourceProperties('AWS::SecretsManager::Secret', {
      Name: 'relay/dev/device-credential-keys',
      SecretString: Match.stringLikeRegexp('"current":"k1".*"keys"'),
    });
    t.hasResourceProperties('AWS::SecretsManager::Secret', {
      Name: 'relay/dev/expo-access-token',
    });
  });
});

describe('ConfigStack device-credential keyring', () => {
  it('has the same placeholder shape as the token keys, which the loader rejects', () => {
    const secrets = Object.values(all.config.findResources('AWS::SecretsManager::Secret')) as any[];
    const ring = (name: string) =>
      secrets.find((x) => x.Properties.Name === `relay/dev/${name}`)?.Properties.SecretString;
    expect(ring('device-credential-keys')).toBe(ring('token-keys'));
    expect(JSON.parse(ring('device-credential-keys'))).toEqual({
      current: 'k1',
      keys: { k1: 'REPLACE_ME_RUN_SEED_KEYS' },
    });
    // The backend keyring loader throws on any "REPLACE_ME" key, so an unseeded API cannot sign.
    expect(backendSource('config/secrets.ts')).toContain("startsWith('REPLACE_ME')");
  });

  it('is seeded and rotated by the seed-keys script', () => {
    const script = readFileSync(
      fileURLToPath(new URL('../scripts/seed-keys.ts', import.meta.url)),
      'utf8',
    );
    expect(script).toContain("'device-credential-keys'");
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
            // The one deliberate exception: the paused queue has a fixed, stage-named identity
            // (relay-<stage>-shard-paused) so operators and runbooks can find it.
            if (key === 'QueueName' && value === `relay-${stage}-shard-paused`) continue;
            expect(GENERATED_ONLY, `${stack}/${id} must not set ${key}`).not.toContain(r.Type);
          }
        }
      }
    });
  }
});

describe('all stacks', () => {
  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: no IAM action contains a wildcard`, () => {
      for (const [name, t] of Object.entries(synthAll(stage))) {
        expect(
          allActions(t).filter((a) => a.includes('*')),
          name,
        ).toEqual([]);
      }
    });

    it(`${stage}: no NAT gateway anywhere`, () => {
      for (const [name, t] of Object.entries(synthAll(stage))) {
        t.resourceCountIs('AWS::EC2::NatGateway', 0);
        expect(JSON.stringify(t.toJSON()), name).not.toContain('NatGateway');
      }
    });
  }
});

describe('stack dependency order', () => {
  // config and data are roots; monitoring needs config; delivery and fanout need them; the late-join
  // stack (it needs fanout's send queues) owns the intent queue the API sends to, so it comes before
  // the API; sender and ingest need fanout (and delivery); the edge stack comes last. Equal ranks
  // never depend on each other.
  const RANK: Record<string, number> = {
    config: 0,
    data: 0,
    monitoring: 1, // reads the kill-switch name from config; everything with alarms notifies it
    delivery: 2,
    fanout: 2,
    latejoin: 3,
    sender: 3,
    ingest: 3,
    api: 4,
    edge: 5,
  };

  for (const stage of ['dev', 'prod'] as const) {
    it(`${stage}: synthesizes, and every dependency points to an earlier rank (no cycles)`, () => {
      const { stacks } = synthApp(stage);
      for (const [key, stack] of Object.entries(stacks)) {
        const rank = RANK[key] as number;
        for (const dep of stack.dependencies) {
          const depKey = Object.entries(stacks).find(([, s]) => s === dep)?.[0];
          if (!depKey) continue; // cross-region export helper stacks are created by CDK itself
          expect(RANK[depKey] as number, `${key} -> ${depKey}`).toBeLessThan(rank);
        }
      }
    });

    it(`${stage}: the edge stack is in us-east-1 and reaches the API across regions`, () => {
      const { stacks, templates } = synthApp(stage);
      expect(stacks.edge.region).toBe('us-east-1');
      expect(stacks.api.region).toBe('us-west-2');
      expect(stacks.edge.dependencies).toContain(stacks.ingest);
      templates.edge.resourceCountIs('AWS::CloudFront::Distribution', 1);
    });
  }

  it('passes the fan-out stack real queues and the cell index to the sender and API', () => {
    const { stacks, templates } = synthApp('dev');
    expect(stacks.sender.dependencies).toContain(stacks.fanout);
    expect(stacks.api.dependencies).toContain(stacks.fanout);
    expect(stacks.ingest.dependencies).toContain(stacks.fanout);
    // No placeholder ARNs remain in the sender template.
    const json = JSON.stringify(templates.sender.toJSON());
    expect(json).not.toContain('relay-dev-send-urgent');
    expect(json).not.toContain('relay-dev-cell-index');
  });
});

/** SDK command -> IAM actions it needs. */
const COMMAND_ACTIONS: Record<string, string[]> = {
  GetCommand: ['dynamodb:GetItem'],
  PutCommand: ['dynamodb:PutItem'],
  UpdateCommand: ['dynamodb:UpdateItem'],
  DeleteCommand: ['dynamodb:DeleteItem'],
  QueryCommand: ['dynamodb:Query'],
  ScanCommand: ['dynamodb:Scan'],
  BatchWriteCommand: ['dynamodb:BatchWriteItem'],
  // A transaction is authorized item by item: each Put needs PutItem, each Update UpdateItem.
  TransactWriteCommand: ['dynamodb:PutItem', 'dynamodb:UpdateItem'],
  SendMessageCommand: ['sqs:SendMessage'],
  SendMessageBatchCommand: ['sqs:SendMessage'],
  ReceiveMessageCommand: ['sqs:ReceiveMessage'],
  DeleteMessageCommand: ['sqs:DeleteMessage'],
  DeleteMessageBatchCommand: ['sqs:DeleteMessage'],
  ChangeMessageVisibilityCommand: ['sqs:ChangeMessageVisibility'],
  InvokeCommand: ['lambda:InvokeFunction'],
  PutObjectCommand: ['s3:PutObject'],
  GetObjectCommand: ['s3:GetObject'],
  SearchPlaceIndexForTextCommand: ['geo:SearchPlaceIndexForText'],
};

/** The IAM actions the given backend sources need, read from the SDK commands they construct. */
function actionsUsed(...paths: string[]): string[] {
  const used = new Set<string>();
  for (const path of paths) {
    const commands = [...backendSource(path).matchAll(/new (\w+Command)\(/g)].map(
      (m) => m[1] as string,
    );
    expect(commands.length, `${path} constructs SDK commands`).toBeGreaterThan(0);
    for (const c of commands) {
      const actions = COMMAND_ACTIONS[c];
      expect(actions, `${path}: ${c} has no entry in COMMAND_ACTIONS`).toBeDefined();
      for (const a of actions ?? []) used.add(a);
    }
  }
  return [...used].sort();
}

describe('IAM grants cover every call the adapters make', () => {
  it('API: check-in store on the checkins table, cell index on the cell index', () => {
    const t = all.api;
    for (const a of actionsUsed('adapters/aws/dynamo-checkin-store.ts')) {
      expect(grantedOnResource(t, 'CheckinsTable'), a).toContain(a);
    }
    for (const a of actionsUsed('scale/cell-index/dynamo-cell-index.ts')) {
      expect(grantedOnResource(t, 'CellIndexTable'), a).toContain(a);
    }
    expect(grantedOnResource(t, 'CellIndexTable')).toEqual(
      expect.arrayContaining([
        'dynamodb:BatchWriteItem',
        'dynamodb:DeleteItem',
        'dynamodb:UpdateItem',
        'dynamodb:Query',
        'dynamodb:GetItem',
        'dynamodb:PutItem',
      ]),
    );
    expect(allActions(t)).toContain('geo:SearchPlaceIndexForText');
  });

  it('check-ins table: TTL on "ttl", byDevice index (keys only) for erasure, scoped grants', () => {
    for (const stage of [synthAll('dev').api, synthAll('prod').api]) {
      const checkins = (Object.values(stage.findResources('AWS::DynamoDB::Table')) as any[]).find(
        (x) => x.Properties.StreamSpecification,
      );
      expect(checkins.Properties.TimeToLiveSpecification).toEqual({
        AttributeName: 'ttl',
        Enabled: true,
      });
      expect(checkins.Properties.GlobalSecondaryIndexes).toEqual([
        {
          IndexName: 'byDevice',
          KeySchema: [{ AttributeName: 'deviceId', KeyType: 'HASH' }],
          Projection: { ProjectionType: 'KEYS_ONLY' },
        },
      ]);
      // The Query grant is on the index only; BatchWriteItem is on the table.
      const onIndex = statements(stage).filter((st) =>
        JSON.stringify(resourcesOf(st)).includes('/index/byDevice'),
      );
      expect(onIndex.flatMap(actionsOf)).toEqual(['dynamodb:Query']);
      expect(grantedOnResource(stage, 'CheckinsTable')).toEqual(
        expect.arrayContaining(['dynamodb:BatchWriteItem', 'dynamodb:Query']),
      );
    }
  });

  it('aggregator: Put and Update on the totals table', () => {
    const used = actionsUsed('scale/checkins/aggregator.ts');
    expect(used).toEqual(['dynamodb:PutItem', 'dynamodb:UpdateItem']);
    for (const a of used) expect(grantedOnResource(all.api, 'CheckinTotalsTable'), a).toContain(a);
  });

  it('ingest: delivery and plan queues, raw archive, public alerts, state table, zone pack', () => {
    const t = all.ingest;
    const actions = allActions(t);
    const used = actionsUsed(
      'adapters/aws/sqs-delivery-queue.ts',
      'adapters/aws/s3-raw-archive.ts',
      'scale/publish/s3-object-writer.ts',
      'adapters/aws/dynamo-seen-store.ts',
      'geo/s3-zone-provider.ts',
    );
    for (const a of used) expect(actions, a).toContain(a);
    expect(grantedOnResource(t, 'PlanQueue')).toContain('sqs:SendMessage');
    // Exact object scopes for the two prefixes.
    const s3 = statements(t).filter((st) => actionsOf(st).some((a) => a.startsWith('s3:')));
    const scope = (action: string) =>
      s3.filter((st) => actionsOf(st).includes(action)).flatMap(resourcesOf);
    expect(JSON.stringify(scope('s3:PutObject'))).toContain('/alerts/v1/*');
    expect(JSON.stringify(scope('s3:GetObject'))).toContain('/zones/v1/*');
    expect(JSON.stringify(scope('s3:GetObject'))).not.toContain('alerts/v1');
  });

  it('sender: queues, idempotency table, cell-index dead-target clearing, device store', () => {
    const t = all.sender;
    const actions = allActions(t);
    const used = actionsUsed(
      'adapters/aws/dynamo-idempotency-store.ts',
      'scale/sender/consumer.ts',
      'scale/sender/sqs-send-queue.ts',
    );
    for (const a of used) expect(actions, a).toContain(a);
    // Dead-target clearing goes through CellIndex.updateDevice, which issues UpdateItem.
    expect(grantedOnResource(t, 'CellIndexTable')).toContain('dynamodb:UpdateItem');
    // Device store path: Aurora through the Data API.
    expect(actions).toEqual(
      expect.arrayContaining(['rds-data:ExecuteStatement', 'rds-data:BeginTransaction']),
    );
  });
});

describe('check-in aggregator', () => {
  const t = all.api;
  const fn = () => {
    const hit = Object.values(t.findResources('AWS::Lambda::Function')).filter(
      (f: any) => 'CHECKIN_TOTALS_TABLE' in (f.Properties.Environment?.Variables ?? {}),
    ) as any[];
    expect(hit).toHaveLength(1);
    return hit[0];
  };

  it('is Node 22 on ARM64 with a 60 s timeout, running aggregator.ts', () => {
    expect(fn().Properties).toMatchObject({
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Timeout: 60,
      Handler: 'index.handler',
    });
    expect(backendSource('scale/checkins/aggregator.ts')).toContain('export const handler');
  });

  checkEnvContract(
    'aggregator',
    Object.keys(fn().Properties.Environment.Variables).sort(),
    envNamesRead('scale/checkins/aggregator.ts'),
    { contract: ['CHECKIN_TOTALS_TABLE'] },
  );

  it('reads the checkins stream in batches of 1000 with a 2 s window and per-item failures', () => {
    // The second mapping is the cell-index repair queue (tested below).
    t.resourceCountIs('AWS::Lambda::EventSourceMapping', 2);
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1000,
      MaximumBatchingWindowInSeconds: 2,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
      BisectBatchOnFunctionError: true,
      MaximumRetryAttempts: 10,
      ParallelizationFactor: 10,
      StartingPosition: 'TRIM_HORIZON',
      EventSourceArn: { 'Fn::GetAtt': [Match.stringLikeRegexp('^CheckinsTable'), 'StreamArn'] },
    });
  });

  it('sends exhausted batches to an SQS DLQ and alarms on it', () => {
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      DestinationConfig: {
        OnFailure: {
          Destination: { 'Fn::GetAtt': [Match.stringLikeRegexp('^AggregatorDlq'), 'Arn'] },
        },
      },
    });
    t.hasResourceProperties('AWS::SQS::Queue', { SqsManagedSseEnabled: true });
    expect(grantedOnResource(t, 'AggregatorDlq')).toContain('sqs:SendMessage');
    t.hasResourceProperties('AWS::CloudWatch::Alarm', {
      MetricName: 'ApproximateNumberOfMessagesVisible',
      Namespace: 'AWS/SQS',
    });
  });

  it('may read the stream (and only the stream) of the checkins table', () => {
    const streamActions = grantedOnResource(t, 'StreamArn');
    expect(streamActions).toEqual(
      expect.arrayContaining(['dynamodb:DescribeStream', 'dynamodb:GetRecords']),
    );
    expect(streamActions).not.toContain('dynamodb:PutItem');
  });
});

describe('fan-out and sender env contracts', () => {
  const fanout = all.fanout;
  const handlers = backendSource('scale/fanout/handlers.ts');

  it('the planner dispatches by Lambda invoke unless SHARD_DISPATCH=sqs (not set by the stack)', () => {
    expect(handlers).toContain("requireEnv(env, ['SHARD_FUNCTION_NAME'])");
    expect(envOfHandler(fanout, 'plannerHandler')).not.toContain('SHARD_DISPATCH');
  });

  checkEnvContract('planner', envOfHandler(fanout, 'plannerHandler'), [
    ...envNamesRead('scale/fanout/handlers.ts', { lists: ['PLANNER_ENV'] }),
    ...envNamesRead('scale/fanout/fleet-scaler.ts', { lists: ['SCALER_ENV'] }),
    'SHARD_FUNCTION_NAME',
  ]);
  checkEnvContract(
    'shard',
    envOfHandler(fanout, 'shardHandler'),
    envNamesRead('scale/fanout/handlers.ts', { lists: ['SHARD_ENV'] }),
  );
  checkEnvContract(
    'redrive',
    envOfHandler(fanout, 'redriveHandler'),
    envNamesRead('scale/fanout/redrive.ts'),
  );
  checkEnvContract(
    'send redrive',
    envOfHandler(fanout, 'sendRedriveHandler'),
    envNamesRead('scale/fanout/send-redrive.ts'),
  );
  checkEnvContract(
    'sender container',
    senderEnv(all.sender),
    envNamesRead('scale/sender/main.ts'),
    {
      optional: [
        'SENDER_CONCURRENCY',
        'PUBLIC_ALERT_BASE_URL',
        'FCM_PROJECT_IDS',
        'FCM_PROJECT_RATE_PER_SEC',
      ],
    },
  );
});

describe('sender CDN base URL', () => {
  it('PUBLIC_ALERT_BASE_URL is set from publicAlertBaseUrl and absent otherwise', () => {
    expect(senderEnv(synthAll('dev').sender)).not.toContain('PUBLIC_ALERT_BASE_URL');
    const t = synthAll('dev', { publicAlertBaseUrl: 'https://alerts.example.org' }).sender;
    const defs = Object.values(t.findResources('AWS::ECS::TaskDefinition')) as any[];
    const env = defs[0].Properties.ContainerDefinitions[0].Environment as any[];
    expect(env.find((e) => e.Name === 'PUBLIC_ALERT_BASE_URL')?.Value).toBe(
      'https://alerts.example.org',
    );
  });
});

describe('IngestStack fan-out mode and zone pack bucket', () => {
  const modeOf = (t: Template) =>
    (
      Object.values(t.findResources('AWS::Lambda::Function')).find(
        (f: any) => 'RAW_BUCKET' in (f.Properties.Environment?.Variables ?? {}),
      ) as any
    ).Properties.Environment.Variables.FANOUT_MODE;

  it('defaults to "inline" on dev and "queue" on prod, overridable', () => {
    expect(modeOf(synthAll('dev').ingest)).toBe('inline');
    expect(modeOf(synthAll('prod').ingest)).toBe('queue');
    expect(modeOf(synthAll('dev', { fanoutMode: 'queue' }).ingest)).toBe('queue');
    expect(modeOf(synthAll('prod', { fanoutMode: 'inline' }).ingest)).toBe('inline');
  });

  it('points PLAN_QUEUE_URL at the fan-out plan queue and ZONE_PACK_BUCKET at its own bucket', () => {
    const t = all.ingest;
    const fn = Object.values(t.findResources('AWS::Lambda::Function')).find(
      (f: any) => 'RAW_BUCKET' in (f.Properties.Environment?.Variables ?? {}),
    ) as any;
    const env = fn.Properties.Environment.Variables;
    expect(JSON.stringify(env.PLAN_QUEUE_URL)).toContain('PlanQueue');
    expect(env.ZONE_PACK_BUCKET).toEqual({ Ref: expect.stringMatching(/^ZonePackBucket/) });
  });

  it('exposes a private, SSL-only zone pack bucket in us-west-2', () => {
    const { stacks, templates } = synthApp('dev');
    expect(stacks.ingest.zonePackBucketName).toBeDefined();
    expect(stacks.ingest.region).toBe('us-west-2');
    const bucket = Object.entries(templates.ingest.findResources('AWS::S3::Bucket')).find(([id]) =>
      id.startsWith('ZonePackBucket'),
    );
    expect(bucket).toBeDefined();
    const [id, res] = bucket as [string, any];
    expect(res.Properties.PublicAccessBlockConfiguration).toEqual({
      BlockPublicAcls: true,
      BlockPublicPolicy: true,
      IgnorePublicAcls: true,
      RestrictPublicBuckets: true,
    });
    expect(res.Properties.BucketName).toBeUndefined(); // generated, so dev and prod cannot collide
    const policies = Object.values(
      templates.ingest.findResources('AWS::S3::BucketPolicy'),
    ) as any[];
    const forThis = policies.filter((p) => JSON.stringify(p.Properties.Bucket).includes(id));
    expect(forThis).toHaveLength(1);
    const st = forThis[0].Properties.PolicyDocument.Statement as any[];
    expect(
      st.some(
        (x) =>
          x.Effect === 'Deny' &&
          x.Condition?.Bool?.['aws:SecureTransport'] === 'false' &&
          JSON.stringify(x.Principal) === JSON.stringify({ AWS: '*' }),
      ),
    ).toBe(true);
    // Nothing is allowed to everyone.
    for (const x of st.filter((y) => y.Effect === 'Allow')) {
      expect(JSON.stringify(x.Principal)).not.toMatch(/"\*"/);
    }
  });

  it('is retained on prod', () => {
    const prod = synthAll('prod').ingest;
    const buckets = Object.entries(prod.findResources('AWS::S3::Bucket')) as [string, any][];
    for (const [id, b] of buckets.filter(([k]) => k.startsWith('ZonePackBucket'))) {
      expect(b.DeletionPolicy, id).toBe('Retain');
    }
  });
});
