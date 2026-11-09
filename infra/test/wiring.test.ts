import { Match, type Template } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
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

describe('DeliveryStack', () => {
  const t = all.delivery;

  it('wires the queue to a DLQ after 5 receives, with SSE and a 6x visibility timeout', () => {
    t.resourceCountIs('AWS::SQS::Queue', 2);
    t.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 180, // 6 x 30 s worker timeout
      SqsManagedSseEnabled: true,
      RedrivePolicy: { maxReceiveCount: 5, deadLetterTargetArn: Match.anyValue() },
    });
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

  it('configures the worker: Node 22, ARM64, 30 s, reserved concurrency 10', () => {
    t.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Timeout: 30,
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

  it('worker env has exactly the contract names', () => {
    expect(envOf(t, 'EXPO_ACCESS_TOKEN_SECRET_ARN')).toEqual(
      ['DB_CLUSTER_ARN', 'DB_NAME', 'DB_SECRET_ARN', 'EXPO_ACCESS_TOKEN_SECRET_ARN'].sort(),
    );
  });
});

describe('IngestStack wiring', () => {
  const t = all.ingest;

  it('state table has TTL on expiresAt', () => {
    t.hasResourceProperties('AWS::DynamoDB::Table', {
      TableName: 'alerts-state',
      KeySchema: [{ AttributeName: 'sourceId', KeyType: 'HASH' }],
      TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
    });
  });

  it('keeps reserved concurrency 1', () => {
    t.hasResourceProperties('AWS::Lambda::Function', { ReservedConcurrentExecutions: 1 });
  });

  it('env has exactly the contract names', () => {
    expect(envOf(t, 'RAW_BUCKET')).toEqual(
      [
        'RAW_BUCKET',
        'STATE_TABLE',
        'NWS_USER_AGENT',
        'POLL_INTERVAL_SECONDS',
        'POLLS_PER_INVOCATION',
        'DB_CLUSTER_ARN',
        'DB_SECRET_ARN',
        'DB_NAME',
        'DELIVERY_QUEUE_URL',
        'IDEMPOTENCY_TABLE',
        'TOKEN_KEYS_SECRET_ARN',
        'ALERT_KEYS_SECRET_ARN',
        'KILL_SWITCH_PARAM',
      ].sort(),
    );
  });

  it('grants the specific actions it needs', () => {
    const actions = allActions(t);
    for (const a of [
      'rds-data:ExecuteStatement',
      'secretsmanager:GetSecretValue',
      'sqs:SendMessage',
      'ssm:GetParameter',
      'dynamodb:PutItem',
    ]) {
      expect(actions).toContain(a);
    }
  });
});

describe('ApiStack', () => {
  const t = all.api;

  it('env has exactly the contract names', () => {
    expect(envOf(t, 'PLACE_INDEX_NAME')).toEqual(
      ['DB_CLUSTER_ARN', 'DB_NAME', 'DB_SECRET_ARN', 'PLACE_INDEX_NAME'].sort(),
    );
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
