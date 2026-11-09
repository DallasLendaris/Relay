import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { HttpApi, HttpMethod, HttpStage } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import {
  AttributeType,
  BillingMode,
  ProjectionType,
  StreamViewType,
  Table,
} from 'aws-cdk-lib/aws-dynamodb';
import { Architecture, Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource, SqsDlq, SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { CfnPlaceIndex } from 'aws-cdk-lib/aws-location';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { allow, dbEnv, grantDb, grantSecretRead, type DbRefs } from './grants.js';

export interface ApiStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  tokenKeysSecretArn: string;
  deviceCredentialKeysSecretArn: string;
  /** Cell-index table (FanoutStack): the API writes a device's places into it. */
  cellIndexTableName: string;
  cellIndexTableArn: string;
  /** Secret CloudFront sends as x-relay-origin-verify (ConfigStack). */
  originVerifySecretArn: string;
  /** Send queues (FanoutStack): late-joining devices are sent active alerts straight from the API. */
  urgentQueue: { arn: string; url: string };
  standardQueue: { arn: string; url: string };
  /**
   * Refuse requests without the origin header (REQUIRE_ORIGIN_HEADER). Default: prod true, dev
   * false (dev is reached directly by developers and the app's dev builds).
   */
  requireOriginHeader?: boolean;
  /** Check-ins table warm throughput (writes/s). Default: prod 40,000, dev none. */
  checkinsWarmWritesPerSecond?: number;
  /** Lambda reserved concurrency. Unset (the default everywhere) means unreserved. */
  reservedConcurrency?: { api?: number; aggregator?: number; repair?: number };
  /** Stage throttle on $default. Defaults: dev 50 rps / burst 100, prod 20,000 rps / burst 10,000. */
  throttle?: { rateLimit: number; burstLimit: number };
}

export const AGGREGATOR_TIMEOUT_SECONDS = 60;
export const REPAIR_TIMEOUT_SECONDS = 60;
/** The check-in burst: 1,000,000 devices in about 5 minutes peaks at 20,000 to 50,000 writes/s. */
export const DEFAULT_CHECKINS_WARM_WRITES = 40_000;

export const DEFAULT_THROTTLE = {
  dev: { rateLimit: 50, burstLimit: 100 },
  prod: { rateLimit: 20_000, burstLimit: 10_000 },
} as const;

/**
 * Every path in packages/shared/openapi/relay.yaml except /agency/*, which belongs
 * to the Cognito-protected agency API (added later). Keep in sync with the spec.
 */
export const API_ROUTES: ReadonlyArray<{ method: HttpMethod; path: string }> = [
  { method: HttpMethod.POST, path: '/devices' },
  { method: HttpMethod.PUT, path: '/devices/me/push-token' },
  { method: HttpMethod.POST, path: '/places' },
  { method: HttpMethod.GET, path: '/places' },
  { method: HttpMethod.DELETE, path: '/places/{placeId}' },
  { method: HttpMethod.GET, path: '/alerts' },
  { method: HttpMethod.GET, path: '/alerts/{alertId}' },
  { method: HttpMethod.POST, path: '/alerts/{alertId}/checkins' },
];

export class ApiStack extends Stack {
  /** Host name of the HTTP API, for the CloudFront origin. */
  readonly apiDomainName: string;
  /** Check-ins table (exposed so the delivery owner can size its own warm throughput to match). */
  readonly checkinsTableName: string;

  constructor(scope: Construct, id: string, props: ApiStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';

    // Address search for saved places. SingleUse means results are not stored.
    const placeIndexName = `relay-${props.stage}-places`;
    const placeIndex = new CfnPlaceIndex(this, 'PlaceIndex', {
      indexName: placeIndexName,
      dataSource: 'Esri',
      dataSourceConfiguration: { intendedUse: 'SingleUse' },
    });

    const warmWrites =
      props.checkinsWarmWritesPerSecond ?? (isProd ? DEFAULT_CHECKINS_WARM_WRITES : 0);
    const requireOrigin = props.requireOriginHeader ?? isProd;
    const reserved = props.reservedConcurrency ?? {};
    const concurrency = (n: number | undefined) =>
      n === undefined ? {} : { reservedConcurrentExecutions: n };

    // Cell-index repair outbox: when the API cannot write a device's place into the cell index
    // it queues a message here, and a Lambda redoes the write from Aurora's truth. A message
    // that keeps failing lands in the DLQ, which alarms to the alerts topic (a missing index entry is a missed alert).
    const repairDlq = new Queue(this, 'IndexRepairDlq', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });
    const repairQueue = new Queue(this, 'IndexRepairQueue', {
      encryption: QueueEncryption.SQS_MANAGED,
      visibilityTimeout: Duration.seconds(REPAIR_TIMEOUT_SECONDS * 6),
      retentionPeriod: Duration.days(14),
      deadLetterQueue: { queue: repairDlq, maxReceiveCount: 5 },
    });

    // Check-ins. Names are generated by CDK (so dev and prod never collide); the Lambda
    // learns them through env vars.
    const tableDefaults = {
      billingMode: BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: isProd },
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    };
    // Stateless check-ins: one row per device per alert (pk = alertId#deviceId), so a device
    // counts once however many times it checks in. The stream feeds the aggregator.
    const checkins = new Table(this, 'CheckinsTable', {
      ...tableDefaults,
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      stream: StreamViewType.NEW_IMAGE,
      // A new on-demand table only serves what its previous peak allowed (4,000 writes/s to
      // start, doubling gradually), so a statewide check-in burst would be throttled. Warm
      // throughput provisions the capacity up front (CloudFormation: WarmThroughput).
      ...(warmWrites > 0 ? { warmThroughput: { writeUnitsPerSecond: warmWrites } } : {}),
    });
    // Running count per alert and H3 cell (written only by the aggregator). This is what
    // agencies see, never the rows above. The byAlert index lists an alert's cells; ttl expires
    // old alerts.
    const checkinTotals = new Table(this, 'CheckinTotalsTable', {
      ...tableDefaults,
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      timeToLiveAttribute: 'ttl',
    });
    checkinTotals.addGlobalSecondaryIndex({
      indexName: 'byAlert',
      partitionKey: { name: 'alertId', type: AttributeType.STRING },
      projectionType: ProjectionType.ALL,
    });

    const apiFn = new NodejsFunction(this, 'ApiFunction', {
      entry: fileURLToPath(new URL('../../backend/src/api/handler.ts', import.meta.url)),
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(15),
      memorySize: 256,
      logGroup: new LogGroup(this, 'ApiLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      }),
      environment: {
        ...dbEnv(props.db),
        PLACE_INDEX_NAME: placeIndexName,
        CHECKINS_TABLE: checkins.tableName,
        TOKEN_KEYS_SECRET_ARN: props.tokenKeysSecretArn,
        DEVICE_CREDENTIAL_KEYS_SECRET_ARN: props.deviceCredentialKeysSecretArn,
        CELL_INDEX_TABLE: props.cellIndexTableName,
        INDEX_REPAIR_QUEUE_URL: repairQueue.queueUrl,
        URGENT_QUEUE_URL: props.urgentQueue.url,
        STANDARD_QUEUE_URL: props.standardQueue.url,
        ORIGIN_VERIFY_SECRET_ARN: props.originVerifySecretArn,
        REQUIRE_ORIGIN_HEADER: requireOrigin ? 'true' : 'false',
      },
      ...concurrency(reserved.api),
    });
    grantSecretRead(apiFn, props.originVerifySecretArn);
    allow(
      apiFn,
      ['sqs:SendMessage'],
      [repairQueue.queueArn, props.urgentQueue.arn, props.standardQueue.arn],
    );

    // Repair Lambda: re-reads Aurora and rewrites the cell-index entries the API failed to write.
    const repairFn = new NodejsFunction(this, 'IndexRepairFunction', {
      entry: fileURLToPath(
        new URL('../../backend/src/scale/cell-index/repair.ts', import.meta.url),
      ),
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(REPAIR_TIMEOUT_SECONDS),
      memorySize: 256,
      logGroup: new LogGroup(this, 'IndexRepairLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      }),
      environment: { ...dbEnv(props.db), CELL_INDEX_TABLE: props.cellIndexTableName },
      ...concurrency(reserved.repair),
    });
    grantDb(repairFn, props.db);
    allow(
      repairFn,
      ['dynamodb:BatchWriteItem', 'dynamodb:PutItem', 'dynamodb:DeleteItem', 'dynamodb:UpdateItem'],
      [props.cellIndexTableArn],
    );
    repairFn.addEventSource(
      new SqsEventSource(repairQueue, { batchSize: 10, reportBatchItemFailures: true }),
    );
    new Alarm(this, 'IndexRepairDlqAlarm', {
      alarmDescription:
        'A cell-index repair failed after retries: a device may be missing from the index and miss alerts',
      metric: repairDlq.metricApproximateNumberOfMessagesVisible({
        period: Duration.minutes(1),
        statistic: 'Maximum',
      }),
      threshold: 0,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    grantDb(apiFn, props.db);
    allow(apiFn, ['geo:SearchPlaceIndexForText'], [placeIndex.attrArn]);
    grantSecretRead(apiFn, props.tokenKeysSecretArn);
    grantSecretRead(apiFn, props.deviceCredentialKeysSecretArn);
    // Check-in rows: written with PutItem, read back with GetItem.
    allow(apiFn, ['dynamodb:PutItem', 'dynamodb:GetItem'], [checkins.tableArn]);
    // Cell index: replace/clear a device's places (BatchWriteItem, DeleteItem), re-point tokens
    // (UpdateItem), read them back (Query, GetItem) and upsert (PutItem).
    allow(
      apiFn,
      [
        'dynamodb:BatchWriteItem',
        'dynamodb:DeleteItem',
        'dynamodb:UpdateItem',
        'dynamodb:Query',
        'dynamodb:GetItem',
        'dynamodb:PutItem',
      ],
      [props.cellIndexTableArn],
    );

    // Check-in aggregator: turns the checkins stream into per-cell totals.
    // DynamoDB has no IAM action called TransactWriteItems. A transaction is authorized item by
    // item: each Put needs dynamodb:PutItem and each Update needs dynamodb:UpdateItem (conditions
    // need nothing extra). ConditionCheckItem is only for a standalone ConditionCheck, which the
    // aggregator does not use.
    const aggregatorDlq = new Queue(this, 'AggregatorDlq', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });
    const aggregatorFn = new NodejsFunction(this, 'AggregatorFunction', {
      entry: fileURLToPath(
        new URL('../../backend/src/scale/checkins/aggregator.ts', import.meta.url),
      ),
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(AGGREGATOR_TIMEOUT_SECONDS),
      memorySize: 512,
      logGroup: new LogGroup(this, 'AggregatorLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      }),
      environment: { CHECKIN_TOTALS_TABLE: checkinTotals.tableName },
      ...concurrency(reserved.aggregator),
    });
    allow(aggregatorFn, ['dynamodb:UpdateItem', 'dynamodb:PutItem'], [checkinTotals.tableArn]);
    aggregatorFn.addEventSource(
      new DynamoEventSource(checkins, {
        startingPosition: StartingPosition.TRIM_HORIZON,
        batchSize: 1000,
        maxBatchingWindow: Duration.seconds(2),
        reportBatchItemFailures: true,
        bisectBatchOnError: true,
        retryAttempts: 10,
        parallelizationFactor: 10,
        onFailure: new SqsDlq(aggregatorDlq),
      }),
    );
    new Alarm(this, 'AggregatorDlqAlarm', {
      alarmDescription: 'Check-in stream records failed after retries: totals are undercounted',
      metric: aggregatorDlq.metricApproximateNumberOfMessagesVisible({
        period: Duration.minutes(1),
        statistic: 'Maximum',
      }),
      threshold: 0,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // The stage is created by hand below so we can set throttling on $default.
    const api = new HttpApi(this, 'HttpApi', {
      apiName: `relay-${props.stage}-api`,
      createDefaultStage: false,
    });
    const stage = new HttpStage(this, 'DefaultStage', {
      httpApi: api,
      stageName: '$default',
      autoDeploy: true,
      throttle: props.throttle ?? DEFAULT_THROTTLE[props.stage],
    });

    const integration = new HttpLambdaIntegration('ApiIntegration', apiFn);
    for (const { method, path } of API_ROUTES) {
      api.addRoutes({ path, methods: [method], integration });
    }

    this.apiDomainName = api.apiEndpoint;
    this.checkinsTableName = checkins.tableName;

    new CfnOutput(this, 'ApiUrl', { value: stage.url });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
