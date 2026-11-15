import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { DELIVERY_RESERVED_CONCURRENCY } from './capacity.js';
import {
  alertVerifyEnv,
  allow,
  dbEnv,
  grantAlertVerify,
  grantDb,
  grantSecretRead,
  type AlertSigningRefs,
  type DbRefs,
} from './grants.js';

/** Idempotency table warm throughput on prod (writes/s). */
export const IDEMPOTENCY_WARM_WRITES_PROD = 20_000;

export interface DeliveryStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  expoSecretArn: string;
  alertSigning: AlertSigningRefs;
  killSwitchParamName: string;
  killSwitchParamArn: string;
}

// The worker uses 45 s leases and an 8 s Expo timeout, so 20 s is plenty.
export const WORKER_TIMEOUT_SECONDS = 20;
// AWS advises 6x the consumer timeout so a slow batch is not redelivered mid-run. We go well
// below that on purpose: every push is claimed in DynamoDB before it is sent, so an early
// redelivery of a still-running job just sees a live lease and retries. A short timeout means a
// crashed attempt is retried within seconds. Keep the worker's LEASE_MS between the worker
// timeout and 2x this value (checked in tests) so redelivery after a crash finds the lease expired.
export const VISIBILITY_TIMEOUT_SECONDS = 30;
// With 30 s per receive, 100 receives is about 50 minutes of kill-switch pause before the DLQ.
export const MAX_RECEIVE_COUNT = 100;
const WORKER_TIMEOUT = Duration.seconds(WORKER_TIMEOUT_SECONDS);

/** Delivery queue, its dead-letter queue, the idempotency table and the push worker. */
export class DeliveryStack extends Stack {
  readonly queueArn: string;
  readonly queueUrl: string;
  readonly idempotencyTableName: string;
  readonly idempotencyTableArn: string;

  constructor(scope: Construct, id: string, props: DeliveryStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';
    const removalPolicy = isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;

    const dlq = new Queue(this, 'DeadLetterQueue', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });

    const queue = new Queue(this, 'DeliveryQueue', {
      encryption: QueueEncryption.SQS_MANAGED,
      visibilityTimeout: Duration.seconds(VISIBILITY_TIMEOUT_SECONDS),
      deadLetterQueue: { queue: dlq, maxReceiveCount: MAX_RECEIVE_COUNT },
    });

    // One row per alert+device that was already sent; TTL cleans old rows.
    const idempotency = new Table(this, 'IdempotencyTable', {
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      // Point-in-time recovery on prod, like every other table (NIST CP-9). Dev is disposable.
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: isProd },
      // A new on-demand table starts at 4,000 writes/s and doubles gradually; warm throughput
      // provisions the headroom up front for a statewide send (prod only).
      ...(isProd ? { warmThroughput: { writeUnitsPerSecond: IDEMPOTENCY_WARM_WRITES_PROD } } : {}),
      removalPolicy,
    });

    const worker = new NodejsFunction(this, 'WorkerFunction', {
      entry: fileURLToPath(new URL('../../backend/src/delivery/handler.ts', import.meta.url)),
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: WORKER_TIMEOUT,
      memorySize: 256,
      // Caps parallel sends so we stay inside Expo's rate limits.
      reservedConcurrentExecutions: DELIVERY_RESERVED_CONCURRENCY,
      logGroup: new LogGroup(this, 'WorkerLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy,
      }),
      environment: {
        ...dbEnv(props.db, 'sender'),
        EXPO_ACCESS_TOKEN_SECRET_ARN: props.expoSecretArn,
        IDEMPOTENCY_TABLE: idempotency.tableName,
        ...alertVerifyEnv(props.alertSigning),
        KILL_SWITCH_PARAM: props.killSwitchParamName,
      },
    });

    // The event source also grants sqs:ReceiveMessage/DeleteMessage/GetQueueAttributes.
    worker.addEventSource(
      new SqsEventSource(queue, { batchSize: 10, reportBatchItemFailures: true }),
    );
    grantDb(worker, props.db, 'sender');
    grantSecretRead(worker, props.expoSecretArn);
    grantAlertVerify(worker, props.alertSigning);
    // Send-time dedupe: claim (PutItem), check (GetItem), markSent (UpdateItem), release on
    // failure (DeleteItem). wiring.test.ts derives this list from the SDK commands in the store.
    allow(
      worker,
      ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem'],
      [idempotency.tableArn],
    );
    allow(worker, ['ssm:GetParameter'], [props.killSwitchParamArn]);

    new Alarm(this, 'DlqNotEmptyAlarm', {
      alarmDescription: 'Push jobs are failing repeatedly and landed in the DLQ',
      metric: dlq.metricApproximateNumberOfMessagesVisible({
        period: Duration.minutes(1),
        statistic: 'Maximum',
      }),
      threshold: 0,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    this.queueArn = queue.queueArn;
    this.queueUrl = queue.queueUrl;
    this.idempotencyTableName = idempotency.tableName;
    this.idempotencyTableArn = idempotency.tableArn;

    new CfnOutput(this, 'DeliveryQueueUrl', { value: this.queueUrl });
    new CfnOutput(this, 'DeadLetterQueueUrl', { value: dlq.queueUrl });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
