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
import { allow, dbEnv, grantDb, grantSecretRead, type DbRefs } from './grants.js';

export interface DeliveryStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  expoSecretArn: string;
  alertKeysSecretArn: string;
  killSwitchParamName: string;
  killSwitchParamArn: string;
}

const WORKER_TIMEOUT = Duration.seconds(30);

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
      // AWS advice: at least 6x the consumer's timeout, so a slow batch is not redelivered mid-run.
      visibilityTimeout: Duration.seconds(WORKER_TIMEOUT.toSeconds() * 6),
      deadLetterQueue: { queue: dlq, maxReceiveCount: 50 },
    });

    // One row per alert+device that was already sent; TTL cleans old rows.
    const idempotency = new Table(this, 'IdempotencyTable', {
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
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
      reservedConcurrentExecutions: 10,
      logGroup: new LogGroup(this, 'WorkerLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy,
      }),
      environment: {
        ...dbEnv(props.db),
        EXPO_ACCESS_TOKEN_SECRET_ARN: props.expoSecretArn,
        IDEMPOTENCY_TABLE: idempotency.tableName,
        ALERT_KEYS_SECRET_ARN: props.alertKeysSecretArn,
        KILL_SWITCH_PARAM: props.killSwitchParamName,
      },
    });

    // The event source also grants sqs:ReceiveMessage/DeleteMessage/GetQueueAttributes.
    worker.addEventSource(
      new SqsEventSource(queue, { batchSize: 10, reportBatchItemFailures: true }),
    );
    grantDb(worker, props.db);
    grantSecretRead(worker, props.expoSecretArn);
    grantSecretRead(worker, props.alertKeysSecretArn);
    // Send-time dedupe: claim (PutItem), check (GetItem), release on failure (DeleteItem).
    allow(
      worker,
      ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:DeleteItem'],
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
