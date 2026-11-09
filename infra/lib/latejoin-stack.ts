import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { allow, dbEnv, grantDb, grantSecretRead, type DbRefs } from './grants.js';

export interface LateJoinStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  tokenKeysSecretArn: string;
  alertKeysSecretArn: string;
  killSwitchParamName: string;
  killSwitchParamArn: string;
  /** Send queues (FanoutStack): this stack's Lambda sends single-recipient batches to them. */
  urgentQueue: { arn: string; url: string };
  standardQueue: { arn: string; url: string };
  /** The sender's idempotency table (DeliveryStack): batch claims, target claims, rate slots. */
  idempotencyTableName: string;
  idempotencyTableArn: string;
  /** Cap on parallel late-join invocations (protects Aurora and the send queues). Default 20. */
  maxConcurrency?: number;
}

export const LATEJOIN_TIMEOUT_SECONDS = 30;
export const LATEJOIN_DEFAULT_CONCURRENCY = 20;
/** A paused kill switch makes the Lambda retry the record; 20 x ~3 min is about an hour. */
export const LATEJOIN_MAX_RECEIVE_COUNT = 20;

/**
 * Late joiners (SEC-C-1, SEC-A-1, REL-12). The API asks, this stack acts:
 *
 *   API -- LateJoinIntent {deviceId, placeId?, reason} --> late-join queue (this stack, relay-<stage>-latejoin)
 *       --> late-join Lambda --> urgent / standard send queues (one single-recipient batch)
 *
 * Only the API can send to the intent queue, and only this Lambda consumes it. The Lambda reads
 * nothing from the intent but the device id: it re-reads the device, places and active alerts
 * from Aurora, verifies each alert's signature, applies the primary-cell rule and push policy,
 * claims one check-in token per push target per alert, rate-limits, mints the v2 token and
 * enqueues. Its IAM is exactly that: consume the intent queue, send to the two send queues,
 * rds-data, the token and alert keyrings, the kill switch and the idempotency table.
 */
export class LateJoinStack extends Stack {
  readonly queueArn: string;
  readonly queueUrl: string;

  constructor(scope: Construct, id: string, props: LateJoinStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';
    const removalPolicy = isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;

    const dlq = new Queue(this, 'LateJoinDlq', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });
    const queue = new Queue(this, 'LateJoinQueue', {
      encryption: QueueEncryption.SQS_MANAGED,
      visibilityTimeout: Duration.seconds(LATEJOIN_TIMEOUT_SECONDS * 6),
      deadLetterQueue: { queue: dlq, maxReceiveCount: LATEJOIN_MAX_RECEIVE_COUNT },
    });

    const fn = new NodejsFunction(this, 'LateJoinFunction', {
      entry: fileURLToPath(new URL('../../backend/src/scale/latejoin/handler.ts', import.meta.url)),
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(LATEJOIN_TIMEOUT_SECONDS),
      memorySize: 256,
      logGroup: new LogGroup(this, 'LateJoinLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy,
      }),
      environment: {
        ...dbEnv(props.db, 'latejoin'),
        URGENT_QUEUE_URL: props.urgentQueue.url,
        STANDARD_QUEUE_URL: props.standardQueue.url,
        IDEMPOTENCY_TABLE: props.idempotencyTableName,
        ALERT_KEYS_SECRET_ARN: props.alertKeysSecretArn,
        TOKEN_KEYS_SECRET_ARN: props.tokenKeysSecretArn,
        KILL_SWITCH_PARAM: props.killSwitchParamName,
      },
    });
    // Consume the intent queue (the event source grants receive/delete on it).
    fn.addEventSource(
      new SqsEventSource(queue, {
        batchSize: 10,
        maxConcurrency: props.maxConcurrency ?? LATEJOIN_DEFAULT_CONCURRENCY,
        reportBatchItemFailures: true,
      }),
    );
    allow(fn, ['sqs:SendMessage'], [props.urgentQueue.arn, props.standardQueue.arn]);
    grantDb(fn, props.db, 'latejoin');
    grantSecretRead(fn, props.tokenKeysSecretArn);
    grantSecretRead(fn, props.alertKeysSecretArn);
    allow(fn, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    // DynamoIdempotencyStore: claim (PutItem, GetItem), markSent/extend (UpdateItem), release.
    allow(
      fn,
      ['dynamodb:PutItem', 'dynamodb:GetItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem'],
      [props.idempotencyTableArn],
    );

    new Alarm(this, 'LateJoinDlqAlarm', {
      alarmDescription:
        'A late-join intent failed after retries: a late joiner may have missed an alert',
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
    new CfnOutput(this, 'LateJoinQueueUrl', { value: queue.queueUrl });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
