import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { SqsDestination } from 'aws-cdk-lib/aws-lambda-destinations';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { allow, dbEnv, grantDb, grantSecretRead, type DbRefs } from './grants.js';

export interface FanoutStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  tokenKeysSecretArn: string;
  alertKeysSecretArn: string;
  killSwitchParamName: string;
  killSwitchParamArn: string;
  /**
   * Reserved concurrency of the shard worker (default 500). It is carved out of the account's
   * Lambda concurrency quota, which must be raised first (see docs/scale-quotas.md).
   */
  shardReservedConcurrency?: number;
}

/** Send queues: a push batch is claimed by the sender for 45 s, so a crashed read returns in 30 s. */
export const SEND_QUEUE_VISIBILITY_SECONDS = 30;
export const DEFAULT_SHARD_CONCURRENCY = 500;
export const SHARD_TIMEOUT_SECONDS = 60;
const PLANNER_TIMEOUT_SECONDS = 60;
const SHARD_MAX_EVENT_AGE = Duration.hours(1);
const SHARD_RETRY_ATTEMPTS = 2;

const entry = (file: string) =>
  fileURLToPath(new URL(`../../backend/src/scale/fanout/${file}`, import.meta.url));

/**
 * Parallel fan-out: plan queue -> planner Lambda -> async invokes of the shard Lambda (one per
 * ShardJob) -> urgent / standard send queues for the sender service. Also owns the cell-index
 * table the shard workers read.
 *
 * Shard jobs go by direct async invoke, not through an SQS event source: an SQS-triggered
 * function adds only about 300 concurrent invocations a minute, far too slow for a 60 s fan-out.
 * Failed shard invokes (after 2 retries or 1 hour) land in the shard DLQ; the redrive function
 * replays them (runbook in infra/README.md).
 */
export class FanoutStack extends Stack {
  readonly cellIndexTableName: string;
  readonly cellIndexTableArn: string;
  readonly planQueueArn: string;
  readonly planQueueUrl: string;
  readonly urgentQueueArn: string;
  readonly urgentQueueUrl: string;
  readonly standardQueueArn: string;
  readonly standardQueueUrl: string;
  readonly shardFunctionName: string;
  readonly shardFunctionArn: string;
  readonly shardDlqUrl: string;

  constructor(scope: Construct, id: string, props: FanoutStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';
    const removalPolicy = isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;

    // pk = h3Cell (res 7), sk = `${deviceId}#${placeId}`. One Query per cell page.
    const cellIndex = new Table(this, 'CellIndexTable', {
      partitionKey: { name: 'h3Cell', type: AttributeType.STRING },
      sortKey: { name: 'sk', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: isProd },
      removalPolicy,
    });

    const queueWithDlq = (
      name: string,
      visibility: number,
      maxReceiveCount: number,
    ): { queue: Queue; dlq: Queue } => {
      const dlq = new Queue(this, `${name}Dlq`, {
        encryption: QueueEncryption.SQS_MANAGED,
        retentionPeriod: Duration.days(14),
      });
      const queue = new Queue(this, `${name}Queue`, {
        encryption: QueueEncryption.SQS_MANAGED,
        visibilityTimeout: Duration.seconds(visibility),
        deadLetterQueue: { queue: dlq, maxReceiveCount },
      });
      return { queue, dlq };
    };

    // Plan: the planner dispatches a few hundred invokes (seconds). A paused kill switch makes it
    // retry the message, so allow many receives before giving up.
    const plan = queueWithDlq('Plan', 120, 100);
    const urgent = queueWithDlq('Urgent', SEND_QUEUE_VISIBILITY_SECONDS, 5);
    const standard = queueWithDlq('Standard', SEND_QUEUE_VISIBILITY_SECONDS, 5);

    // Receives shard jobs whose async invoke failed (Lambda on-failure destination).
    const shardDlq = new Queue(this, 'ShardDlq', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });

    const common = {
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
    };
    const logs = (name: string) =>
      new LogGroup(this, name, { retention: RetentionDays.ONE_MONTH, removalPolicy });

    const shardFn = new NodejsFunction(this, 'ShardFunction', {
      ...common,
      entry: entry('handlers.ts'),
      handler: 'shardHandler',
      timeout: Duration.seconds(SHARD_TIMEOUT_SECONDS),
      memorySize: 1024,
      reservedConcurrentExecutions: props.shardReservedConcurrency ?? DEFAULT_SHARD_CONCURRENCY,
      // Async invoke policy: 2 retries within 1 hour, then the shard DLQ.
      maxEventAge: SHARD_MAX_EVENT_AGE,
      retryAttempts: SHARD_RETRY_ATTEMPTS,
      onFailure: new SqsDestination(shardDlq),
      logGroup: logs('ShardLogs'),
      environment: {
        ...dbEnv(props.db),
        CELL_INDEX_TABLE: cellIndex.tableName,
        URGENT_QUEUE_URL: urgent.queue.queueUrl,
        STANDARD_QUEUE_URL: standard.queue.queueUrl,
        ALERT_KEYS_SECRET_ARN: props.alertKeysSecretArn,
        TOKEN_KEYS_SECRET_ARN: props.tokenKeysSecretArn,
        KILL_SWITCH_PARAM: props.killSwitchParamName,
      },
    });
    grantDb(shardFn, props.db);
    grantSecretRead(shardFn, props.alertKeysSecretArn);
    grantSecretRead(shardFn, props.tokenKeysSecretArn);
    allow(shardFn, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    allow(shardFn, ['dynamodb:Query'], [cellIndex.tableArn]);
    allow(shardFn, ['sqs:SendMessage'], [urgent.queue.queueArn, standard.queue.queueArn]);

    const planner = new NodejsFunction(this, 'PlannerFunction', {
      ...common,
      entry: entry('handlers.ts'),
      handler: 'plannerHandler',
      timeout: Duration.seconds(PLANNER_TIMEOUT_SECONDS),
      memorySize: 512,
      logGroup: logs('PlannerLogs'),
      environment: {
        ...dbEnv(props.db),
        SHARD_FUNCTION_NAME: shardFn.functionName,
        ALERT_KEYS_SECRET_ARN: props.alertKeysSecretArn,
        KILL_SWITCH_PARAM: props.killSwitchParamName,
      },
    });
    // One AlertReady at a time, so a failure retries just that alert.
    planner.addEventSource(
      new SqsEventSource(plan.queue, { batchSize: 1, reportBatchItemFailures: true }),
    );
    grantDb(planner, props.db);
    grantSecretRead(planner, props.alertKeysSecretArn);
    allow(planner, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    // Invoke the shard function only (no wildcard, no other functions).
    allow(planner, ['lambda:InvokeFunction'], [shardFn.functionArn]);

    // Manual DLQ replay. Not scheduled: a person decides when replaying is wise.
    const redrive = new NodejsFunction(this, 'RedriveFunction', {
      ...common,
      entry: entry('redrive.ts'),
      handler: 'redriveHandler',
      timeout: Duration.minutes(5),
      memorySize: 256,
      logGroup: logs('RedriveLogs'),
      environment: {
        SHARD_DLQ_URL: shardDlq.queueUrl,
        SHARD_FUNCTION_NAME: shardFn.functionName,
      },
    });
    allow(
      redrive,
      ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes'],
      [shardDlq.queueArn],
    );
    allow(redrive, ['lambda:InvokeFunction'], [shardFn.functionArn]);

    const dlqs: Array<[string, Queue, string]> = [
      [
        'PlanDlqAlarm',
        plan.dlq,
        'AlertReady messages are failing (alerts are not being fanned out)',
      ],
      [
        'ShardDlqAlarm',
        shardDlq,
        'Shard jobs failed after retries: some areas have not been sent. Run the redrive',
      ],
      ['UrgentDlqAlarm', urgent.dlq, 'Urgent send batches are landing in the DLQ'],
      ['StandardDlqAlarm', standard.dlq, 'Standard send batches are landing in the DLQ'],
    ];
    for (const [alarmId, dlq, description] of dlqs) {
      new Alarm(this, alarmId, {
        alarmDescription: description,
        metric: dlq.metricApproximateNumberOfMessagesVisible({
          period: Duration.minutes(1),
          statistic: 'Maximum',
        }),
        threshold: 0,
        evaluationPeriods: 1,
        comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      });
    }

    this.cellIndexTableName = cellIndex.tableName;
    this.cellIndexTableArn = cellIndex.tableArn;
    this.planQueueArn = plan.queue.queueArn;
    this.planQueueUrl = plan.queue.queueUrl;
    this.urgentQueueArn = urgent.queue.queueArn;
    this.urgentQueueUrl = urgent.queue.queueUrl;
    this.standardQueueArn = standard.queue.queueArn;
    this.standardQueueUrl = standard.queue.queueUrl;
    this.shardFunctionName = shardFn.functionName;
    this.shardFunctionArn = shardFn.functionArn;
    this.shardDlqUrl = shardDlq.queueUrl;

    new CfnOutput(this, 'CellIndexTableName', { value: this.cellIndexTableName });
    new CfnOutput(this, 'PlanQueueUrl', { value: this.planQueueUrl });
    new CfnOutput(this, 'UrgentQueueUrl', { value: this.urgentQueueUrl });
    new CfnOutput(this, 'StandardQueueUrl', { value: this.standardQueueUrl });
    new CfnOutput(this, 'ShardDlqUrl', { value: this.shardDlqUrl });
    new CfnOutput(this, 'RedriveFunctionName', { value: redrive.functionName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
