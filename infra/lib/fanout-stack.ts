import { fileURLToPath } from 'node:url';
import {
  ArnFormat,
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { Rule, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { SqsDestination } from 'aws-cdk-lib/aws-lambda-destinations';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import { DEFAULT_SHARD_CONCURRENCY, PLANNER_RESERVED_CONCURRENCY } from './capacity.js';
import { latejoinQueueName } from './latejoin-stack.js';
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

export interface FanoutStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  tokenKeysSecretArn: string;
  alertSigning: AlertSigningRefs;
  killSwitchParamName: string;
  killSwitchParamArn: string;
  /**
   * Reserved concurrency of the shard worker (default 500). It is carved out of the account's
   * Lambda concurrency quota, which must be raised first (see docs/scale-quotas.md).
   */
  shardReservedConcurrency?: number;
  /** Reserved concurrency of the planner (default 10): an API flood cannot starve it (SEC-C-2). */
  plannerReservedConcurrency?: number;
}

/** Send queues: a push batch is claimed by the sender for 45 s, so a crashed read returns in 30 s. */
export const SEND_QUEUE_VISIBILITY_SECONDS = 30;
export { DEFAULT_SHARD_CONCURRENCY };
export const SHARD_TIMEOUT_SECONDS = 60;
/**
 * Cell-index table warm throughput on prod (PERF-4a). A new on-demand table serves 4,000 writes/s
 * and 12,000 reads/s at first and only doubles on previous-peak growth.
 *  - Reads: a shard worker pages 1,000 entries of about 353 B (eventually consistent, 0.5 RRU per
 *    4 KB), so a 10M-recipient send in 120 s (83k entries/s) needs about 3.6k RRU/s. 30,000 is
 *    roughly 8x that for uneven shard bursts and stays under the 40,000 per-table default quota.
 *  - Writes: not a send-path load. 10,000 covers an onboarding wave or a full re-index by the
 *    outbox drainer without waiting on doubling.
 * See docs/scale-quotas.md.
 */
export const CELL_INDEX_WARM_READS_PROD = 30_000;
export const CELL_INDEX_WARM_WRITES_PROD = 10_000;
/**
 * The cell-index outbox drainer runs every minute on prod. Dev runs it every 5 minutes so the dev
 * Aurora cluster (min 0 ACU, auto-pause) can sleep between ticks; prod's cluster never pauses
 * (min 0.5 ACU), and the every-minute drainer is what also keeps it warm (PERF-5).
 */
export const DRAINER_MINUTES = { dev: 5, prod: 1 } as const;
/** 2048 MB buys about 1.2 vCPU: the shard worker is CPU-bound on token signing and bucketing. */
export const SHARD_MEMORY_MB = 2048;
/**
 * Send queues: a message is received again after every pause or lease, so 5 receives was far too
 * few. 100 receives x 30 s is about 50 minutes of retrying before a batch goes to the DLQ.
 */
export const SEND_QUEUE_MAX_RECEIVE_COUNT = 100;

// ---- Sender fleet pre-scale: how the planner finds the sender service without a cycle ----
// The sender stack depends on this stack (it reads the send queues), so this stack cannot read
// the service's name or ARN from it: that would be a cycle. Instead both stacks derive the
// cluster and service names from the stage alone (explicit physical names), and this stack builds
// the ARNs from those names. No SSM parameter, no cross-stack reference, no deploy-order trap.
export const senderClusterName = (stage: string) => `relay-${stage}-sender`;
export const senderServiceName = (stage: string) => `relay-${stage}-sender`;
export const SENDER_MAX_TASKS_PROD = 100;
export const SENDER_MAX_TASKS_DEV = 10;
export const senderMaxTasks = (stage: string) =>
  stage === 'prod' ? SENDER_MAX_TASKS_PROD : SENDER_MAX_TASKS_DEV;
/** Expected recipients per ShardJob (16 cells of mixed density); only an estimate. */
export const SENDER_SCALE_PER_SHARD = 15_000;
/** What one 1-vCPU sender task pushes per minute (about 150/s), the basis of the pre-scale. */
export const SENDER_RECIPIENTS_PER_TASK_PER_MIN = 9_000;
/** Context flag shared with the sender stack: the sender fleet only runs once this is "true". */
export const SENDER_ENABLED_CONTEXT = 'senderEnabled';
/** Second flag required to turn the prod fleet off: `-c senderEnabled=false -c iUnderstandSenderStops=true`. */
export const SENDER_STOPS_ACK_CONTEXT = 'iUnderstandSenderStops';
/**
 * Whether the sender fleet runs. Prod defaults to ON, so a deploy that forgets the flag can never
 * scale the fleet to 0 by accident (the old default killed delivery). Dev defaults to OFF. Turning
 * prod off must be spelled out with both `-c senderEnabled=false` and `-c iUnderstandSenderStops=true`
 * (needed once, for the very first prod deploy, before the APNs/FCM secrets hold real values).
 */
export const isSenderEnabled = (scope: Construct, stage: string): boolean => {
  const raw = scope.node.tryGetContext(SENDER_ENABLED_CONTEXT);
  if (raw === undefined) return stage === 'prod';
  const on = String(raw) === 'true';
  if (!on && stage === 'prod') {
    const ack = String(scope.node.tryGetContext(SENDER_STOPS_ACK_CONTEXT) ?? 'false') === 'true';
    if (!ack) {
      throw new Error(
        `Refusing to deploy prod with the sender fleet off: it would stop all push delivery. ` +
          `Pass -c ${SENDER_STOPS_ACK_CONTEXT}=true together with -c ${SENDER_ENABLED_CONTEXT}=false if you mean it.`,
      );
    }
  }
  return on;
};
const PLANNER_TIMEOUT_SECONDS = 60;
const PAUSED_CONSUMER_TIMEOUT_SECONDS = 60;
const OUTBOX_DRAINER_TIMEOUT_SECONDS = 60;
/** The drainer's alarm threshold (attempts); keep in step with OUTBOX_ALARM_ATTEMPTS in the backend. */
const OUTBOX_ALARM_ATTEMPTS = 10;
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
  readonly urgentQueueName: string;
  readonly standardQueueArn: string;
  readonly standardQueueUrl: string;
  readonly standardQueueName: string;
  readonly shardFunctionName: string;
  readonly shardFunctionArn: string;
  readonly shardDlqUrl: string;
  readonly pausedQueueUrl: string;

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
      ...(isProd
        ? {
            warmThroughput: {
              readUnitsPerSecond: CELL_INDEX_WARM_READS_PROD,
              writeUnitsPerSecond: CELL_INDEX_WARM_WRITES_PROD,
            },
          }
        : {}),
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
    const urgent = queueWithDlq(
      'Urgent',
      SEND_QUEUE_VISIBILITY_SECONDS,
      SEND_QUEUE_MAX_RECEIVE_COUNT,
    );
    const standard = queueWithDlq(
      'Standard',
      SEND_QUEUE_VISIBILITY_SECONDS,
      SEND_QUEUE_MAX_RECEIVE_COUNT,
    );

    // Receives shard jobs whose async invoke failed (Lambda on-failure destination).
    const shardDlq = new Queue(this, 'ShardDlq', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });

    // Parking queue for work found while the kill switch is paused (instead of throwing into the
    // shard DLQ). Messages come back every 60 s to the paused consumer, which re-checks the switch
    // and re-dispatches once it is running. After 24 h of pause a message fails into this DLQ.
    const pausedDlq = new Queue(this, 'ShardPausedDlq', {
      encryption: QueueEncryption.SQS_MANAGED,
      retentionPeriod: Duration.days(14),
    });
    const paused = new Queue(this, 'ShardPausedQueue', {
      queueName: `relay-${props.stage}-shard-paused`,
      encryption: QueueEncryption.SQS_MANAGED,
      visibilityTimeout: Duration.seconds(PAUSED_CONSUMER_TIMEOUT_SECONDS),
      retentionPeriod: Duration.days(2),
      // Normal messages are received once (the consumer re-parks as a NEW message); a few
      // receives cover transient errors and move over-age messages to the DLQ.
      deadLetterQueue: { queue: pausedDlq, maxReceiveCount: 3 },
    });

    const common = {
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
    };
    const logs = (name: string) =>
      new LogGroup(this, name, { retention: RetentionDays.ONE_MONTH, removalPolicy });

    const shardFunctionName = `relay-${props.stage}-shard-worker`;
    const shardFunctionArnByName = this.formatArn({
      service: 'lambda',
      resource: 'function',
      resourceName: shardFunctionName,
      arnFormat: ArnFormat.COLON_RESOURCE_NAME,
    });
    const shardFn = new NodejsFunction(this, 'ShardFunction', {
      ...common,
      entry: entry('handlers.ts'),
      handler: 'shardHandler',
      timeout: Duration.seconds(SHARD_TIMEOUT_SECONDS),
      // An explicit name lets the function be granted permission to invoke itself (for its
      // continuation jobs) without its policy referring to its own ARN, which would be a cycle.
      functionName: shardFunctionName,
      memorySize: SHARD_MEMORY_MB,
      reservedConcurrentExecutions: props.shardReservedConcurrency ?? DEFAULT_SHARD_CONCURRENCY,
      // Async invoke policy: 2 retries within 1 hour, then the shard DLQ.
      maxEventAge: SHARD_MAX_EVENT_AGE,
      retryAttempts: SHARD_RETRY_ATTEMPTS,
      onFailure: new SqsDestination(shardDlq),
      logGroup: logs('ShardLogs'),
      environment: {
        ...dbEnv(props.db, 'fanout'),
        CELL_INDEX_TABLE: cellIndex.tableName,
        URGENT_QUEUE_URL: urgent.queue.queueUrl,
        STANDARD_QUEUE_URL: standard.queue.queueUrl,
        ...alertVerifyEnv(props.alertSigning),
        TOKEN_KEYS_SECRET_ARN: props.tokenKeysSecretArn,
        KILL_SWITCH_PARAM: props.killSwitchParamName,
        PAUSED_QUEUE_URL: paused.queueUrl,
      },
    });
    grantDb(shardFn, props.db, 'fanout');
    grantAlertVerify(shardFn, props.alertSigning);
    grantSecretRead(shardFn, props.tokenKeysSecretArn);
    allow(shardFn, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    allow(shardFn, ['dynamodb:Query'], [cellIndex.tableArn]);
    allow(shardFn, ['sqs:SendMessage'], [urgent.queue.queueArn, standard.queue.queueArn]);
    allow(shardFn, ['sqs:SendMessage'], [paused.queueArn]);
    // A shard that runs short of time hands the rest of its cells to a new invoke of itself.
    allow(shardFn, ['lambda:InvokeFunction'], [shardFunctionArnByName]);

    const senderEnabled = isSenderEnabled(this, props.stage);
    const senderClusterArn = this.formatArn({
      service: 'ecs',
      resource: 'cluster',
      resourceName: senderClusterName(props.stage),
      arnFormat: ArnFormat.SLASH_RESOURCE_NAME,
    });
    const senderServiceArn = this.formatArn({
      service: 'ecs',
      resource: 'service',
      resourceName: `${senderClusterName(props.stage)}/${senderServiceName(props.stage)}`,
      arnFormat: ArnFormat.SLASH_RESOURCE_NAME,
    });
    const planner = new NodejsFunction(this, 'PlannerFunction', {
      ...common,
      entry: entry('handlers.ts'),
      handler: 'plannerHandler',
      timeout: Duration.seconds(PLANNER_TIMEOUT_SECONDS),
      memorySize: 512,
      reservedConcurrentExecutions:
        props.plannerReservedConcurrency ?? PLANNER_RESERVED_CONCURRENCY,
      logGroup: logs('PlannerLogs'),
      environment: {
        ...dbEnv(props.db, 'fanout'),
        SHARD_FUNCTION_NAME: shardFn.functionName,
        ...alertVerifyEnv(props.alertSigning),
        KILL_SWITCH_PARAM: props.killSwitchParamName,
        PAUSED_QUEUE_URL: paused.queueUrl,
        // Pre-scale of the sender fleet. SENDER_MAX_TASKS is 0 (off) until senderEnabled=true,
        // so the planner never wakes a fleet that has placeholder secrets.
        SENDER_CLUSTER_ARN: senderClusterArn,
        SENDER_SERVICE_NAME: senderServiceName(props.stage),
        SENDER_MAX_TASKS: senderEnabled ? String(senderMaxTasks(props.stage)) : '0',
        SENDER_SCALE_PER_SHARD: String(SENDER_SCALE_PER_SHARD),
        SENDER_RECIPIENTS_PER_TASK_PER_MIN: String(SENDER_RECIPIENTS_PER_TASK_PER_MIN),
      },
    });
    // One AlertReady at a time, so a failure retries just that alert.
    planner.addEventSource(
      new SqsEventSource(plan.queue, { batchSize: 1, reportBatchItemFailures: true }),
    );
    grantDb(planner, props.db, 'fanout');
    grantAlertVerify(planner, props.alertSigning);
    allow(planner, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    allow(planner, ['sqs:SendMessage'], [paused.queueArn]);
    // Invoke the shard function only (no wildcard, no other functions).
    allow(planner, ['lambda:InvokeFunction'], [shardFn.functionArn]);
    // Pre-scale: read the sender service's desired count and raise it. Nothing else, on that service only.
    allow(planner, ['ecs:UpdateService', 'ecs:DescribeServices'], [senderServiceArn]);

    // Paused consumer: re-checks the kill switch for parked shard jobs / AlertReady messages and
    // re-dispatches them through the shard function (or the plan queue) once it is running.
    const pausedConsumer = new NodejsFunction(this, 'PausedConsumerFunction', {
      ...common,
      entry: entry('paused-consumer.ts'),
      handler: 'handler',
      timeout: Duration.seconds(PAUSED_CONSUMER_TIMEOUT_SECONDS),
      memorySize: 256,
      logGroup: logs('PausedConsumerLogs'),
      environment: {
        KILL_SWITCH_PARAM: props.killSwitchParamName,
        PAUSED_QUEUE_URL: paused.queueUrl,
        PLAN_QUEUE_URL: plan.queue.queueUrl,
        SHARD_FUNCTION_NAME: shardFn.functionName,
      },
    });
    pausedConsumer.addEventSource(
      new SqsEventSource(paused, { batchSize: 10, reportBatchItemFailures: true }),
    );
    allow(pausedConsumer, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    allow(pausedConsumer, ['sqs:SendMessage'], [paused.queueArn, plan.queue.queueArn]);
    allow(pausedConsumer, ['lambda:InvokeFunction'], [shardFn.functionArn]);

    // Cell-index outbox drainer (every minute): redoes the cell-index writes the API did not
    // finish (a crash between the Aurora commit and the DynamoDB write), from Aurora's truth.
    // The late-join queue lives in LateJoinStack, which depends on this stack, so it is addressed
    // by its fixed name (no cross-stack reference, no cycle).
    const lateJoinQueueArn = this.formatArn({
      service: 'sqs',
      resource: latejoinQueueName(props.stage),
    });
    const lateJoinQueueUrl = `https://sqs.${this.region}.amazonaws.com/${this.account}/${latejoinQueueName(props.stage)}`;
    const drainer = new NodejsFunction(this, 'OutboxDrainerFunction', {
      ...common,
      entry: fileURLToPath(
        new URL('../../backend/src/scale/cell-index/drainer.ts', import.meta.url),
      ),
      handler: 'handler',
      timeout: Duration.seconds(OUTBOX_DRAINER_TIMEOUT_SECONDS),
      memorySize: 256,
      logGroup: logs('OutboxDrainerLogs'),
      environment: {
        ...dbEnv(props.db, 'ops'),
        CELL_INDEX_TABLE: cellIndex.tableName,
        // A device the drainer indexes late is sent the active alerts (late-join intent).
        LATEJOIN_QUEUE_URL: lateJoinQueueUrl,
      },
    });
    grantDb(drainer, props.db, 'ops');
    allow(drainer, ['sqs:SendMessage'], [lateJoinQueueArn]);
    allow(
      drainer,
      [
        'dynamodb:BatchWriteItem',
        'dynamodb:PutItem',
        'dynamodb:DeleteItem',
        'dynamodb:UpdateItem',
        'dynamodb:Query',
      ],
      [cellIndex.tableArn],
    );
    const drainerMinutes = DRAINER_MINUTES[props.stage];
    new Rule(this, 'OutboxDrainerSchedule', {
      schedule: Schedule.rate(Duration.minutes(drainerMinutes)),
      targets: [new LambdaFunction(drainer)],
    });
    new Alarm(this, 'CellIndexOutboxStuckAlarm', {
      alarmDescription:
        'A cell-index outbox row failed 10 or more times: a device may be missing from the index and miss alerts',
      metric: new Metric({
        namespace: 'Relay/CellIndex',
        metricName: 'OutboxMaxAttempts',
        statistic: 'Maximum',
        period: Duration.minutes(1),
      }),
      threshold: OUTBOX_ALARM_ATTEMPTS - 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });
    new Alarm(this, 'DrainerNotRunningAlarm', {
      alarmDescription:
        'The cell-index outbox drainer has not reported for 3 schedule periods (5 minutes on prod): unfinished index writes are not being redone',
      metric: new Metric({
        namespace: 'Relay/CellIndex',
        metricName: 'OutboxPending',
        statistic: 'SampleCount',
        // Three schedule periods (5 on prod), so a slower dev schedule never looks like an outage.
        period: Duration.minutes(Math.max(5, drainerMinutes * 3)),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.LESS_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.BREACHING,
    });

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

    // Manual replay of the send DLQs (urgent, standard): SQS moves the messages back itself.
    const sendRedrive = new NodejsFunction(this, 'SendRedriveFunction', {
      ...common,
      entry: entry('send-redrive.ts'),
      handler: 'sendRedriveHandler',
      timeout: Duration.seconds(30),
      memorySize: 256,
      logGroup: logs('SendRedriveLogs'),
      environment: {
        URGENT_DLQ_ARN: urgent.dlq.queueArn,
        STANDARD_DLQ_ARN: standard.dlq.queueArn,
      },
    });
    // StartMessageMoveTask acts as this role: it must be able to read and delete the DLQs and
    // send to the source queues.
    allow(
      sendRedrive,
      [
        'sqs:StartMessageMoveTask',
        'sqs:ReceiveMessage',
        'sqs:DeleteMessage',
        'sqs:GetQueueAttributes',
      ],
      [urgent.dlq.queueArn, standard.dlq.queueArn],
    );
    allow(sendRedrive, ['sqs:SendMessage'], [urgent.queue.queueArn, standard.queue.queueArn]);

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
      [
        'ShardPausedDlqAlarm',
        pausedDlq,
        'Work parked during a pause was not resumed within 24 hours (or the resume failed): re-run the redrive for these jobs',
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
    this.urgentQueueName = urgent.queue.queueName;
    this.standardQueueArn = standard.queue.queueArn;
    this.standardQueueUrl = standard.queue.queueUrl;
    this.standardQueueName = standard.queue.queueName;
    this.shardFunctionName = shardFn.functionName;
    this.shardFunctionArn = shardFn.functionArn;
    this.shardDlqUrl = shardDlq.queueUrl;
    this.pausedQueueUrl = paused.queueUrl;

    new CfnOutput(this, 'CellIndexTableName', { value: this.cellIndexTableName });
    new CfnOutput(this, 'PlanQueueUrl', { value: this.planQueueUrl });
    new CfnOutput(this, 'UrgentQueueUrl', { value: this.urgentQueueUrl });
    new CfnOutput(this, 'StandardQueueUrl', { value: this.standardQueueUrl });
    new CfnOutput(this, 'ShardPausedQueueUrl', { value: this.pausedQueueUrl });
    new CfnOutput(this, 'ShardDlqUrl', { value: this.shardDlqUrl });
    new CfnOutput(this, 'RedriveFunctionName', { value: redrive.functionName });
    new CfnOutput(this, 'SendRedriveFunctionName', { value: sendRedrive.functionName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
