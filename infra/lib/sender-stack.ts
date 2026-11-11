import { fileURLToPath } from 'node:url';
import {
  Arn,
  ArnFormat,
  CfnOutput,
  Duration,
  RemovalPolicy,
  SecretValue,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import {
  Alarm,
  ComparisonOperator,
  MathExpression,
  Metric,
  TreatMissingData,
} from 'aws-cdk-lib/aws-cloudwatch';
import {
  FlowLogDestination,
  FlowLogTrafficType,
  GatewayVpcEndpointAwsService,
  Peer,
  Port,
  SecurityGroup,
  SubnetType,
  Vpc,
} from 'aws-cdk-lib/aws-ec2';
import { DockerImageAsset, Platform } from 'aws-cdk-lib/aws-ecr-assets';
import {
  AwsLogDriver,
  AlarmBehavior,
  Cluster,
  ContainerImage,
  ContainerInsights,
  CpuArchitecture,
  FargateService,
  FargateTaskDefinition,
  OperatingSystemFamily,
} from 'aws-cdk-lib/aws-ecs';
import { AdjustmentType } from 'aws-cdk-lib/aws-applicationautoscaling';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import {
  isSenderEnabled,
  SENDER_MAX_TASKS_PROD,
  senderClusterName,
  senderMaxTasks,
  senderServiceName,
} from './fanout-stack.js';
import { fcmProjectIds } from './fcm-projects.js';
import {
  alertVerifyEnv,
  alertVerifyStatement,
  dbEnv,
  grantDbToRole,
  type AlertSigningRefs,
  type DbRefs,
} from './grants.js';

/** One SQS queue, passed between stacks as plain strings (like every other prop). */
export interface QueueRefs {
  arn: string;
  url: string;
  name: string;
}

export interface SenderStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  /** emergency + warning batches. */
  urgentQueue: QueueRefs;
  /** watch + advisory batches (and nothing is ever starved: see the scaling comment). */
  standardQueue: QueueRefs;
  idempotencyTableName: string;
  idempotencyTableArn: string;
  cellIndexTableName: string;
  cellIndexTableArn: string;
  alertSigning: AlertSigningRefs;
  killSwitchParamName: string;
  killSwitchParamArn: string;
  /**
   * CDN origin for alert documents (e.g. https://alerts.example.org), from `-c publicAlertBaseUrl`.
   * Becomes PUBLIC_ALERT_BASE_URL; unset means pushes carry no alertUrl and phones use the API.
   */
  publicAlertBaseUrl?: string;
}

/** Fargate stop timeout: the consumer drains for 25 s and main.ts hard-exits at 29 s. */
export const SENDER_STOP_TIMEOUT_SECONDS = 30;
/** Prod fleet ceiling. Needs a Fargate vCPU quota of at least this many 1-vCPU tasks (README). */
export const SENDER_MAX_TASKS = SENDER_MAX_TASKS_PROD;
/** Prod floor when the fleet is enabled: warm tasks, so an alert never waits for a cold start. */
export const SENDER_MIN_TASKS_PROD = 10;
/** Context: override the prod minimum task count (default 10). */
export const SENDER_MIN_TASKS_CONTEXT = 'senderMinTasks';
/**
 * Context: per-Firebase-project send budget for the whole sender fleet, per second (default 9,000,
 * under the ~10,000/s quota). Each task sends budget / running tasks (FCM_PROJECT_FLEET_RATE_PER_SEC).
 */
export const FCM_PROJECT_FLEET_RATE_CONTEXT = 'fcmProjectFleetRatePerSec';
export const FCM_PROJECT_FLEET_RATE_DEFAULT = 9_000;

function fcmProjectFleetRate(scope: Construct): number {
  const raw = scope.node.tryGetContext(FCM_PROJECT_FLEET_RATE_CONTEXT);
  if (raw === undefined) return FCM_PROJECT_FLEET_RATE_DEFAULT;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`context ${FCM_PROJECT_FLEET_RATE_CONTEXT} must be a positive number`);
  }
  return n;
}

/** Scale in only after this many consecutive idle minutes. */
export const SENDER_IDLE_MINUTES_BEFORE_SCALE_IN = 15;
/** Context flag: when "true" the image comes from a public base image instead of building the Dockerfile. */
export const SKIP_IMAGE_CONTEXT = 'relay:skipSenderImageAsset';

const ASSET_EXCLUDES = [
  '**/node_modules',
  '**/dist',
  '**/cdk.out',
  '**/coverage',
  '.git',
  '.claude',
  'app',
  'agency',
  'docs',
];

/** Prod minimum task count: context senderMinTasks, default 10, an integer from 0 to the maximum. */
function senderMinTasks(scope: Construct, maxTasks: number): number {
  const raw = scope.node.tryGetContext(SENDER_MIN_TASKS_CONTEXT);
  if (raw === undefined) return SENDER_MIN_TASKS_PROD;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0 || n > maxTasks) {
    throw new Error(`context ${SENDER_MIN_TASKS_CONTEXT} must be an integer from 0 to ${maxTasks}`);
  }
  return n;
}

/**
 * The sender service: long-running Fargate tasks that drain the urgent and standard send queues
 * and push to APNs and FCM. Scaled on queue depth and age.
 *
 * Gating (see isSenderEnabled): prod runs the fleet by default, so a deploy can never scale it to 0
 * by accident. The APNs and FCM secrets are created with placeholder values, and a sender that
 * boots with them crash-loops, so the very first prod deploy (and any dev deploy before the
 * secrets are real) turns the fleet off explicitly:
 *   1. deploy with `-c senderEnabled=false -c iUnderstandSenderStops=true` (prod) or no flag (dev)
 *   2. put the real APNs and FCM values into the two secrets
 *   3. deploy again with no flag on prod, or `-c senderEnabled=true` on dev
 * The planner reads the same flag, so it never pre-scales a fleet that is off.
 *
 * A bad image rolls back: the ECS circuit breaker catches tasks that never start, and a deployment
 * alarm (running tasks below desired for 3 minutes) catches tasks that start and then die.
 */
export class SenderStack extends Stack {
  readonly clusterName: string;
  readonly serviceName: string;
  readonly apnsSecretArn: string;
  readonly fcmSecretArn: string;

  constructor(scope: Construct, id: string, props: SenderStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';
    const removalPolicy = isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY;

    // Public subnets, no NAT gateway. A NAT gateway costs about $33 a month plus $0.045 per GB
    // processed, and a statewide alert pushes gigabytes of APNs/FCM traffic through it. Tasks
    // instead get a public IP and reach Apple, Google and AWS directly through the internet
    // gateway (free). That is safe because the security group below has NO ingress at all (a task
    // cannot be reached from outside) and egress is limited to port 443. DynamoDB goes through a
    // free gateway endpoint so its traffic never leaves the AWS network.
    const vpc = new Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: 'public', subnetType: SubnetType.PUBLIC, cidrMask: 24 }],
      gatewayEndpoints: { dynamo: { service: GatewayVpcEndpointAwsService.DYNAMODB } },
      // NIST SC-7: the default security group is never used; strip its rules (CDK custom resource).
      restrictDefaultSecurityGroup: true,
    });
    // NIST AU-12 / SI-4: record the tasks' flows (all egress, and any rejected inbound probe).
    vpc.addFlowLog('FlowLogs', {
      destination: FlowLogDestination.toCloudWatchLogs(
        new LogGroup(this, 'VpcFlowLogs', { retention: RetentionDays.ONE_MONTH, removalPolicy }),
      ),
      trafficType: FlowLogTrafficType.ALL,
    });
    const securityGroup = new SecurityGroup(this, 'SenderSg', {
      vpc,
      description: 'Sender tasks: no inbound, outbound HTTPS only',
      allowAllOutbound: false,
    });
    securityGroup.addEgressRule(Peer.anyIpv4(), Port.tcp(443), 'APNs, FCM and AWS APIs over HTTPS');

    const cluster = new Cluster(this, 'Cluster', {
      // Fixed name: the fan-out stack builds the planner's pre-scale ARN from it (see fanout-stack.ts).
      clusterName: senderClusterName(props.stage),
      vpc,
      containerInsightsV2: ContainerInsights.ENABLED,
    });

    // Placeholders: put the real values in by hand after the first deploy (never in git).
    const apnsSecret = new Secret(this, 'ApnsKey', {
      secretName: `relay/${props.stage}/apns-key`,
      description: 'APNs auth key: JSON {keyId, teamId, bundleId, p8, sandbox}',
      secretStringValue: SecretValue.unsafePlainText(
        JSON.stringify({
          keyId: 'REPLACE_ME',
          teamId: 'REPLACE_ME',
          bundleId: 'REPLACE_ME',
          p8: 'REPLACE_ME',
          sandbox: true,
        }),
      ),
      removalPolicy,
    });
    // Android pushes are spread over N Firebase projects (PERF-1). With none configured the
    // secret holds one service account object (one project); with N ids it holds a JSON array of
    // N service accounts, one per project id. The sender checks that the two agree at startup.
    const projectIds = fcmProjectIds(this);
    const fcmPlaceholder =
      projectIds.length === 0
        ? { type: 'service_account', project_id: 'REPLACE_ME' }
        : projectIds.map((id) => ({ type: 'service_account', project_id: id }));
    const fcmSecret = new Secret(this, 'FcmServiceAccount', {
      secretName: `relay/${props.stage}/fcm-service-account`,
      description:
        'Firebase service account JSON for FCM HTTP v1: one object, or an array with one per Firebase project',
      secretStringValue: SecretValue.unsafePlainText(JSON.stringify(fcmPlaceholder)),
      removalPolicy,
    });

    const logGroup = new LogGroup(this, 'SenderLogs', {
      retention: RetentionDays.ONE_MONTH,
      removalPolicy,
    });

    const taskDef = new FargateTaskDefinition(this, 'TaskDef', {
      cpu: 1024,
      memoryLimitMiB: 2048,
      runtimePlatform: {
        cpuArchitecture: CpuArchitecture.ARM64,
        operatingSystemFamily: OperatingSystemFamily.LINUX,
      },
    });

    // Tests and quick synths set the context flag so no image is hashed or built (building needs
    // Docker). Real deploys build backend/Dockerfile.sender from the repository root.
    const skipImage = String(this.node.tryGetContext(SKIP_IMAGE_CONTEXT) ?? 'false') === 'true';
    const image = skipImage
      ? ContainerImage.fromRegistry('public.ecr.aws/docker/library/node:22-slim')
      : ContainerImage.fromDockerImageAsset(
          new DockerImageAsset(this, 'SenderImage', {
            directory: fileURLToPath(new URL('../../', import.meta.url)),
            file: 'backend/Dockerfile.sender',
            platform: Platform.LINUX_ARM64,
            exclude: ASSET_EXCLUDES,
          }),
        );

    taskDef.addContainer('sender', {
      image,
      // SEC-C-14: the process writes no files (no temp dir, no disk cache; logs go to stdout), so
      // the root filesystem is read-only. If a future dependency needs scratch space, add a task
      // volume mounted at /tmp instead of lifting this. The non-root user comes from the image (USER node).
      readonlyRootFilesystem: true,
      stopTimeout: Duration.seconds(SENDER_STOP_TIMEOUT_SECONDS),
      logging: new AwsLogDriver({ streamPrefix: 'sender', logGroup }),
      environment: {
        ...dbEnv(props.db, 'sender'),
        URGENT_QUEUE_URL: props.urgentQueue.url,
        STANDARD_QUEUE_URL: props.standardQueue.url,
        IDEMPOTENCY_TABLE: props.idempotencyTableName,
        CELL_INDEX_TABLE: props.cellIndexTableName,
        ...alertVerifyEnv(props.alertSigning),
        KILL_SWITCH_PARAM: props.killSwitchParamName,
        APNS_KEY_SECRET_ARN: apnsSecret.secretArn,
        FCM_SERVICE_ACCOUNT_SECRET_ARN: fcmSecret.secretArn,
        // The sender counts its own fleet (ecs:DescribeServices) to split the FCM budget.
        ECS_CLUSTER: senderClusterName(props.stage),
        ECS_SERVICE: senderServiceName(props.stage),
        // Fleet size to assume while ECS cannot be read: the fleet maximum (fail safe).
        FCM_FLEET_SIZE_FALLBACK: String(senderMaxTasks(props.stage)),
        FCM_PROJECT_FLEET_RATE_PER_SEC: String(fcmProjectFleetRate(this)),
        ...(props.publicAlertBaseUrl ? { PUBLIC_ALERT_BASE_URL: props.publicAlertBaseUrl } : {}),
        ...(projectIds.length > 0 ? { FCM_PROJECT_IDS: projectIds.join(',') } : {}),
      },
    });

    // Grants use explicit ARNs and explicit actions (no wildcards).
    const allow = (actions: string[], resources: string[]) =>
      taskDef.taskRole.addToPrincipalPolicy(
        new PolicyStatement({ effect: Effect.ALLOW, actions, resources }),
      );
    const queueArns = [props.urgentQueue.arn, props.standardQueue.arn];
    // Receive and settle messages; send is for re-queuing failed recipients.
    allow(
      [
        'sqs:ReceiveMessage',
        'sqs:DeleteMessage',
        'sqs:ChangeMessageVisibility',
        'sqs:GetQueueAttributes',
        'sqs:SendMessage',
      ],
      queueArns,
    );
    // Idempotency: claim (PutItem), check (GetItem), markSent (UpdateItem), release (DeleteItem).
    allow(
      ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem', 'dynamodb:DeleteItem'],
      [props.idempotencyTableArn],
    );
    // Dead targets are cleared in the cell index.
    allow(['dynamodb:UpdateItem', 'dynamodb:DeleteItem'], [props.cellIndexTableArn]);
    grantDbToRole(taskDef.taskRole, props.db, 'sender');
    allow(['secretsmanager:GetSecretValue'], [apnsSecret.secretArn, fcmSecret.secretArn]);
    // Verify only: the sender fetches the alert public key once and never holds kms:Sign.
    taskDef.taskRole.addToPrincipalPolicy(alertVerifyStatement(props.alertSigning));
    allow(['ssm:GetParameter'], [props.killSwitchParamArn]);
    // Read the running task count of its own service (FCM fleet rate); a built ARN, no cycle.
    allow(
      ['ecs:DescribeServices'],
      [
        Arn.format(
          {
            service: 'ecs',
            resource: 'service',
            resourceName: `${senderClusterName(props.stage)}/${senderServiceName(props.stage)}`,
            arnFormat: ArnFormat.SLASH_RESOURCE_NAME,
          },
          this,
        ),
      ],
    );

    const enabled = isSenderEnabled(this, props.stage);

    // Running tasks below desired for 3 minutes (Container Insights). Names are the fixed physical
    // names, not the service object, because the service takes this alarm as a deployment alarm.
    // It doubles as the deployment-rollback signal below. FILL: a task-less service may stop
    // publishing RunningTaskCount, which must count as 0.
    const taskMetric = (metricName: string) =>
      new Metric({
        namespace: 'ECS/ContainerInsights',
        metricName,
        dimensionsMap: {
          ClusterName: senderClusterName(props.stage),
          ServiceName: senderServiceName(props.stage),
        },
        statistic: 'Minimum',
        period: Duration.minutes(1),
      });
    const runningBelowDesiredAlarm = enabled
      ? new Alarm(this, 'RunningTasksBelowDesiredAlarm', {
          alarmDescription:
            'Sender running tasks have been below desired for 3 minutes: the fleet is down or crash-looping',
          metric: new MathExpression({
            expression: 'FILL(desired, 0) - FILL(running, 0)',
            usingMetrics: {
              desired: taskMetric('DesiredTaskCount'),
              running: taskMetric('RunningTaskCount'),
            },
            period: Duration.minutes(1),
          }),
          threshold: 0,
          evaluationPeriods: 3,
          datapointsToAlarm: 3,
          comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
          treatMissingData: TreatMissingData.NOT_BREACHING,
        })
      : undefined;

    const service = new FargateService(this, 'Service', {
      cluster,
      serviceName: senderServiceName(props.stage),
      taskDefinition: taskDef,
      securityGroups: [securityGroup],
      vpcSubnets: { subnetType: SubnetType.PUBLIC },
      assignPublicIp: true, // required without a NAT: this is how tasks reach the internet
      minHealthyPercent: 100,
      maxHealthyPercent: 200,
      circuitBreaker: { rollback: true },
      // A deploy whose tasks start and then die trips the alarm and rolls back to the last good image.
      ...(runningBelowDesiredAlarm
        ? {
            deploymentAlarms: {
              alarmNames: [runningBelowDesiredAlarm.alarmName],
              behavior: AlarmBehavior.ROLLBACK_ON_ALARM,
            },
          }
        : {}),
      // Off (0) only when the fleet is disabled; otherwise left to auto scaling so a deploy never
      // resets the fleet size.
      ...(enabled ? {} : { desiredCount: 0 }),
    });

    // ---- Auto scaling ----
    // Scale out fast, in slowly. Depth is both queues, because with a minimum of 0 (dev) a
    // standard-only alert must still wake the fleet; the urgent queue is polled first anyway
    // (strict priority), so adding standard depth never delays an emergency. Age is urgent only.
    const sqsMetric = (queue: QueueRefs, metricName: string, statistic = 'Maximum') =>
      new Metric({
        namespace: 'AWS/SQS',
        metricName,
        dimensionsMap: { QueueName: queue.name },
        statistic,
        period: Duration.minutes(1),
      });
    const visible = new MathExpression({
      expression: 'FILL(u, 0) + FILL(s, 0)',
      usingMetrics: {
        u: sqsMetric(props.urgentQueue, 'ApproximateNumberOfMessagesVisible'),
        s: sqsMetric(props.standardQueue, 'ApproximateNumberOfMessagesVisible'),
      },
      period: Duration.minutes(1),
    });
    const busy = new MathExpression({
      expression: 'FILL(uv, 0) + FILL(un, 0) + FILL(sv, 0) + FILL(sn, 0)',
      usingMetrics: {
        uv: sqsMetric(props.urgentQueue, 'ApproximateNumberOfMessagesVisible'),
        un: sqsMetric(props.urgentQueue, 'ApproximateNumberOfMessagesNotVisible'),
        sv: sqsMetric(props.standardQueue, 'ApproximateNumberOfMessagesVisible'),
        sn: sqsMetric(props.standardQueue, 'ApproximateNumberOfMessagesNotVisible'),
      },
      period: Duration.minutes(1),
    });
    const urgentAge = sqsMetric(props.urgentQueue, 'ApproximateAgeOfOldestMessage');

    const maxTasks = senderMaxTasks(props.stage);
    const minTasks = isProd ? senderMinTasks(this, maxTasks) : 0;
    // Without the flag there is no scaling at all: the service stays at 0 tasks.
    const scaling = enabled
      ? service.autoScaleTaskCount({ minCapacity: minTasks, maxCapacity: maxTasks })
      : undefined;
    // Idle SQS queues stop publishing metrics, so each input is FILLed with 0; otherwise the
    // scale-in alarm would sit in INSUFFICIENT_DATA forever. All metrics use 1-minute periods.
    // Out on depth (a message holds up to 10 batches, about 3,000 recipients; 1M recipients is
    // about 350 messages). Steps are aggressive: ramp time, not cost, is the risk.
    scaling?.scaleOnMetric('ScaleOutOnDepth', {
      metric: visible,
      adjustmentType: AdjustmentType.CHANGE_IN_CAPACITY,
      scalingSteps: [
        { upper: 0, change: 0 },
        { lower: 1, change: +5 },
        { lower: 30, change: +15 },
        { lower: 150, change: +30 },
        { lower: 400, change: +50 },
      ],
      evaluationPeriods: 1,
      cooldown: Duration.seconds(30),
    });
    // Out on age: work waiting longer than 10 s means the fleet is too small right now.
    scaling?.scaleOnMetric('ScaleOutOnAge', {
      metric: urgentAge,
      adjustmentType: AdjustmentType.CHANGE_IN_CAPACITY,
      scalingSteps: [
        { upper: 10, change: 0 },
        { lower: 10, change: +10 },
        { lower: 30, change: +25 },
        { lower: 60, change: +50 },
      ],
      evaluationPeriods: 1,
      cooldown: Duration.seconds(30),
    });
    // In: only after 15 quiet minutes (no visible AND no in-flight messages), a few tasks at a
    // time every 5 minutes, never below the minimum. A statewide alert has long tails (retries)
    // and the check-in burst follows it, so we hold capacity rather than flap.
    scaling?.scaleOnMetric('ScaleInWhenIdle', {
      metric: busy,
      adjustmentType: AdjustmentType.CHANGE_IN_CAPACITY,
      scalingSteps: [
        { upper: 0, change: -5 },
        { lower: 1, change: 0 },
      ],
      evaluationPeriods: SENDER_IDLE_MINUTES_BEFORE_SCALE_IN,
      cooldown: Duration.minutes(5),
    });

    new Alarm(this, 'UrgentBacklogAgeAlarm', {
      alarmDescription:
        'Urgent send queue has waited over 2 minutes: alerts are reaching phones late',
      metric: urgentAge,
      threshold: 120,
      evaluationPeriods: 2,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
    });

    // ---- Sender self-reported metrics (EMF, namespace Relay/Sender) ----
    // PermanentBreakerOpen and DeadTargetDropped have no dimensions; GaveUp is per Queue.
    const senderMetric = (metricName: string, dimensionsMap?: Record<string, string>) =>
      new Metric({
        namespace: 'Relay/Sender',
        metricName,
        statistic: 'Sum',
        period: Duration.minutes(1),
        ...(dimensionsMap ? { dimensionsMap } : {}),
      });
    const atLeastOne = (
      alarmId: string,
      alarmDescription: string,
      metric: Metric | MathExpression,
    ) =>
      new Alarm(this, alarmId, {
        alarmDescription,
        metric,
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      });
    atLeastOne(
      'PermanentBreakerOpenAlarm',
      'Sender permanent-failure breaker opened: pushes are failing for good (bad APNs/FCM credentials?)',
      senderMetric('PermanentBreakerOpen'),
    );
    atLeastOne(
      'GaveUpAlarm',
      'Sender gave up on recipients (retry window expired): some people were not notified',
      new MathExpression({
        expression: 'FILL(u, 0) + FILL(s, 0)',
        usingMetrics: {
          u: senderMetric('GaveUp', { Queue: 'urgent' }),
          s: senderMetric('GaveUp', { Queue: 'standard' }),
        },
        period: Duration.minutes(1),
      }),
    );
    atLeastOne(
      'DeadTargetDroppedAlarm',
      'Sender dropped dead-token cleanups (queue full): stale tokens are piling up',
      senderMetric('DeadTargetDropped'),
    );

    this.clusterName = cluster.clusterName;
    this.serviceName = service.serviceName;
    this.apnsSecretArn = apnsSecret.secretArn;
    this.fcmSecretArn = fcmSecret.secretArn;

    new CfnOutput(this, 'SenderClusterName', { value: cluster.clusterName });
    new CfnOutput(this, 'ApnsSecretArn', { value: apnsSecret.secretArn });
    new CfnOutput(this, 'FcmSecretArn', { value: fcmSecret.secretArn });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
