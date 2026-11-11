import {
  Aspects,
  CfnOutput,
  Duration,
  Fn,
  IAspect,
  RemovalPolicy,
  Stack,
  StackProps,
  Tags,
} from 'aws-cdk-lib';
import { CfnBudget } from 'aws-cdk-lib/aws-budgets';
import { CfnAnomalyMonitor, CfnAnomalySubscription } from 'aws-cdk-lib/aws-ce';
import { Trail } from 'aws-cdk-lib/aws-cloudtrail';
import {
  Alarm,
  ComparisonOperator,
  IMetric,
  Metric,
  TreatMissingData,
} from 'aws-cdk-lib/aws-cloudwatch';
import { SnsAction } from 'aws-cdk-lib/aws-cloudwatch-actions';
import { StepScalingPolicy } from 'aws-cdk-lib/aws-applicationautoscaling';
import { Function as LambdaFunction } from 'aws-cdk-lib/aws-lambda';
import { CfnDetector } from 'aws-cdk-lib/aws-guardduty';
import { Rule, RuleTargetInput, EventField } from 'aws-cdk-lib/aws-events';
import { SnsTopic } from 'aws-cdk-lib/aws-events-targets';
import { Effect, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import {
  CfnLogGroup,
  FilterPattern,
  ILogGroup,
  LogGroup,
  MetricFilter,
  RetentionDays,
} from 'aws-cdk-lib/aws-logs';
import {
  BlockPublicAccess,
  Bucket,
  BucketEncryption,
  ObjectLockRetention,
} from 'aws-cdk-lib/aws-s3';
import { Topic, ITopic } from 'aws-cdk-lib/aws-sns';
import { EmailSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import { Construct, IConstruct } from 'constructs';
import { accessLogsBucket, logAccessTo } from './buckets.js';

/** Monitoring settings. bin/relay.ts reads them from CDK context; every field is optional. */
export interface MonitoringOptions {
  /** Context `alarmEmail`: subscribed to the alerts topic. Required on prod (see allowNoAlarmEmail). */
  alarmEmail?: string;
  /** Context `allowNoAlarmEmail=true`: lets prod synth without an alarm email (alarms then page nobody). */
  allowNoAlarmEmail?: boolean;
  /** Context `enableGuardDuty`: default true on prod, false on dev. One detector per region per account. */
  enableGuardDuty?: boolean;
  /** Context `logRetentionDays`: default 365 on prod, 30 on dev. Applied to every log group. */
  logRetentionDays?: number;
  /** Context `monthlyBudgetUsd`: default 600 on prod, 50 on dev. */
  monthlyBudgetUsd?: number;
  /**
   * Context `trailObjectLockDays`: default Object Lock retention (governance mode) of every trail
   * log object. Default 365 on prod, 1 on dev (so test evidence does not pile up undeletable).
   */
  trailObjectLockDays?: number;
}

export const DEFAULT_LOG_RETENTION_DAYS = { dev: 30, prod: 365 } as const;
export const DEFAULT_MONTHLY_BUDGET_USD = { dev: 50, prod: 600 } as const;
/** CloudTrail log files are kept for a year. */
export const TRAIL_RETENTION_DAYS = 365;
/** Default Object Lock retention of trail objects (governance mode), by stage. */
export const DEFAULT_TRAIL_OBJECT_LOCK_DAYS = { dev: 1, prod: 365 } as const;

/** Values CloudWatch Logs accepts for `retentionInDays`. */
const VALID_RETENTION = new Set<number>(
  Object.values(RetentionDays).filter((v): v is number => typeof v === 'number'),
);

const oneMinute = Duration.minutes(1);

/** Secrets Manager events that change a secret, its policy or its replicas. */
export const SECRET_EVENTS = [
  'PutSecretValue',
  'UpdateSecret',
  'DeleteSecret',
  'RotateSecret',
  'RestoreSecret',
  'PutResourcePolicy',
  'DeleteResourcePolicy',
  'ReplicateSecretToRegions',
] as const;
/** KMS events on the alert-signing keys themselves. */
export const KMS_KEY_EVENTS = [
  'ScheduleKeyDeletion',
  'CancelKeyDeletion',
  'DisableKey',
  'EnableKey',
  'PutKeyPolicy',
  'CreateGrant',
] as const;
/** KMS events on their aliases. */
export const KMS_ALIAS_EVENTS = ['CreateAlias', 'UpdateAlias', 'DeleteAlias'] as const;

export const alertsTopicName = (stage: string) => `relay-${stage}-alerts`;

export interface MonitoringStackProps extends StackProps, MonitoringOptions {
  stage: 'dev' | 'prod';
  /** SSM parameter name of the delivery kill switch (changes to it are reported). */
  killSwitchParamName: string;
  /** ARNs of every alert-signing KMS key (ConfigStack). Changes to them or their aliases are reported. */
  alertSigningKeyArns: string[];
  /**
   * ARNs of the customer-managed data-domain keys of this region (ConfigStack: data, messaging,
   * logs; encryption.ts). Disabling, deleting or re-policying one of them is reported.
   */
  dataKeyArns?: string[];
}

/**
 * KMS events reported for the data-domain keys. CreateGrant is left out on purpose: DynamoDB and
 * RDS create grants on these keys in normal operation, so it would page on every deploy.
 */
export const DATA_KEY_EVENTS = KMS_KEY_EVENTS.filter((e) => e !== 'CreateGrant');
/** Alias names of the data-domain keys (encryption.ts keyAlias). */
export const DATA_KEY_ALIAS_NAMES = ['data', 'messaging', 'logs'] as const;

/**
 * The alerting and audit backbone. Owns the SNS topic every CloudWatch alarm in every stack
 * notifies (via AlarmActionsAspect), the CloudTrail trail, the EventBridge rules that report
 * kill-switch, secret and IAM changes, GuardDuty and the cost budget. It depends on nothing but
 * the kill-switch parameter name, so every other stack can depend on it.
 */
export class MonitoringStack extends Stack {
  readonly topic: ITopic;
  readonly topicArn: string;
  readonly logRetentionDays: number;

  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);
    const { stage } = props;
    const isProd = stage === 'prod';

    const email = props.alarmEmail?.trim();
    if (isProd && !email && !props.allowNoAlarmEmail) {
      throw new Error(
        'prod needs an alarm email: pass -c alarmEmail=oncall@example.com ' +
          '(or -c allowNoAlarmEmail=true to deploy knowing no alarm will reach anyone)',
      );
    }
    const retention = props.logRetentionDays ?? DEFAULT_LOG_RETENTION_DAYS[stage];
    if (!VALID_RETENTION.has(retention)) {
      throw new Error(`logRetentionDays ${retention} is not a value CloudWatch Logs accepts`);
    }
    this.logRetentionDays = retention;

    const topic = new Topic(this, 'AlertsTopic', {
      topicName: alertsTopicName(stage),
      displayName: `Relay ${stage} alerts`,
      enforceSSL: true,
    });
    if (email) topic.addSubscription(new EmailSubscription(email));
    const sourceAccount = { StringEquals: { 'aws:SourceAccount': this.account } };
    // CloudWatch alarms (also added by SnsAction, kept explicit), Budgets and Cost Anomaly
    // Detection publish with their own service principals.
    topic.addToResourcePolicy(
      new PolicyStatement({
        sid: 'CloudWatchAlarms',
        effect: Effect.ALLOW,
        principals: [new ServicePrincipal('cloudwatch.amazonaws.com')],
        actions: ['sns:Publish'],
        resources: [topic.topicArn],
        conditions: sourceAccount,
      }),
    );
    topic.addToResourcePolicy(
      new PolicyStatement({
        sid: 'Budgets',
        effect: Effect.ALLOW,
        principals: [new ServicePrincipal('budgets.amazonaws.com')],
        actions: ['sns:Publish'],
        resources: [topic.topicArn],
        conditions: sourceAccount,
      }),
    );
    topic.addToResourcePolicy(
      new PolicyStatement({
        sid: 'CostAnomalyDetection',
        effect: Effect.ALLOW,
        principals: [new ServicePrincipal('costalerts.amazonaws.com')],
        actions: ['sns:Publish'],
        resources: [topic.topicArn],
      }),
    );
    this.topic = topic;
    this.topicArn = topic.topicArn;

    // ---- Audit trail ----
    const lockDays = props.trailObjectLockDays ?? DEFAULT_TRAIL_OBJECT_LOCK_DAYS[stage];
    if (!Number.isInteger(lockDays) || lockDays < 1 || lockDays > 36_500) {
      throw new Error(`trailObjectLockDays ${lockDays} must be a whole number of days, 1 to 36500`);
    }
    const trailBucket = new Bucket(this, 'TrailBucket', {
      encryption: BucketEncryption.S3_MANAGED,
      versioned: true,
      // NIST AU-9: no trail object version can be overwritten or deleted for `lockDays` unless the
      // caller has s3:BypassGovernanceRetention and asks for it explicitly (governance mode; an
      // account administrator still can, and that call is itself in the trail). Object Lock can
      // only be switched on for a new bucket here (and never off), so set it before first deploy.
      objectLockEnabled: true,
      objectLockDefaultRetention: ObjectLockRetention.governance(Duration.days(lockDays)),
      enforceSSL: true,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          expiration: Duration.days(TRAIL_RETENTION_DAYS),
          noncurrentVersionExpiration: Duration.days(TRAIL_RETENTION_DAYS),
        },
      ],
      // Audit evidence outlives the stack on every stage.
      removalPolicy: RemovalPolicy.RETAIN,
    });
    // Who read or deleted audit evidence (NIST AU-9).
    logAccessTo(trailBucket, accessLogsBucket(this, isProd), 'trail/');
    const trail = new Trail(this, 'Trail', {
      trailName: `relay-${stage}-trail`,
      bucket: trailBucket,
      isMultiRegionTrail: true,
      includeGlobalServiceEvents: true,
      enableFileValidation: true,
      sendToCloudWatchLogs: true,
      // Overridden by LogRetentionAspect; stated here so the intent survives without it.
      cloudWatchLogsRetention: RetentionDays.ONE_YEAR,
    });
    const trailLogs = trail.logGroup;
    if (!trailLogs) throw new Error('trail log group missing');

    // ---- Security events, reported to the topic ----
    const report = (
      ruleId: string,
      source: string,
      description: string,
      detail: Record<string, unknown>,
      headline = 'security event',
    ) =>
      new Rule(this, ruleId, {
        description,
        eventPattern: {
          source: [source],
          detailType: ['AWS API Call via CloudTrail'],
          detail,
        },
        targets: [
          new SnsTopic(topic, {
            message: RuleTargetInput.fromText(
              `Relay ${stage} ${headline}: ${EventField.fromPath('$.detail.eventName')} ` +
                `by ${EventField.fromPath('$.detail.userIdentity.arn')} ` +
                `at ${EventField.fromPath('$.detail.eventTime')} ` +
                `from ${EventField.fromPath('$.detail.sourceIPAddress')}`,
            ),
          }),
        ],
      });
    // PutParameter / DeleteParameter name one parameter; DeleteParameters (plural) lists them in
    // `names`. An EventBridge pattern on an array field matches when any element matches.
    const ssmDetail = (names: string[], field: 'name' | 'names') => ({
      eventSource: ['ssm.amazonaws.com'],
      eventName: names,
      requestParameters: { [field]: [props.killSwitchParamName] },
    });
    report(
      'KillSwitchChangedRule',
      'aws.ssm',
      'The delivery kill switch parameter was written or deleted',
      ssmDetail(['PutParameter', 'DeleteParameter'], 'name'),
    );
    report(
      'KillSwitchBulkDeleteRule',
      'aws.ssm',
      'The delivery kill switch parameter was deleted through DeleteParameters',
      ssmDetail(['DeleteParameters'], 'names'),
    );
    // Deleting the parameter is worse than a change: a missing parameter means "running", so the
    // switch silently stops working. Its own rule and wording so it is not read as routine.
    report(
      'KillSwitchDeletedRule',
      'aws.ssm',
      'The delivery kill switch parameter was DELETED: a missing parameter counts as running',
      ssmDetail(['DeleteParameter'], 'name'),
      'KILL SWITCH PARAMETER DELETED (a missing parameter fails open to running; recreate it)',
    );
    report(
      'KillSwitchBulkDeletedRule',
      'aws.ssm',
      'The delivery kill switch parameter was DELETED (DeleteParameters): a missing parameter counts as running',
      ssmDetail(['DeleteParameters'], 'names'),
      'KILL SWITCH PARAMETER DELETED (a missing parameter fails open to running; recreate it)',
    );
    report(
      'SecretChangedRule',
      'aws.secretsmanager',
      'A relay secret (keys, push credentials, DB passwords incl. the Aurora admin secret) was written, updated, deleted, restored, rotated, replicated or had its resource policy changed',
      {
        eventSource: ['secretsmanager.amazonaws.com'],
        eventName: [...SECRET_EVENTS],
        // secretId is a name or a full ARN depending on the caller; match both. Every Relay
        // secret is named relay/<stage>/... (a test pins that, the Aurora admin secret included).
        requestParameters: { secretId: [{ wildcard: `*relay/${stage}/*` }] },
      },
    );
    // The alert-signing keys (KMS). The key events name the key by id, ARN or alias; the alias
    // events carry the alias name. Anyone who can use or re-policy a key can forge alerts.
    const keyIds = props.alertSigningKeyArns.map((arn) => ({
      wildcard: Fn.join('', ['*', Fn.select(1, Fn.split('/', arn))]),
    }));
    const aliasPattern = { wildcard: `*alias/relay/${stage}/alert-signing-*` };
    report(
      'AlertSigningKeyChangedRule',
      'aws.kms',
      'An alert-signing KMS key was disabled, scheduled for deletion, re-enabled, had its key policy changed, or got a grant',
      {
        eventSource: ['kms.amazonaws.com'],
        eventName: [...KMS_KEY_EVENTS],
        requestParameters: { keyId: [...keyIds, aliasPattern] },
      },
      'ALERT SIGNING KEY event',
    );
    report(
      'AlertSigningAliasChangedRule',
      'aws.kms',
      'An alert-signing KMS key alias was created, repointed or deleted',
      {
        eventSource: ['kms.amazonaws.com'],
        eventName: [...KMS_ALIAS_EVENTS],
        requestParameters: { aliasName: [aliasPattern] },
      },
      'ALERT SIGNING KEY ALIAS event',
    );
    // The data-domain keys (encryption.ts). Disabling or deleting one makes every table, queue,
    // topic or log group it protects unreadable, so it is reported like the signing keys.
    const dataKeyArns = props.dataKeyArns ?? [];
    if (dataKeyArns.length > 0) {
      const dataAliases = DATA_KEY_ALIAS_NAMES.map((n) => ({
        wildcard: `*alias/relay/${stage}/${n}`,
      }));
      report(
        'DataKeyChangedRule',
        'aws.kms',
        'A Relay data, messaging or logs KMS key was disabled, scheduled for deletion, re-enabled or had its key policy changed',
        {
          eventSource: ['kms.amazonaws.com'],
          eventName: [...DATA_KEY_EVENTS],
          requestParameters: {
            keyId: [
              ...dataKeyArns.map((arn) => ({
                wildcard: Fn.join('', ['*', Fn.select(1, Fn.split('/', arn))]),
              })),
              ...dataAliases,
            ],
          },
        },
        'DATA KEY event',
      );
      report(
        'DataKeyAliasChangedRule',
        'aws.kms',
        'A Relay data, messaging or logs KMS key alias was created, repointed or deleted',
        {
          eventSource: ['kms.amazonaws.com'],
          eventName: [...KMS_ALIAS_EVENTS],
          requestParameters: { aliasName: dataAliases },
        },
        'DATA KEY ALIAS event',
      );
    }

    // ---- Kill-switch health (metrics from backend/src/config/kill-switch.ts, EMF) ----
    // Delivery stops on its own when the switch is mis-set or unreadable. Without these alarms
    // the only signal was the shard-paused DLQ alarm about a day later.
    const killMetric = (metricName: string, statistic: string, period: Duration) =>
      new Metric({
        namespace: 'Relay/Config',
        metricName,
        dimensionsMap: { Source: 'killswitch' },
        statistic,
        period,
      });
    const fiveMinutes = Duration.minutes(5);
    const killAlarm = (id: string, metricName: string, description: string) =>
      new Alarm(this, id, {
        alarmDescription: description,
        metric: killMetric(metricName, 'Sum', fiveMinutes),
        threshold: 1,
        evaluationPeriods: 1,
        comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: TreatMissingData.NOT_BREACHING,
      });
    killAlarm(
      'KillSwitchInvalidValueAlarm',
      'KillSwitchInvalidValue',
      `Delivery is PAUSED because ${props.killSwitchParamName} holds a value other than running/paused ` +
        '(a typo?). Check the parameter value in SSM and set it to "running" or "paused".',
    );
    killAlarm(
      'KillSwitchFailedClosedAlarm',
      'KillSwitchFailedClosed',
      'Delivery is PAUSED because the kill switch could not be read from SSM and the last good value ' +
        `is older than 10 minutes (or never loaded). Check SSM availability/throttling and the IAM ` +
        `grant on ${props.killSwitchParamName}.`,
    );
    killAlarm(
      'KillSwitchParameterMissingAlarm',
      'KillSwitchParameterMissing',
      `${props.killSwitchParamName} does not exist; a missing parameter counts as RUNNING, so the ` +
        'kill switch cannot stop delivery. Recreate it (value "running") by redeploying the config stack.',
    );
    // Heartbeat: ingest reads the switch every minute, so a paused switch reports every minute.
    // Minimum over the minute is 1 only when every read in it said paused. 14 of 15 minutes
    // tolerates one minute without a read.
    new Alarm(this, 'KillSwitchPausedAlarm', {
      alarmDescription:
        'Delivery has been paused for about 15 minutes (kill switch "paused", or an invalid value, ' +
        `or SSM unreadable). If intentional, this is a reminder; otherwise set ${props.killSwitchParamName} ` +
        'to "running" and check the other KillSwitch alarms for the cause.',
      metric: killMetric('KillSwitchPaused', 'Minimum', oneMinute),
      threshold: 1,
      evaluationPeriods: 15,
      datapointsToAlarm: 14,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // IAM is a global service: its events are recorded in us-east-1, where no us-west-2 EventBridge
    // rule can see them. The multi-region trail delivers them to this log group, so IAM policy
    // changes are caught by a metric filter on the trail's log group instead, and the alarm goes
    // through the same topic. CloudFormation's own changes (deploys) are excluded.
    const iamEvents = [
      'PutRolePolicy',
      'DeleteRolePolicy',
      'AttachRolePolicy',
      'DetachRolePolicy',
      'PutUserPolicy',
      'DeleteUserPolicy',
      'AttachUserPolicy',
      'DetachUserPolicy',
      'PutGroupPolicy',
      'DeleteGroupPolicy',
      'AttachGroupPolicy',
      'DetachGroupPolicy',
      'CreatePolicy',
      'DeletePolicy',
      'CreatePolicyVersion',
      'DeletePolicyVersion',
      'SetDefaultPolicyVersion',
      'UpdateAssumeRolePolicy',
    ];
    const iamPattern =
      `{ ($.eventSource = "iam.amazonaws.com") && (${iamEvents
        .map((e) => `$.eventName = "${e}"`)
        .join(' || ')}) && ` +
      `(($.userIdentity.invokedBy NOT EXISTS) || ($.userIdentity.invokedBy != "cloudformation.amazonaws.com")) }`;
    new MetricFilter(this, 'IamPolicyChangeFilter', {
      logGroup: trailLogs,
      metricNamespace: 'Relay/Security',
      metricName: 'IamPolicyChange',
      filterPattern: FilterPattern.literal(iamPattern),
      metricValue: '1',
      defaultValue: 0,
    });
    new Alarm(this, 'IamPolicyChangeAlarm', {
      alarmDescription:
        'An IAM policy or role trust was changed outside CloudFormation (see CloudTrail)',
      metric: new Metric({
        namespace: 'Relay/Security',
        metricName: 'IamPolicyChange',
        statistic: 'Sum',
        period: Duration.minutes(5),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // ---- GuardDuty: high-severity findings only ----
    const guardDuty = props.enableGuardDuty ?? isProd;
    if (guardDuty) {
      new CfnDetector(this, 'GuardDuty', {
        enable: true,
        findingPublishingFrequency: 'FIFTEEN_MINUTES',
      });
      new Rule(this, 'GuardDutyHighRule', {
        description: 'GuardDuty findings of severity 7 and above',
        eventPattern: {
          source: ['aws.guardduty'],
          detailType: ['GuardDuty Finding'],
          detail: { severity: [{ numeric: ['>=', 7] }] },
        },
        targets: [
          new SnsTopic(topic, {
            message: RuleTargetInput.fromText(
              `Relay ${stage} GuardDuty finding (severity ${EventField.fromPath('$.detail.severity')}): ` +
                `${EventField.fromPath('$.detail.title')} - ${EventField.fromPath('$.detail.description')}`,
            ),
          }),
        ],
      });
    }

    // ---- Cost ----
    const budgetUsd = props.monthlyBudgetUsd ?? DEFAULT_MONTHLY_BUDGET_USD[stage];
    // No cost filter: a stage filter needs the `stage` cost-allocation tag to be activated in the
    // billing console, and an unmatched filter would silently watch nothing. When dev and prod
    // share an account, each budget sees the account's total.
    new CfnBudget(this, 'MonthlyBudget', {
      budget: {
        budgetName: `relay-${stage}-monthly`,
        budgetType: 'COST',
        timeUnit: 'MONTHLY',
        budgetLimit: { amount: budgetUsd, unit: 'USD' },
      },
      notificationsWithSubscribers: [80, 100].map((threshold) => ({
        notification: {
          notificationType: 'ACTUAL',
          comparisonOperator: 'GREATER_THAN',
          threshold,
          thresholdType: 'PERCENTAGE',
        },
        subscribers: [{ subscriptionType: 'SNS', address: topic.topicArn }],
      })),
    });
    if (isProd) {
      // Account-wide (per AWS service), so created once, on prod.
      const monitor = new CfnAnomalyMonitor(this, 'CostAnomalyMonitor', {
        monitorName: 'relay-prod-services',
        monitorType: 'DIMENSIONAL',
        monitorDimension: 'SERVICE',
      });
      new CfnAnomalySubscription(this, 'CostAnomalySubscription', {
        subscriptionName: 'relay-prod-anomalies',
        frequency: 'IMMEDIATE',
        monitorArnList: [monitor.attrMonitorArn],
        subscribers: [{ type: 'SNS', address: topic.topicArn }],
        thresholdExpression: JSON.stringify({
          Dimensions: {
            Key: 'ANOMALY_TOTAL_IMPACT_ABSOLUTE',
            MatchOptions: ['GREATER_THAN_OR_EQUAL'],
            Values: [String(Math.max(10, Math.round(budgetUsd / 10)))],
          },
        }),
      });
    }

    new CfnOutput(this, 'AlertsTopicArn', { value: topic.topicArn });
    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', stage);
  }
}

/**
 * Makes every CloudWatch alarm in a stack notify the alerts topic, on ALARM and on OK. Alarms
 * created by auto scaling (they live under a step scaling policy and already act on it) are left alone.
 */
export class AlarmActionsAspect implements IAspect {
  private readonly actions = new Map<Stack, SnsAction>();
  constructor(private readonly topicArn: string) {}

  visit(node: IConstruct): void {
    if (!(node instanceof Alarm) || isAutoScalingAlarm(node)) return;
    const stack = Stack.of(node);
    let action = this.actions.get(stack);
    if (!action) {
      action = new SnsAction(Topic.fromTopicArn(stack, 'AlertsTopicRef', this.topicArn));
      this.actions.set(stack, action);
    }
    node.addAlarmAction(action);
    node.addOkAction(action);
  }
}

function isAutoScalingAlarm(alarm: Alarm): boolean {
  return alarm.node.scope instanceof StepScalingPolicy;
}

/** Sets the retention of every CloudWatch log group in a stack. */
export class LogRetentionAspect implements IAspect {
  constructor(private readonly days: number) {}
  visit(node: IConstruct): void {
    if (node instanceof CfnLogGroup) node.retentionInDays = this.days;
  }
}

/** Wires the alerts topic and the log retention into a stack (alarms only where `alarms` is true). */
export function applyMonitoring(
  stack: Stack,
  monitoring: MonitoringStack,
  opts: { alarms: boolean },
): void {
  Aspects.of(stack).add(new LogRetentionAspect(monitoring.logRetentionDays));
  if (opts.alarms) Aspects.of(stack).add(new AlarmActionsAspect(monitoring.topicArn));
}

function child<T>(stack: Stack, id: string, type: abstract new (...a: never[]) => T): T {
  const found = stack.node.tryFindChild(id);
  if (!(found instanceof type)) throw new Error(`${stack.node.id}: expected ${id} (${type.name})`);
  return found;
}

const atLeastOne = (scope: Construct, id: string, description: string, metric: IMetric) =>
  new Alarm(scope, id, {
    alarmDescription: description,
    metric,
    threshold: 1,
    evaluationPeriods: 1,
    comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
    treatMissingData: TreatMissingData.NOT_BREACHING,
  });

/** Counts a log line (`needle`) from each log group as one custom metric and alarms on it. */
export function addLogCountAlarm(
  scope: Construct,
  idPrefix: string,
  opts: {
    logGroups: ILogGroup[];
    needle: string;
    namespace: string;
    metricName: string;
    description: string;
  },
): Alarm {
  opts.logGroups.forEach((logGroup, i) => {
    new MetricFilter(scope, `${idPrefix}Filter${i}`, {
      logGroup,
      metricNamespace: opts.namespace,
      metricName: opts.metricName,
      // Quoted: an exact term match works on the "ts reqid LEVEL {json}" lines Lambda writes.
      filterPattern: FilterPattern.literal(`"${opts.needle}"`),
      metricValue: '1',
      defaultValue: 0,
    });
  });
  return atLeastOne(
    scope,
    `${idPrefix}Alarm`,
    opts.description,
    new Metric({
      namespace: opts.namespace,
      metricName: opts.metricName,
      statistic: 'Sum',
      period: oneMinute,
    }),
  );
}

/** Alarms on resources that belong to other stacks, created in the owning stack's scope (no cross-stack references). */
export function addFanoutAlarms(
  fanout: Stack,
  props: { planQueueArn: string; standardQueueName: string },
): void {
  const planner = child(fanout, 'PlannerFunction', LambdaFunction);
  const shard = child(fanout, 'ShardFunction', LambdaFunction);
  atLeastOne(
    fanout,
    'PlannerErrorsAlarm',
    'The fan-out planner Lambda errored: an alert may not have been planned',
    planner.metricErrors({ period: oneMinute, statistic: 'Sum' }),
  );
  atLeastOne(
    fanout,
    'ShardErrorsAlarm',
    'The shard worker Lambda errored: some areas may not have been sent',
    shard.metricErrors({ period: oneMinute, statistic: 'Sum' }),
  );
  const age = (alarmId: string, queueName: string, description: string) =>
    new Alarm(fanout, alarmId, {
      alarmDescription: description,
      metric: new Metric({
        namespace: 'AWS/SQS',
        metricName: 'ApproximateAgeOfOldestMessage',
        dimensionsMap: { QueueName: queueName },
        statistic: 'Maximum',
        period: oneMinute,
      }),
      threshold: 120,
      evaluationPeriods: 2,
      comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });
  // The queue name is the last ARN segment.
  age(
    'PlanQueueAgeAlarm',
    Fn.select(5, Fn.split(':', props.planQueueArn)),
    'An AlertReady message has waited over 2 minutes in the plan queue: fan-out is stuck or slow',
  );
  age(
    'StandardQueueAgeAlarm',
    props.standardQueueName,
    'The standard send queue has waited over 2 minutes: watch/advisory alerts are late',
  );
  addLogCountAlarm(fanout, 'FanoutSignatureFailed', {
    logGroups: [child(fanout, 'PlannerLogs', LogGroup), child(fanout, 'ShardLogs', LogGroup)],
    needle: 'fanout.signature_failed',
    namespace: 'Relay/Fanout',
    metricName: 'SignatureFailed',
    description: 'Alert signatures fail in fan-out (alert key rotation not loaded, or tampering)',
  });
}

export function addAggregatorAlarms(api: Stack): void {
  const aggregator = child(api, 'AggregatorFunction', LambdaFunction);
  new Alarm(api, 'AggregatorIteratorAgeAlarm', {
    alarmDescription: 'Check-in totals are over 60 seconds behind the check-ins stream',
    // IteratorAge is reported in milliseconds.
    metric: aggregator.metric('IteratorAge', { period: oneMinute, statistic: 'Maximum' }),
    threshold: 60_000,
    evaluationPeriods: 2,
    comparisonOperator: ComparisonOperator.GREATER_THAN_THRESHOLD,
    treatMissingData: TreatMissingData.NOT_BREACHING,
  });
}

export function addDeliveryAlarms(delivery: Stack): void {
  addLogCountAlarm(delivery, 'DeliverySignatureFailed', {
    logGroups: [child(delivery, 'WorkerLogs', LogGroup)],
    needle: 'delivery.signature_failed',
    namespace: 'Relay/Delivery',
    metricName: 'SignatureFailed',
    description:
      'Alert signatures fail in the delivery worker (alert key rotation not loaded, or tampering)',
  });
}
