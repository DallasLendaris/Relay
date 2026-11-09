import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Rule, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { BlockPublicAccess, Bucket, BucketEncryption, StorageClass } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { INGEST_RESERVED_CONCURRENCY } from './capacity.js';
import {
  alertSignEnv,
  allow,
  dbEnv,
  grantAlertSign,
  grantDb,
  grantSecretRead,
  type AlertSigningRefs,
  type DbRefs,
} from './grants.js';

export interface IngestStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  deliveryQueueArn: string;
  deliveryQueueUrl: string;
  tokenKeysSecretArn: string;
  alertSigning: AlertSigningRefs;
  killSwitchParamName: string;
  killSwitchParamArn: string;
  /** Public alert bucket (edge stack, us-east-1), named deterministically so no cross-region reference is needed. */
  publicAlerts: { bucketName: string; region: string };
  /** Plan queue (FanoutStack): ingest sends AlertReady here when FANOUT_MODE is "queue". */
  planQueueArn: string;
  planQueueUrl: string;
  /** "queue" hands fan-out to the planner; "inline" fans out inside ingest. Default: prod queue, dev inline. */
  fanoutMode?: FanoutMode;
  /** NWS User-Agent, including contact info (email or URL). */
  nwsUserAgent?: string;
}

/** Ingest Lambda memory (CPU scales with it: JSON signing, zone geometry and the S3 fan-out). */
export const INGEST_MEMORY_MB = 1024;

/**
 * The poller must never run twice at once: two pollers would double the request rate against
 * NWS (which asks for at most one request per 30 s per URL). One named value so every stack that
 * budgets account concurrency reads the same number.
 */
/** One source of truth with the capacity plan (capacity.ts). */
export const POLLER_RESERVED_CONCURRENCY = INGEST_RESERVED_CONCURRENCY;
/** Seconds between NWS requests (the NWS minimum; the code raises anything lower). */
export const NWS_POLL_INTERVAL_SECONDS = 30;
/** Polls per run: at 0 s and 30 s. A run must end before the next minute's starts. */
export const NWS_POLLS_PER_INVOCATION = 2;
/** Under 60 s so a run can never overlap the next one (which reserved concurrency would drop). */
export const INGEST_TIMEOUT_SECONDS = 58;
/** NWS requires a User-Agent with contact info; override with the nwsUserAgent prop. */
export const DEFAULT_NWS_USER_AGENT = 'Relay capstone (https://github.com/DallasLendaris/Relay)';

export type FanoutMode = 'queue' | 'inline';

/** Where ingest reads the zone pack from (fixed key, see backend/src/geo/s3-zone-provider.ts). */
export const ZONE_PACK_KEY = 'zones/v1/california.json.gz';

export class IngestStack extends Stack {
  /** Private bucket holding the zone pack (zones/v1/*). Exposed for the publish script. */
  readonly zonePackBucket: Bucket;
  readonly zonePackBucketName: string;

  constructor(scope: Construct, id: string, props: IngestStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';

    // Raw NWS responses, kept for audit/replay. Prod data survives a stack teardown.
    const rawBucket = new Bucket(this, 'RawBucket', {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      encryption: BucketEncryption.S3_MANAGED,
      lifecycleRules: [
        {
          transitions: [
            { storageClass: StorageClass.INFREQUENT_ACCESS, transitionAfter: Duration.days(30) },
          ],
        },
      ],
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      autoDeleteObjects: !isProd,
    });

    // Zone pack (NWS zone / county polygons as one gzip), uploaded by the pack publisher and read
    // by ingest. Same region as ingest. Private: only the ingest role reads it.
    this.zonePackBucket = new Bucket(this, 'ZonePackBucket', {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      encryption: BucketEncryption.S3_MANAGED,
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      autoDeleteObjects: !isProd,
    });
    this.zonePackBucketName = this.zonePackBucket.bucketName;

    // Dedupe state only: what ingest has already seen. The full alert record
    // goes to Aurora later (ticket S-07).
    const stateTable = new Table(this, 'StateTable', {
      partitionKey: { name: 'sourceId', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: isProd },
      // Rows disappear on their own once the alert has expired.
      timeToLiveAttribute: 'expiresAt',
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    });

    const fanoutMode = props.fanoutMode ?? (isProd ? 'queue' : 'inline');
    if (fanoutMode !== 'queue' && fanoutMode !== 'inline') {
      throw new Error(`fanoutMode must be "queue" or "inline", got "${String(fanoutMode)}"`);
    }

    // Inline mode plans in-process, so it also reads places and devices; queue mode never does.
    const ingestDbRole = fanoutMode === 'inline' ? 'ingestInline' : 'ingest';
    const ingestFn = new NodejsFunction(this, 'IngestFunction', {
      entry: fileURLToPath(new URL('../../backend/src/ingest/handler.ts', import.meta.url)),
      // npm workspaces keep one lockfile at the repo root.
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      // The entry lives outside infra/, so the bundling root must be the repo root.
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(INGEST_TIMEOUT_SECONDS), // 2 polls, 30 s apart, must end inside the minute
      memorySize: INGEST_MEMORY_MB, // the post-AlertReady CDN publish runs 16 uploads at once
      reservedConcurrentExecutions: POLLER_RESERVED_CONCURRENCY, // never two overlapping pollers; also a floor an API flood cannot take
      logGroup: new LogGroup(this, 'IngestLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      }),
      environment: {
        RAW_BUCKET: rawBucket.bucketName,
        STATE_TABLE: stateTable.tableName,
        NWS_USER_AGENT: props.nwsUserAgent ?? DEFAULT_NWS_USER_AGENT,
        POLL_INTERVAL_SECONDS: String(NWS_POLL_INTERVAL_SECONDS),
        POLLS_PER_INVOCATION: String(NWS_POLLS_PER_INVOCATION),
        ...dbEnv(props.db, ingestDbRole),
        DELIVERY_QUEUE_URL: props.deliveryQueueUrl,
        TOKEN_KEYS_SECRET_ARN: props.tokenKeysSecretArn,
        ...alertSignEnv(props.alertSigning),
        KILL_SWITCH_PARAM: props.killSwitchParamName,
        FANOUT_MODE: fanoutMode,
        PLAN_QUEUE_URL: props.planQueueUrl,
        ZONE_PACK_BUCKET: this.zonePackBucket.bucketName,
        PUBLIC_ALERTS_BUCKET: props.publicAlerts.bucketName,
        PUBLIC_ALERTS_REGION: props.publicAlerts.region,
      },
    });

    // Least privilege: write-only on the bucket, read/write on the table.
    // Explicit actions only (the bucket and table grant helpers expand to wildcards).
    allow(ingestFn, ['s3:PutObject'], [rawBucket.arnForObjects('*')]);
    allow(ingestFn, ['dynamodb:GetItem', 'dynamodb:PutItem'], [stateTable.tableArn]);
    grantDb(ingestFn, props.db, ingestDbRole);
    allow(ingestFn, ['sqs:SendMessage'], [props.deliveryQueueArn, props.planQueueArn]);
    allow(ingestFn, ['s3:GetObject'], [this.zonePackBucket.arnForObjects('zones/v1/*')]);
    grantSecretRead(ingestFn, props.tokenKeysSecretArn);
    grantAlertSign(ingestFn, props.alertSigning);
    allow(ingestFn, ['ssm:GetParameter'], [props.killSwitchParamArn]);
    allow(
      ingestFn,
      ['s3:PutObject'],
      [`arn:aws:s3:::${props.publicAlerts.bucketName}/alerts/v1/*`],
    );

    // No retries: the next minute's run catches up on anything missed.
    new Rule(this, 'EveryMinute', {
      schedule: Schedule.rate(Duration.minutes(1)),
      targets: [new LambdaFunction(ingestFn, { retryAttempts: 0 })],
    });

    new Alarm(this, 'IngestErrorsAlarm', {
      metric: ingestFn.metricErrors({ period: Duration.minutes(5), statistic: 'Sum' }),
      threshold: 3,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // The Lambda catches per-feature errors, so its own Errors metric stays 0 during an NWS,
    // DB or SQS outage. The handler emits these custom metrics (EMF) once per invocation.
    const custom = (metricName: string) =>
      new Metric({
        namespace: 'Relay/Ingest',
        metricName,
        statistic: 'Sum',
        period: Duration.minutes(5),
      });

    new Alarm(this, 'IngestFeatureErrorsAlarm', {
      alarmDescription: 'Ingest hit >= 3 errors in 5 minutes (NWS, DB, SQS or geo failures).',
      metric: custom('Errors'),
      threshold: 3,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // FetchFailed is emitted every invocation (0 when fine), so a missing metric means the
    // Lambda is not running or not reporting: treat that as breaching.
    new Alarm(this, 'IngestNoSuccessfulFetchAlarm', {
      alarmDescription: 'NWS fetch failed >= 5 times in 5 minutes, or ingest is not reporting.',
      metric: custom('FetchFailed'),
      threshold: 5,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.BREACHING,
    });

    // A quarantined alert is marked seen and never delivered, so a person must look at it.
    new Alarm(this, 'IngestQuarantinedAlarm', {
      alarmDescription:
        'An alert was quarantined (permanent data error) and will not be delivered.',
      metric: custom('Quarantined'),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    // Some UGC zones of an alert could not be resolved: the alert went out for the known part
    // only, so someone should check the zone pack or NWS zone ids.
    new Alarm(this, 'IngestUnknownZonesAlarm', {
      alarmDescription:
        'Alert zones (UGC) could not be resolved: the alert reached only the known part of its area.',
      metric: custom('UnknownZones'),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
    });

    new CfnOutput(this, 'RawBucketName', { value: rawBucket.bucketName });
    new CfnOutput(this, 'ZonePackBucketName', { value: this.zonePackBucketName });
    new CfnOutput(this, 'StateTableName', { value: stateTable.tableName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
