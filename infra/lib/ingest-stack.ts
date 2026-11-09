import { fileURLToPath } from 'node:url';
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';
import { Rule, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { BlockPublicAccess, Bucket, BucketEncryption, StorageClass } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';
import { allow, dbEnv, grantDb, grantSecretRead, type DbRefs } from './grants.js';

export interface IngestStackProps extends StackProps {
  stage: 'dev' | 'prod';
  db: DbRefs;
  deliveryQueueArn: string;
  deliveryQueueUrl: string;
  idempotencyTableName: string;
  idempotencyTableArn: string;
  tokenKeysSecretArn: string;
  alertKeysSecretArn: string;
  killSwitchParamName: string;
  killSwitchParamArn: string;
}

export class IngestStack extends Stack {
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

    // Dedupe state only: what ingest has already seen. The full alert record
    // goes to Aurora later (ticket S-07).
    const stateTable = new Table(this, 'StateTable', {
      tableName: 'alerts-state',
      partitionKey: { name: 'sourceId', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: isProd },
      // Rows disappear on their own once the alert has expired.
      timeToLiveAttribute: 'expiresAt',
      removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    });

    const ingestFn = new NodejsFunction(this, 'IngestFunction', {
      entry: fileURLToPath(new URL('../../backend/src/ingest/handler.ts', import.meta.url)),
      // npm workspaces keep one lockfile at the repo root.
      depsLockFilePath: fileURLToPath(new URL('../../package-lock.json', import.meta.url)),
      // The entry lives outside infra/, so the bundling root must be the repo root.
      projectRoot: fileURLToPath(new URL('../../', import.meta.url)),
      handler: 'handler',
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      timeout: Duration.seconds(70), // 5 polls x 12 s plus headroom
      memorySize: 256,
      reservedConcurrentExecutions: 1, // never two overlapping pollers
      logGroup: new LogGroup(this, 'IngestLogs', {
        retention: RetentionDays.ONE_MONTH,
        removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      }),
      environment: {
        RAW_BUCKET: rawBucket.bucketName,
        STATE_TABLE: stateTable.tableName,
        NWS_USER_AGENT: 'Relay capstone (contact via GitHub DallasLendaris/Relay)',
        POLL_INTERVAL_SECONDS: '12',
        POLLS_PER_INVOCATION: '5',
        ...dbEnv(props.db),
        DELIVERY_QUEUE_URL: props.deliveryQueueUrl,
        IDEMPOTENCY_TABLE: props.idempotencyTableName,
        TOKEN_KEYS_SECRET_ARN: props.tokenKeysSecretArn,
        ALERT_KEYS_SECRET_ARN: props.alertKeysSecretArn,
        KILL_SWITCH_PARAM: props.killSwitchParamName,
      },
    });

    // Least privilege: write-only on the bucket, read/write on the table.
    rawBucket.grantWrite(ingestFn);
    stateTable.grantReadWriteData(ingestFn);
    grantDb(ingestFn, props.db);
    allow(ingestFn, ['sqs:SendMessage'], [props.deliveryQueueArn]);
    // Read/write on the idempotency table (it lives in DeliveryStack, so explicit ARN).
    allow(
      ingestFn,
      [
        'dynamodb:GetItem',
        'dynamodb:BatchGetItem',
        'dynamodb:Query',
        'dynamodb:PutItem',
        'dynamodb:UpdateItem',
        'dynamodb:DeleteItem',
        'dynamodb:BatchWriteItem',
      ],
      [props.idempotencyTableArn],
    );
    grantSecretRead(ingestFn, props.tokenKeysSecretArn);
    grantSecretRead(ingestFn, props.alertKeysSecretArn);
    allow(ingestFn, ['ssm:GetParameter'], [props.killSwitchParamArn]);

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

    new CfnOutput(this, 'RawBucketName', { value: rawBucket.bucketName });
    new CfnOutput(this, 'StateTableName', { value: stateTable.tableName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
