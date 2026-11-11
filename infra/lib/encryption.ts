import { Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { CfnTrail } from 'aws-cdk-lib/aws-cloudtrail';
import { CfnTable } from 'aws-cdk-lib/aws-dynamodb';
import { ContainerDefinition } from 'aws-cdk-lib/aws-ecs';
import { AnyPrincipal, Effect, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Key, type IKey } from 'aws-cdk-lib/aws-kms';
import { Function as LambdaFunction } from 'aws-cdk-lib/aws-lambda';
import { CfnLogGroup } from 'aws-cdk-lib/aws-logs';
import { CfnDBCluster } from 'aws-cdk-lib/aws-rds';
import { Bucket, CfnBucket } from 'aws-cdk-lib/aws-s3';
import { CfnSecret } from 'aws-cdk-lib/aws-secretsmanager';
import { CfnTopic } from 'aws-cdk-lib/aws-sns';
import { CfnQueue, Queue } from 'aws-cdk-lib/aws-sqs';
import type { Construct, IConstruct } from 'constructs';
import { ACCESS_LOGS_BUCKET_ID } from './buckets.js';

/**
 * Customer-managed KMS keys, grouped by data domain to keep the count (and cost, $1 per key per
 * month plus $1 for each of the first two yearly rotations) small:
 *
 *   us-west-2 (ConfigStack):  data      DynamoDB tables, Aurora storage, S3 data buckets, secrets
 *                             messaging SQS queues, SNS topics
 *                             logs      CloudWatch log groups, CloudTrail, the trail bucket
 *   us-east-1 (EdgeStack):    edge      public-alerts bucket, edge SNS topic, WAF log group
 *
 * All four are symmetric, rotate yearly (KMS automatic rotation) and are retained on prod.
 *
 * Key policies keep the account-root statement (IAM administers the key) and add, like the AWS
 * managed keys they replace, a statement that lets principals of this account use the key only
 * through the services that store the data (kms:ViaService). A principal still needs the
 * service's own permission (for example sqs:SendMessage on that queue) to touch the data, so the
 * per-resource IAM grants in grants.ts remain the access control. AWS services that publish or
 * write on their own (CloudWatch alarms, EventBridge, Budgets, CloudFront, CloudWatch Logs,
 * CloudTrail) get narrow service-principal statements.
 */
export interface RegionalKeys {
  data: IKey;
  messaging: IKey;
  logs: IKey;
}

export const keyAlias = (stage: string, name: string): string => `alias/relay/${stage}/${name}`;

function cmk(scope: Construct, id: string, stage: 'dev' | 'prod', name: string, what: string): Key {
  const isProd = stage === 'prod';
  return new Key(scope, id, {
    alias: keyAlias(stage, name),
    description: `Relay ${stage} ${name} key: ${what}`,
    enableKeyRotation: true,
    rotationPeriod: Duration.days(365),
    removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    pendingWindow: Duration.days(isProd ? 30 : 7),
  });
}

/** Lets this account's principals use `key` only through the named services in this region. */
function allowViaServices(key: Key, services: string[], region: string): void {
  const stack = Stack.of(key);
  key.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowUseThroughServices',
      effect: Effect.ALLOW,
      principals: [new AnyPrincipal()],
      actions: [
        'kms:Encrypt',
        'kms:Decrypt',
        'kms:ReEncrypt*',
        'kms:GenerateDataKey*',
        'kms:DescribeKey',
      ],
      resources: ['*'],
      conditions: {
        StringEquals: {
          'kms:CallerAccount': stack.account,
          'kms:ViaService': services.map((s) => `${s}.${region}.amazonaws.com`),
        },
      },
    }),
  );
  // Services that hold a key for a long-lived resource (DynamoDB, RDS) use grants.
  key.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowServiceGrants',
      effect: Effect.ALLOW,
      principals: [new AnyPrincipal()],
      actions: ['kms:CreateGrant', 'kms:ListGrants', 'kms:RevokeGrant'],
      resources: ['*'],
      conditions: {
        StringEquals: {
          'kms:CallerAccount': stack.account,
          'kms:ViaService': services.map((s) => `${s}.${region}.amazonaws.com`),
        },
        Bool: { 'kms:GrantIsForAWSResource': 'true' },
      },
    }),
  );
}

/** Lets AWS services that publish to an encrypted SNS topic (or queue) on their own use the key. */
function allowPublishers(key: Key, sid: string, principals: string[], account: string): void {
  key.addToResourcePolicy(
    new PolicyStatement({
      sid,
      effect: Effect.ALLOW,
      principals: principals.map((p) => new ServicePrincipal(p)),
      actions: ['kms:Decrypt', 'kms:GenerateDataKey*'],
      resources: ['*'],
      conditions: { StringEquals: { 'aws:SourceAccount': account } },
    }),
  );
}

/** CloudWatch Logs may use the key for this account's log groups in this region only. */
function allowLogs(key: Key, region: string, account: string): void {
  const { partition } = Stack.of(key);
  key.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowCloudWatchLogs',
      effect: Effect.ALLOW,
      principals: [new ServicePrincipal(`logs.${region}.amazonaws.com`)],
      actions: [
        'kms:Encrypt*',
        'kms:Decrypt*',
        'kms:ReEncrypt*',
        'kms:GenerateDataKey*',
        'kms:Describe*',
      ],
      resources: ['*'],
      conditions: {
        ArnLike: {
          'kms:EncryptionContext:aws:logs:arn': `arn:${partition}:logs:${region}:${account}:log-group:*`,
        },
      },
    }),
  );
}

/** The three us-west-2 keys. Created in ConfigStack, which every other stack already follows. */
export function createRegionalKeys(scope: Stack, stage: 'dev' | 'prod'): RegionalKeys {
  const { region, account, partition } = scope;
  const data = cmk(scope, 'DataKey', stage, 'data', 'DynamoDB, Aurora, S3 data buckets, secrets');
  allowViaServices(data, ['dynamodb', 'rds', 's3', 'secretsmanager'], region);

  const messaging = cmk(scope, 'MessagingKey', stage, 'messaging', 'SQS queues and SNS topics');
  allowViaServices(messaging, ['sqs', 'sns'], region);
  // Alarm actions, EventBridge rules and Budgets publish to the alerts topic themselves.
  allowPublishers(
    messaging,
    'AllowAwsPublishers',
    ['cloudwatch.amazonaws.com', 'events.amazonaws.com', 'budgets.amazonaws.com'],
    account,
  );
  // Cost Anomaly Detection publishes without an aws:SourceAccount condition (see its topic policy).
  messaging.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowCostAnomalyDetection',
      effect: Effect.ALLOW,
      principals: [new ServicePrincipal('costalerts.amazonaws.com')],
      actions: ['kms:Decrypt', 'kms:GenerateDataKey*'],
      resources: ['*'],
    }),
  );

  const logs = cmk(scope, 'LogsKey', stage, 'logs', 'CloudWatch Logs, CloudTrail');
  allowLogs(logs, region, account);
  allowViaServices(logs, ['s3'], region);
  logs.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowCloudTrailEncrypt',
      effect: Effect.ALLOW,
      principals: [new ServicePrincipal('cloudtrail.amazonaws.com')],
      actions: ['kms:GenerateDataKey*'],
      resources: ['*'],
      conditions: {
        StringLike: {
          'kms:EncryptionContext:aws:cloudtrail:arn': `arn:${partition}:cloudtrail:*:${account}:trail/*`,
        },
        StringEquals: { 'aws:SourceAccount': account },
      },
    }),
  );
  logs.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowCloudTrailDescribe',
      effect: Effect.ALLOW,
      principals: [new ServicePrincipal('cloudtrail.amazonaws.com')],
      actions: ['kms:DescribeKey'],
      resources: ['*'],
    }),
  );
  return { data, messaging, logs };
}

/**
 * The us-east-1 key of the edge stack. CloudFront reads the public-alerts bucket through Origin
 * Access Control, so the key lets cloudfront.amazonaws.com decrypt for this account's
 * distributions. The condition names the account's distributions (distribution/*) rather than
 * the one distribution: the distribution depends on the bucket, which depends on the key, so
 * the key policy cannot reference the distribution without a cycle (CDK's own OAC helper uses the
 * same wildcard for the same reason).
 */
export function createEdgeKey(scope: Stack, stage: 'dev' | 'prod'): Key {
  const { region, account, partition } = scope;
  const key = cmk(scope, 'EdgeKey', stage, 'edge', 'public-alerts bucket, edge topic, WAF logs');
  // s3: ingest (us-west-2) writes alert documents to the bucket in this region; sns: edge topic.
  allowViaServices(key, ['s3', 'sns'], region);
  allowLogs(key, region, account);
  allowPublishers(key, 'AllowCloudWatchAlarms', ['cloudwatch.amazonaws.com'], account);
  // The edge stack's EventBridge rule (edge key changes) publishes to the edge topic.
  allowPublishers(key, 'AllowEventBridgeRules', ['events.amazonaws.com'], account);
  key.addToResourcePolicy(
    new PolicyStatement({
      sid: 'AllowCloudFrontOacDecrypt',
      effect: Effect.ALLOW,
      principals: [new ServicePrincipal('cloudfront.amazonaws.com')],
      actions: ['kms:Decrypt'],
      resources: ['*'],
      conditions: {
        ArnLike: { 'aws:SourceArn': `arn:${partition}:cloudfront::${account}:distribution/*` },
      },
    }),
  );
  return key;
}

export interface EncryptionKeys {
  /** DynamoDB, Aurora, S3 data buckets, secrets. */
  data?: IKey;
  /** SQS, SNS. */
  messaging?: IKey;
  /** CloudWatch log groups, CloudTrail. */
  logs?: IKey;
}

/**
 * Points every resource of `stack` at its domain key: DynamoDB tables, Aurora clusters, S3
 * buckets (except the access log bucket, which S3 requires to be SSE-S3), secrets, SQS queues,
 * SNS topics, log groups and trails. It walks the finished construct tree once, so a resource
 * added later is covered without anyone remembering to pass a key; encryption.test.ts asserts
 * the result on every resource of those types.
 *
 * It also enforces TLS 1.2+ on every S3 bucket (deny s3:TlsVersion < 1.2) and TLS on every SQS
 * queue (deny aws:SecureTransport false). SNS topics set enforceSSL where they are defined.
 */
export function encryptResources(stack: Stack, keys: EncryptionKeys): void {
  const need = (key: IKey | undefined, what: string, node: IConstruct): IKey => {
    if (!key) throw new Error(`${node.node.path}: no ${what} key for this stack`);
    return key;
  };
  for (const node of stack.node.findAll()) {
    if (node instanceof CfnTable) {
      node.sseSpecification = {
        sseEnabled: true,
        sseType: 'KMS',
        kmsMasterKeyId: need(keys.data, 'data', node).keyArn,
      };
    } else if (node instanceof CfnDBCluster) {
      node.storageEncrypted = true;
      node.kmsKeyId = need(keys.data, 'data', node).keyArn;
    } else if (node instanceof CfnSecret) {
      node.kmsKeyId = need(keys.data, 'data', node).keyArn;
    } else if (node instanceof CfnBucket) {
      if (node.node.scope?.node.id === ACCESS_LOGS_BUCKET_ID) continue;
      // The trail bucket holds logs; everything else is data.
      const isTrail = node.node.scope?.node.id === 'TrailBucket';
      const key = isTrail ? need(keys.logs, 'logs', node) : need(keys.data, 'data', node);
      node.bucketEncryption = {
        serverSideEncryptionConfiguration: [
          {
            serverSideEncryptionByDefault: { sseAlgorithm: 'aws:kms', kmsMasterKeyId: key.keyArn },
            // S3 Bucket Keys cut KMS requests (and their cost) by orders of magnitude.
            bucketKeyEnabled: true,
          },
        ],
      };
    } else if (node instanceof CfnQueue) {
      node.sqsManagedSseEnabled = undefined;
      node.kmsMasterKeyId = need(keys.messaging, 'messaging', node).keyArn;
      // Reuse a data key for 5 minutes (the default, stated): fewer KMS calls at fan-out volume.
      node.kmsDataKeyReusePeriodSeconds = 300;
    } else if (node instanceof CfnTopic) {
      node.kmsMasterKeyId = need(keys.messaging, 'messaging', node).keyArn;
    } else if (node instanceof CfnLogGroup) {
      node.kmsKeyId = need(keys.logs, 'logs', node).keyArn;
    } else if (node instanceof CfnTrail) {
      node.kmsKeyId = need(keys.logs, 'logs', node).keyArn;
    }

    if (node instanceof Bucket) {
      node.addToResourcePolicy(
        new PolicyStatement({
          sid: 'DenyTlsBelow12',
          effect: Effect.DENY,
          principals: [new AnyPrincipal()],
          actions: ['s3:*'],
          resources: [node.bucketArn, node.arnForObjects('*')],
          conditions: { NumericLessThan: { 's3:TlsVersion': '1.2' } },
        }),
      );
    } else if (node instanceof Queue) {
      node.addToResourcePolicy(
        new PolicyStatement({
          sid: 'DenyInsecureTransport',
          effect: Effect.DENY,
          principals: [new AnyPrincipal()],
          actions: ['sqs:*'],
          resources: [node.queueArn],
          conditions: { Bool: { 'aws:SecureTransport': 'false' } },
        }),
      );
    }
  }
}

/** Env var the AWS SDK for JavaScript v3 reads to use FIPS 140 validated endpoints. */
export const FIPS_ENV = 'AWS_USE_FIPS_ENDPOINT';

/**
 * Lambdas whose code calls a service with no FIPS endpoint in us-west-2. The SDK applies
 * AWS_USE_FIPS_ENDPOINT to every client in the process, so these must not get it:
 *  - ApiFunction calls Amazon Location Service (places.geo), which has FIPS endpoints only in
 *    GovCloud (docs/compliance/cdk-nag-exceptions.md, "FIPS endpoints").
 */
export const FIPS_EXCLUDED_FUNCTIONS = new Set(['ApiFunction']);

/**
 * Sets AWS_USE_FIPS_ENDPOINT=true on every Lambda (except FIPS_EXCLUDED_FUNCTIONS) and every
 * Fargate container of the stack. Every other AWS service the backend calls (DynamoDB, RDS Data
 * API, SQS, Secrets Manager, KMS, SSM, Lambda, ECS, S3) has FIPS endpoints in us-west-2 and
 * us-east-1. APNs, FCM and Expo are not AWS endpoints and are not affected.
 */
export function useFipsEndpoints(stack: Stack): void {
  for (const node of stack.node.findAll()) {
    if (node instanceof LambdaFunction && !FIPS_EXCLUDED_FUNCTIONS.has(node.node.id)) {
      node.addEnvironment(FIPS_ENV, 'true');
    } else if (node instanceof ContainerDefinition) {
      node.addEnvironment(FIPS_ENV, 'true');
    }
  }
}
