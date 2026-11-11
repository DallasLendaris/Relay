import { Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import { Effect, PolicyStatement, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import {
  BlockPublicAccess,
  Bucket,
  BucketEncryption,
  CfnBucket,
  ObjectOwnership,
  type IBucket,
  type LifecycleRule,
} from 'aws-cdk-lib/aws-s3';
import type { Construct } from 'constructs';

/** How long S3 server access logs are kept (NIST AU-11). */
export const ACCESS_LOG_RETENTION_DAYS = 365;
/** How long an overwritten or deleted object version is kept in a versioned data bucket. */
export const NONCURRENT_VERSION_DAYS = 30;

/** Construct id of the per-stack access log bucket (compliance.ts and encryption.ts find it by id). */
export const ACCESS_LOGS_BUCKET_ID = 'AccessLogsBucket';

/** Lifecycle rule that keeps old object versions for NONCURRENT_VERSION_DAYS only. */
export const noncurrentVersionRule: LifecycleRule = {
  id: 'expire-noncurrent-versions',
  noncurrentVersionExpiration: Duration.days(NONCURRENT_VERSION_DAYS),
};

/**
 * S3 server access log bucket for the buckets of one stack (NIST AU-2 / AU-12). S3 delivers
 * access logs only to a bucket with SSE-S3 default encryption (SSE-KMS targets are not
 * supported), so this bucket stays S3-managed; see docs/compliance/cdk-nag-exceptions.md.
 * Delivery is authorized by a bucket policy (no ACLs: object ownership is bucket-owner enforced).
 */
export function accessLogsBucket(scope: Construct, isProd: boolean): Bucket {
  return new Bucket(scope, ACCESS_LOGS_BUCKET_ID, {
    blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
    encryption: BucketEncryption.S3_MANAGED,
    enforceSSL: true,
    versioned: true,
    objectOwnership: ObjectOwnership.BUCKET_OWNER_ENFORCED,
    lifecycleRules: [
      {
        id: 'expire-access-logs',
        expiration: Duration.days(ACCESS_LOG_RETENTION_DAYS),
        noncurrentVersionExpiration: Duration.days(NONCURRENT_VERSION_DAYS),
      },
    ],
    removalPolicy: isProd ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
    autoDeleteObjects: !isProd,
  });
}

/**
 * Sends `bucket`'s server access logs to `logs` under `prefix`, authorized by a bucket policy
 * statement for the S3 logging service limited to this source bucket and account.
 */
export function logAccessTo(bucket: IBucket, logs: Bucket, prefix: string): void {
  const cfn = bucket.node.defaultChild as CfnBucket;
  cfn.loggingConfiguration = { destinationBucketName: logs.bucketName, logFilePrefix: prefix };
  logs.addToResourcePolicy(
    new PolicyStatement({
      sid: `S3ServerAccessLogs${prefix.replace(/[^A-Za-z0-9]/g, '')}`,
      effect: Effect.ALLOW,
      principals: [new ServicePrincipal('logging.s3.amazonaws.com')],
      actions: ['s3:PutObject'],
      resources: [logs.arnForObjects(`${prefix}*`)],
      conditions: {
        ArnLike: { 'aws:SourceArn': bucket.bucketArn },
        StringEquals: { 'aws:SourceAccount': Stack.of(bucket).account },
      },
    }),
  );
}
