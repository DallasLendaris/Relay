import { Aspects, Stack, type App } from 'aws-cdk-lib';
import { NIST80053R5Checks, NagSuppressions, type NagPackSuppression } from 'cdk-nag';
import type { IConstruct } from 'constructs';
import { ACCESS_LOGS_BUCKET_ID } from './buckets.js';
import { DB_ROLES } from './grants.js';
import type { buildRelayApp } from './relay-app.js';

export type RelayStacks = ReturnType<typeof buildRelayApp>;
type StackKey = keyof RelayStacks;

/** Rule id prefix of the NIST 800-53 rev 5 pack. */
export const NIST = 'NIST.800.53.R5';
const rule = (name: string): string => `${NIST}-${name}`;

export interface ComplianceOptions {
  stage: 'dev' | 'prod';
  /** Write the per-stack CSV reports to cdk.out (default true; tests turn it off). */
  reports?: boolean;
}

/**
 * Applies the cdk-nag NIST 800-53 rev 5 pack to the whole app and registers the justified
 * exceptions. Unsuppressed findings are error annotations, so `cdk synth` fails on them.
 * bin/relay.ts calls this unless `-c nag=off`; test/nag.test.ts calls it on dev and prod.
 *
 * Every exception is scoped to named resources (never a whole stack) and is listed, with its
 * NIST control and when to revisit it, in docs/compliance/cdk-nag-exceptions.md. A new resource
 * that trips a rule is therefore an error until someone fixes it or adds a reasoned exception.
 */
export function applyCompliance(app: App, stacks: RelayStacks, options: ComplianceOptions): void {
  Aspects.of(app).add(new NIST80053R5Checks({ verbose: true, reports: options.reports ?? true }));
  addExceptions(stacks, options.stage === 'dev');
}

/** Resolves a construct by its path below the stack; throws when it is gone (keeps this file honest). */
function at(stack: Stack, path: string): IConstruct {
  let node: IConstruct = stack;
  for (const id of path.split('/')) {
    const next = node.node.tryFindChild(id);
    if (!next) throw new Error(`cdk-nag exception target ${stack.node.id}/${path} not found`);
    node = next;
  }
  return node;
}

function suppress(stack: Stack, path: string, s: NagPackSuppression[]): void {
  NagSuppressions.addResourceSuppressions(at(stack, path), s, true);
}

// ---- Reasons (each one also in docs/compliance/cdk-nag-exceptions.md) ----

const OUTSIDE_VPC =
  'Deliberate design: the Lambdas run outside a VPC. They reach Aurora through the RDS Data API ' +
  '(HTTPS, IAM-authorized) and every other dependency through public AWS endpoints, so a VPC would ' +
  'only add NAT gateway cost (about $33/month per AZ plus per-GB processing) and cold-start latency ' +
  'on the alert path without removing any network path an attacker could use.';

const INLINE_POLICY =
  'CDK attaches each grant as an AWS::IAM::Policy owned by exactly one role (the role default ' +
  'policy). It is not shared and is reviewed in code (grants.ts, wiring.test.ts); a customer ' +
  'managed policy would add no isolation and only more resources to keep in sync.';

const DLQ_SQS_SOURCE =
  'Invoked synchronously by an SQS event source mapping. A Lambda DLQ only applies to ' +
  'asynchronous invokes; failed messages go to the source queue redrive DLQ, which is alarmed.';
const DLQ: Record<string, string> = {
  ApiFunction:
    'Invoked synchronously by API Gateway; the caller receives the error. A Lambda DLQ only ' +
    'applies to asynchronous invokes.',
  AggregatorFunction:
    'Invoked by a DynamoDB stream event source mapping, whose on-failure destination ' +
    '(AggregatorDlq, alarmed) keeps failed batches. A Lambda DLQ only applies to asynchronous invokes.',
  ShardFunction:
    'Asynchronous invokes use a Lambda on-failure destination (ShardDlq, alarmed) instead of the ' +
    'legacy DeadLetterConfig; destinations carry the full request and response. cdk-nag only ' +
    'recognizes DeadLetterConfig.',
  OutboxDrainerFunction:
    'Scheduled every minute (5 on dev) by EventBridge with no payload; Lambda async retries are ' +
    'off (retryAttempts 0, events older than one period dropped). A failed run is redone by the ' +
    'next run from the Aurora outbox, and DrainerNotRunningAlarm fires when runs stop, so a DLQ of ' +
    'empty schedule events would hold nothing actionable.',
  IngestFunction:
    'Scheduled every minute by EventBridge with retries off on purpose, both on the rule target ' +
    'and in Lambda (retryAttempts 0, events older than 60 s dropped): ingest is idempotent and the ' +
    'next run catches up. Failures raise IngestErrorsAlarm and the feed-stale alarm; a DLQ of ' +
    'empty schedule events would hold nothing actionable.',
  RedriveFunction:
    'Operator tool, invoked synchronously by a person (aws lambda invoke); the caller sees the error.',
  SendRedriveFunction:
    'Operator tool, invoked synchronously by a person (aws lambda invoke); the caller sees the error.',
};
for (const id of [
  'IndexRepairFunction',
  'WorkerFunction',
  'PlannerFunction',
  'PausedConsumerFunction',
  'LateJoinFunction',
]) {
  DLQ[id] = DLQ_SQS_SOURCE;
}

const LAMBDAS: Partial<Record<StackKey, string[]>> = {
  api: ['ApiFunction', 'IndexRepairFunction', 'AggregatorFunction'],
  delivery: ['WorkerFunction'],
  fanout: [
    'ShardFunction',
    'PlannerFunction',
    'PausedConsumerFunction',
    'OutboxDrainerFunction',
    'RedriveFunction',
    'SendRedriveFunction',
  ],
  ingest: ['IngestFunction'],
  latejoin: ['LateJoinFunction'],
};

const CONCURRENCY_OPS =
  'Low-volume function bounded by its trigger: an SQS event source with a maxConcurrency cap ' +
  '(late-join 20, paused consumer 10), a schedule whose run fits its period (outbox drainer), or a ' +
  'person (redrive tools). Reserving concurrency would take capacity from the account quota that ' +
  'capacity.ts divides between the send path and the API (docs/scale-quotas.md); the send path ' +
  'functions all have reservations on prod.';
const CONCURRENCY_DEV =
  'Dev only: dev deliberately reserves no concurrency because a new account starts with a Lambda ' +
  'quota of 10, which cannot hold reservations. Prod reserves concurrency for this function ' +
  '(capacity.ts planConcurrency).';
/** Functions with no reserved concurrency on any stage. */
const UNRESERVED: Partial<Record<StackKey, string[]>> = {
  fanout: [
    'PausedConsumerFunction',
    'OutboxDrainerFunction',
    'RedriveFunction',
    'SendRedriveFunction',
  ],
  latejoin: ['LateJoinFunction'],
};
/** Functions that prod reserves but dev does not. */
const UNRESERVED_DEV: Partial<Record<StackKey, string[]>> = {
  api: ['ApiFunction', 'IndexRepairFunction', 'AggregatorFunction'],
};

const BACKUP_PLAN =
  'Recovery uses the native mechanisms instead of an AWS Backup plan: DynamoDB point-in-time ' +
  'recovery (35 days, prod) and Aurora automated backups (14 days prod, 7 dev). An AWS Backup plan ' +
  '(vault, retention, cross-region copy) is a records-retention decision for the State that has ' +
  'not been made (docs/compliance/business-impact-and-contingency.md).';
const PITR_DEV =
  'Dev only: dev tables hold disposable test data and are destroyed with the stack. Prod has ' +
  'point-in-time recovery on every table (tests assert it).';
const TABLES: Partial<Record<StackKey, string[]>> = {
  api: ['CheckinsTable', 'CheckinTotalsTable'],
  delivery: ['IdempotencyTable'],
  fanout: ['CellIndexTable'],
  ingest: ['StateTable'],
};

const REPLICATION =
  'No cross-region replication yet. The objects are reproducible (raw feed copies, generated zone ' +
  'packs and public alert documents are re-published by ingest) or audit logs already copied to ' +
  'CloudWatch Logs. Replication doubles storage and needs a second-region bucket and key; decide ' +
  'it with the RPO in the BIA.';
const LOG_BUCKET_SSE_S3 =
  'S3 server access log delivery requires the destination bucket to use SSE-S3 (SSE-KMS default ' +
  'encryption is not supported for access log targets). Objects are still encrypted at rest (AES-256).';

const PUBLIC_SUBNETS =
  'Deliberate design: sender tasks run in public subnets with a public IP and no NAT gateway, ' +
  'because a statewide alert pushes gigabytes to APNs/FCM and NAT processing would cost about ' +
  '$0.045/GB plus $33/month per AZ. The task security group has no ingress at all and egress only ' +
  'to TCP 443, so nothing can connect in; flow logs record every flow.';

const DELETION_PROTECTION_DEV =
  'Dev only: the dev cluster is disposable and is torn down with the stack. Prod has deletion ' +
  'protection on and a final snapshot on removal (data-stack.test.ts).';

const ROTATION_DB =
  'Not rotated by Secrets Manager: the hosted rotation Lambda connects to Postgres directly, so it ' +
  'must run inside the data VPC with a Secrets Manager interface endpoint (no NAT exists), about ' +
  '$15/month for two AZs plus a VPC Lambda to patch. Relay deliberately runs no compute in that ' +
  'VPC (Data API only). Rotated manually every 90 days: docs/runbooks/key-rotation.md.';
const ROTATION_KEYRING =
  'Keyring secret ({current, keys}) whose rotation adds a key id while older ids keep verifying ' +
  'outstanding tokens; Secrets Manager single-value rotation cannot express that. Rotated with ' +
  '`npm run seed-keys -- <stage> --rotate` yearly or on suspicion: docs/runbooks/key-rotation.md.';
const ROTATION_EXTERNAL =
  'Credential issued by an outside provider (Apple, Google or Expo); AWS cannot mint a new one. ' +
  'Rotated by hand on a fixed cadence: docs/runbooks/push-credential-rotation.md.';
const ROTATION_ORIGIN =
  'CloudFront holds a copy of this value from the last edge deploy, so rotating it in Secrets ' +
  'Manager alone would make every API request fail with 403 until the edge stack is redeployed. ' +
  'Rotated as a planned two-step change every 90 days: docs/runbooks/key-rotation.md.';

function addExceptions(stacks: RelayStacks, isDev: boolean): void {
  for (const [key, ids] of Object.entries(LAMBDAS) as [StackKey, string[]][]) {
    for (const id of ids) {
      const reason = DLQ[id];
      if (!reason) throw new Error(`no LambdaDLQ reason for ${id}`);
      suppress(stacks[key], id, [
        { id: rule('LambdaInsideVPC'), reason: OUTSIDE_VPC },
        { id: rule('LambdaDLQ'), reason },
        { id: rule('IAMNoInlinePolicy'), reason: INLINE_POLICY },
      ]);
    }
  }
  for (const [key, ids] of Object.entries(UNRESERVED) as [StackKey, string[]][]) {
    for (const id of ids) {
      suppress(stacks[key], id, [{ id: rule('LambdaConcurrency'), reason: CONCURRENCY_OPS }]);
    }
  }
  if (isDev) {
    for (const [key, ids] of Object.entries(UNRESERVED_DEV) as [StackKey, string[]][]) {
      for (const id of ids) {
        suppress(stacks[key], id, [{ id: rule('LambdaConcurrency'), reason: CONCURRENCY_DEV }]);
      }
    }
  }

  // Role default policies outside Lambda functions.
  for (const [key, path] of [
    ['sender', 'TaskDef/TaskRole'],
    ['sender', 'TaskDef/ExecutionRole'],
    ['sender', 'Vpc/FlowLogs/IAMRole'],
    ['data', 'Vpc/FlowLogs/IAMRole'],
    ['monitoring', 'Trail/LogsRole'],
  ] as [StackKey, string][]) {
    suppress(stacks[key], path, [{ id: rule('IAMNoInlinePolicy'), reason: INLINE_POLICY }]);
  }

  for (const [key, ids] of Object.entries(TABLES) as [StackKey, string[]][]) {
    for (const id of ids) {
      suppress(stacks[key], id, [
        { id: rule('DynamoDBInBackupPlan'), reason: BACKUP_PLAN },
        ...(isDev ? [{ id: rule('DynamoDBPITREnabled'), reason: PITR_DEV }] : []),
      ]);
    }
  }
  suppress(stacks.data, 'Cluster', [
    { id: rule('RDSInBackupPlan'), reason: BACKUP_PLAN },
    ...(isDev
      ? [{ id: rule('RDSInstanceDeletionProtectionEnabled'), reason: DELETION_PROTECTION_DEV }]
      : []),
  ]);

  for (const [key, id] of [
    ['ingest', 'RawBucket'],
    ['ingest', 'ZonePackBucket'],
    ['edge', 'PublicAlertsBucket'],
    ['monitoring', 'TrailBucket'],
  ] as [StackKey, string][]) {
    suppress(stacks[key], id, [{ id: rule('S3BucketReplicationEnabled'), reason: REPLICATION }]);
  }
  for (const key of ['ingest', 'edge', 'monitoring'] as StackKey[]) {
    suppress(stacks[key], ACCESS_LOGS_BUCKET_ID, [
      { id: rule('S3BucketReplicationEnabled'), reason: REPLICATION },
      { id: rule('S3DefaultEncryptionKMS'), reason: LOG_BUCKET_SSE_S3 },
    ]);
  }

  suppress(stacks.sender, 'Vpc', [
    { id: rule('VPCSubnetAutoAssignPublicIpDisabled'), reason: PUBLIC_SUBNETS },
    { id: rule('VPCNoUnrestrictedRouteToIGW'), reason: PUBLIC_SUBNETS },
  ]);

  const rotation = (reason: string) => [{ id: rule('SecretsManagerRotationEnabled'), reason }];
  suppress(stacks.data, 'Cluster/Secret', rotation(ROTATION_DB));
  for (const role of Object.keys(DB_ROLES)) {
    suppress(stacks.data, `RoleSecret-${role}`, rotation(ROTATION_DB));
  }
  suppress(stacks.config, 'TokenKeys', rotation(ROTATION_KEYRING));
  suppress(stacks.config, 'DeviceCredentialKeys', rotation(ROTATION_KEYRING));
  suppress(stacks.config, 'ExpoAccessToken', rotation(ROTATION_EXTERNAL));
  suppress(stacks.config, 'OriginVerify', rotation(ROTATION_ORIGIN));
  suppress(stacks.sender, 'ApnsKey', rotation(ROTATION_EXTERNAL));
  suppress(stacks.sender, 'FcmServiceAccount', rotation(ROTATION_EXTERNAL));
}
