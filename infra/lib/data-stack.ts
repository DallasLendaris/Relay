import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import { SubnetType, Vpc } from 'aws-cdk-lib/aws-ec2';
import {
  AuroraPostgresEngineVersion,
  ClusterInstance,
  Credentials,
  DatabaseCluster,
  DatabaseClusterEngine,
  DatabaseSecret,
} from 'aws-cdk-lib/aws-rds';
import type { ISecret } from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import { DB_ROLES, dbAdminSecretName, dbRoleSecretName, type DbRole } from './grants.js';

export interface DataStackProps extends StackProps {
  stage: 'dev' | 'prod';
  /** Aurora Serverless v2 maximum ACU. Default: prod 16, dev 2. */
  maxAcu?: number;
}

/** Automated backup (point-in-time recovery window) in days. */
export const BACKUP_RETENTION_DAYS = { dev: 7, prod: 14 } as const;

export const DEFAULT_MAX_ACU = { dev: 2, prod: 16 } as const;

/**
 * Minimum ACU. Prod keeps 0.5 (PERF-5 / OPS-10 / REL-9): at 0 the cluster pauses when idle and the
 * first Data API call after a pause waits 15-30 s for the resume, longer than the DB client's
 * ~17 s retry budget, so the first request after an idle spell could fail, and that first request
 * may be an alert. 0.5 ACU costs about $44 a month (0.5 x $0.12 per ACU-hour x 730 h, us-west-2).
 * The cell-index outbox drainer (every minute on prod, see FanoutStack) already touches the
 * cluster each minute, so on prod it would rarely pause anyway; 0.5 makes that a guarantee instead
 * of a side effect. Dev stays at 0 and auto-pauses; its drainer runs every 5 minutes.
 */
export const MIN_ACU = { dev: 0, prod: 0.5 } as const;

/**
 * Aurora Serverless v2 PostgreSQL, reached only through the RDS Data API (HTTPS).
 * Lambdas therefore do not need a VPC, which avoids NAT gateway costs. The cluster
 * sits in a VPC with isolated subnets only, so it has no route to the internet.
 */
export class DataStack extends Stack {
  readonly clusterArn: string;
  /**
   * The master (relay_admin) secret. Only for migrations and scripts/db-roles.ts, run by an
   * operator. It is exposed as an output, never wired into any compute principal.
   */
  readonly secretArn: string;
  /** One credentials secret per database role (migration 0011 creates the Postgres roles). */
  readonly roleSecretArns: Record<DbRole, string>;
  readonly databaseName = 'relay';

  constructor(scope: Construct, id: string, props: DataStackProps) {
    super(scope, id, props);
    const isProd = props.stage === 'prod';

    const vpc = new Vpc(this, 'Vpc', {
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [{ name: 'isolated', subnetType: SubnetType.PRIVATE_ISOLATED }],
    });

    const cluster = new DatabaseCluster(this, 'Cluster', {
      engine: DatabaseClusterEngine.auroraPostgres({
        // 16.8+ supports scaling to 0 ACU (auto-pause).
        version: AuroraPostgresEngineVersion.VER_16_8,
      }),
      vpc,
      vpcSubnets: { subnetType: SubnetType.PRIVATE_ISOLATED },
      writer: ClusterInstance.serverlessV2('writer'),
      serverlessV2MinCapacity: MIN_ACU[props.stage],
      serverlessV2MaxCapacity: props.maxAcu ?? DEFAULT_MAX_ACU[props.stage],
      enableDataApi: true,
      storageEncrypted: true,
      defaultDatabaseName: this.databaseName,
      // CDK generates the credentials and keeps them in Secrets Manager.
      credentials: Credentials.fromGeneratedSecret('relay_admin', {
        secretName: dbAdminSecretName(props.stage),
      }),
      // Prod keeps a final snapshot on removal; dev is disposable.
      removalPolicy: isProd ? RemovalPolicy.SNAPSHOT : RemovalPolicy.DESTROY,
      deletionProtection: isProd,
      // The default is 1 day: a bad migration or a bad delete noticed on day 2 would be
      // unrecoverable. Tags are copied so snapshots stay attributable (cost, ownership).
      backup: { retention: Duration.days(BACKUP_RETENTION_DAYS[props.stage]) },
      copyTagsToSnapshot: true,
    });

    // fromGeneratedSecret always creates a secret; the guard satisfies the type.
    const secret: ISecret | undefined = cluster.secret;
    if (!secret) throw new Error('Cluster secret missing');

    this.clusterArn = cluster.clusterArn;
    this.secretArn = secret.secretArn;

    // One login secret per Postgres role: fixed username, generated password, attached to the
    // cluster so the Data API accepts it. The password only takes effect once
    // scripts/db-roles.ts has run (ALTER ROLE ... LOGIN PASSWORD); until then the role cannot log in.
    const roleSecretArns = {} as Record<DbRole, string>;
    for (const role of Object.keys(DB_ROLES) as DbRole[]) {
      const roleSecret = new DatabaseSecret(this, `RoleSecret-${role}`, {
        username: DB_ROLES[role],
        secretName: dbRoleSecretName(props.stage, role),
        dbname: this.databaseName,
      }).attach(cluster);
      roleSecretArns[role] = roleSecret.secretArn;
    }
    this.roleSecretArns = roleSecretArns;

    new CfnOutput(this, 'ClusterArn', { value: this.clusterArn });
    new CfnOutput(this, 'SecretArn', { value: this.secretArn });
    new CfnOutput(this, 'DatabaseName', { value: this.databaseName });

    Tags.of(this).add('project', 'relay');
    Tags.of(this).add('stage', props.stage);
  }
}
