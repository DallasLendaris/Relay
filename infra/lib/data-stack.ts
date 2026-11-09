import { CfnOutput, RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
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
import { DB_ROLES, dbRoleSecretName, type DbRole } from './grants.js';

export interface DataStackProps extends StackProps {
  stage: 'dev' | 'prod';
  /** Aurora Serverless v2 maximum ACU. Default: prod 16, dev 2. */
  maxAcu?: number;
}

export const DEFAULT_MAX_ACU = { dev: 2, prod: 16 } as const;

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
      serverlessV2MinCapacity: 0,
      serverlessV2MaxCapacity: props.maxAcu ?? DEFAULT_MAX_ACU[props.stage],
      enableDataApi: true,
      storageEncrypted: true,
      defaultDatabaseName: this.databaseName,
      // CDK generates the credentials and keeps them in Secrets Manager.
      credentials: Credentials.fromGeneratedSecret('relay_admin'),
      // Prod keeps a final snapshot on removal; dev is disposable.
      removalPolicy: isProd ? RemovalPolicy.SNAPSHOT : RemovalPolicy.DESTROY,
      deletionProtection: isProd,
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
