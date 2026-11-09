import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import type { IFunction } from 'aws-cdk-lib/aws-lambda';

/** What every Lambda needs to reach Aurora through the RDS Data API. */
export interface DbRefs {
  clusterArn: string;
  secretArn: string;
  databaseName: string;
}

/** Environment variables shared by every function that talks to the database. */
export function dbEnv(db: DbRefs) {
  return { DB_CLUSTER_ARN: db.clusterArn, DB_SECRET_ARN: db.secretArn, DB_NAME: db.databaseName };
}

/**
 * Grants use explicit ARNs (plain strings passed between stacks) instead of
 * construct grants, so no stack needs a reference back to another one's resources.
 */
export function allow(fn: IFunction, actions: string[], resources: string[]): void {
  fn.addToRolePolicy(new PolicyStatement({ effect: Effect.ALLOW, actions, resources }));
}

export function grantDb(fn: IFunction, db: DbRefs): void {
  allow(fn, ['rds-data:ExecuteStatement'], [db.clusterArn]);
  allow(fn, ['secretsmanager:GetSecretValue'], [db.secretArn]);
}

export function grantSecretRead(fn: IFunction, secretArn: string): void {
  allow(fn, ['secretsmanager:GetSecretValue'], [secretArn]);
}
