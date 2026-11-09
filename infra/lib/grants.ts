import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import type { IRole } from 'aws-cdk-lib/aws-iam';
import type { IFunction } from 'aws-cdk-lib/aws-lambda';

/**
 * Database roles (migration 0011). Each compute principal gets exactly one, through its own
 * Secrets Manager secret. The Postgres role name is the fixed login username of that secret.
 *   api           devices and places read/write, alerts read
 *   ingest        alerts and their source-id/cell tables read/write; no devices or places
 *   ingestInline  ingest plus read on devices and places (dev FANOUT_MODE=inline plans in-process)
 *   fanout        alerts read (planner and shard workers)
 *   sender        alerts read, devices read and push-target clearing, places read (sender, legacy worker)
 *   ops           devices and places read, cell-index outbox (index repair, drainer, backfill)
 *   latejoin      read-only: devices, places, alerts and their cells (the late-join Lambda)
 */
export const DB_ROLES = {
  api: 'relay_api',
  ingest: 'relay_ingest',
  ingestInline: 'relay_ingest_inline',
  fanout: 'relay_fanout',
  sender: 'relay_sender',
  ops: 'relay_ops',
  latejoin: 'relay_latejoin',
} as const;
export type DbRole = keyof typeof DB_ROLES;

/** Secrets Manager name of a role's credentials: relay/<stage>/db/<postgres role>. */
export function dbRoleSecretName(stage: string, role: DbRole): string {
  return `relay/${stage}/db/${DB_ROLES[role]}`;
}

/**
 * What a Lambda needs to reach Aurora through the RDS Data API. There is deliberately no
 * admin secret here: the master secret never leaves the data stack's operator path.
 */
export interface DbRefs {
  clusterArn: string;
  databaseName: string;
  /** Credentials secret ARN of each database role. */
  roleSecretArns: Record<DbRole, string>;
}

/** Environment variables shared by every function that talks to the database. */
export function dbEnv(db: DbRefs, role: DbRole) {
  return {
    DB_CLUSTER_ARN: db.clusterArn,
    DB_SECRET_ARN: db.roleSecretArns[role],
    DB_NAME: db.databaseName,
  };
}

/**
 * Grants use explicit ARNs (plain strings passed between stacks) instead of
 * construct grants, so no stack needs a reference back to another one's resources.
 */
export function allow(fn: IFunction, actions: string[], resources: string[]): void {
  fn.addToRolePolicy(new PolicyStatement({ effect: Effect.ALLOW, actions, resources }));
}

/** Statements giving a principal the Data API on the cluster and ITS OWN role secret only. */
export function dbStatements(db: DbRefs, role: DbRole): PolicyStatement[] {
  return [
    new PolicyStatement({
      effect: Effect.ALLOW,
      actions: [
        'rds-data:ExecuteStatement',
        'rds-data:BeginTransaction',
        'rds-data:CommitTransaction',
        'rds-data:RollbackTransaction',
      ],
      resources: [db.clusterArn],
    }),
    new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['secretsmanager:GetSecretValue'],
      resources: [db.roleSecretArns[role]],
    }),
  ];
}

export function grantDb(fn: IFunction, db: DbRefs, role: DbRole): void {
  for (const s of dbStatements(db, role)) fn.addToRolePolicy(s);
}

/** Same grant for a role that is not a Lambda's (the sender's ECS task role). */
export function grantDbToRole(principal: IRole, db: DbRefs, role: DbRole): void {
  for (const s of dbStatements(db, role)) principal.addToPrincipalPolicy(s);
}

export function grantSecretRead(fn: IFunction, secretArn: string): void {
  allow(fn, ['secretsmanager:GetSecretValue'], [secretArn]);
}
