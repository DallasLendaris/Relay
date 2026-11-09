/**
 * Operator script: turns the NOLOGIN database roles of migration 0011 into working logins.
 *
 *   npx tsx scripts/db-roles.ts <stage> --cluster-arn <ARN> --admin-secret-arn <ARN> [--database relay]
 *
 * ClusterArn and SecretArn are outputs of the `data` stack (SecretArn is the admin secret). Run it
 * with operator credentials, after `cdk deploy` and after the migrations, in that order. It:
 *   1. re-applies migrations/0011_roles.sql as the admin (idempotent), so tables added by later
 *      migrations get their grants;
 *   2. for each role, reads relay/<stage>/db/<role> from Secrets Manager and runs
 *      ALTER ROLE <role> LOGIN PASSWORD '<password>'.
 * To rotate a password, put a new value in the role's secret (or let Secrets Manager generate one)
 * and run the script again. Passwords are never printed or logged.
 *
 * The password is never concatenated into SQL by hand: the statement text is built by Postgres's
 * own format('%I', '%L'), with the role name and password bound as parameters, and then executed.
 * (ALTER ROLE is a utility statement; it cannot take bind parameters itself.)
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  ExecuteStatementCommand,
  RDSDataClient,
  type SqlParameter,
} from '@aws-sdk/client-rds-data';
import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { DB_ROLES, dbRoleSecretName, type DbRole } from '../lib/grants.js';

/** The little the script needs from a database connection: one statement, positional $n binds. */
export interface AdminDb {
  query(sql: string, params: string[]): Promise<Array<Record<string, unknown>>>;
}

export interface RoleLogin {
  username: string;
  password: string;
}

export interface SyncOptions {
  db: AdminDb;
  stage: string;
  /** Contents of migrations/0011_roles.sql. Omit to skip step 1. */
  grantsSql?: string;
  /** Returns the parsed secret of a role. */
  readSecret: (secretName: string) => Promise<RoleLogin>;
  log?: (msg: string) => void;
}

/**
 * Split a SQL file into statements, keeping $$ ... $$ bodies whole. Same rules as
 * splitStatements in backend/src/db/sql.ts (infra cannot import from backend: separate rootDir).
 */
export function splitSql(sql: string): string[] {
  const text = sql
    .split('\n')
    .filter((line) => !line.trim().startsWith('--'))
    .join('\n');
  const out: string[] = [];
  let start = 0;
  let tag: string | undefined;
  for (let i = 0; i < text.length; i++) {
    if (tag === undefined) {
      if (text[i] === '$') {
        const m = /^\$(?:[A-Za-z_]\w*)?\$/.exec(text.slice(i));
        if (m) {
          tag = m[0];
          i += tag.length - 1;
        }
      } else if (text[i] === ';') {
        out.push(text.slice(start, i));
        start = i + 1;
      }
    } else if (text.startsWith(tag, i)) {
      i += tag.length - 1;
      tag = undefined;
    }
  }
  out.push(text.slice(start));
  return out.map((s) => s.trim()).filter(Boolean);
}

/** Grants first, then one ALTER ROLE per role. Returns the Postgres role names it enabled. */
export async function syncDbRoles(opts: SyncOptions): Promise<string[]> {
  const log = opts.log ?? (() => undefined);
  if (opts.grantsSql) {
    for (const statement of splitSql(opts.grantsSql)) await opts.db.query(statement, []);
    log('grants re-applied');
  }
  const enabled: string[] = [];
  for (const role of Object.keys(DB_ROLES) as DbRole[]) {
    const pgRole = DB_ROLES[role];
    const secretName = dbRoleSecretName(opts.stage, role);
    const login = await opts.readSecret(secretName);
    // The secret must be for this role: a swapped secret would hand one role another's password.
    if (login.username !== pgRole) {
      throw new Error(`${secretName}: username is not ${pgRole}`);
    }
    if (typeof login.password !== 'string' || login.password.length < 16) {
      throw new Error(`${secretName}: password is missing or too short`);
    }
    const rows = await opts.db.query(
      "SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', $1::text, $2::text) AS stmt",
      [pgRole, login.password],
    );
    const stmt = rows[0]?.stmt;
    if (typeof stmt !== 'string') throw new Error('format() returned no statement');
    try {
      await opts.db.query(stmt, []);
    } catch {
      // Not the original error: a database error can quote the statement, which holds the password.
      throw new Error(`${pgRole}: ALTER ROLE failed`);
    }
    log(`${pgRole}: login enabled`);
    enabled.push(pgRole);
  }
  return enabled;
}

/** RDS Data API as the admin. $n binds become :pN only when there are binds. */
export class DataApiAdminDb implements AdminDb {
  constructor(
    private readonly client: RDSDataClient,
    private readonly clusterArn: string,
    private readonly secretArn: string,
    private readonly database: string,
  ) {}

  async query(sql: string, params: string[]): Promise<Array<Record<string, unknown>>> {
    const parameters: SqlParameter[] = params.map((value, i) => ({
      name: `p${i + 1}`,
      value: { stringValue: value },
    }));
    const out = await this.client.send(
      new ExecuteStatementCommand({
        resourceArn: this.clusterArn,
        secretArn: this.secretArn,
        database: this.database,
        // Statements without binds go through untouched: a "$1" inside a password literal is data.
        sql: params.length > 0 ? sql.replace(/\$(\d+)/g, ':p$1') : sql,
        parameters,
        formatRecordsAs: 'JSON',
      }),
    );
    return out.formattedRecords
      ? (JSON.parse(out.formattedRecords) as Array<Record<string, unknown>>)
      : [];
  }
}

function flag(args: string[], name: string, env?: string): string | undefined {
  const i = args.indexOf(name);
  return (i >= 0 ? args[i + 1] : undefined) ?? (env ? process.env[env] : undefined);
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const stage = args[0];
  const clusterArn = flag(args, '--cluster-arn', 'DB_CLUSTER_ARN');
  const adminSecretArn = flag(args, '--admin-secret-arn', 'DB_ADMIN_SECRET_ARN');
  const database = flag(args, '--database') ?? 'relay';
  if ((stage !== 'dev' && stage !== 'prod') || !clusterArn || !adminSecretArn) {
    console.error(
      'usage: tsx scripts/db-roles.ts <dev|prod> --cluster-arn <ARN> --admin-secret-arn <ARN> [--database relay]',
    );
    process.exit(2);
  }
  const region = 'us-west-2';
  const secrets = new SecretsManagerClient({ region });
  const grantsSql = readFileSync(
    fileURLToPath(new URL('../../backend/migrations/0011_roles.sql', import.meta.url)),
    'utf8',
  );
  const enabled = await syncDbRoles({
    db: new DataApiAdminDb(new RDSDataClient({ region }), clusterArn, adminSecretArn, database),
    stage,
    grantsSql,
    readSecret: async (name) => {
      const out = await secrets.send(new GetSecretValueCommand({ SecretId: name }));
      return JSON.parse(out.SecretString ?? '{}') as RoleLogin;
    },
    log: (msg) => console.log(msg),
  });
  console.log(`${enabled.length} database roles can now log in`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err: unknown) => {
    // The message names secrets and roles, never passwords.
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
}
