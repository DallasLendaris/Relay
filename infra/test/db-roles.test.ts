import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { PGlite } from '@electric-sql/pglite';
import { Match } from 'aws-cdk-lib/assertions';
import { describe, expect, it } from 'vitest';
import { DB_ROLES, dbRoleSecretName, type DbRole } from '../lib/grants.js';
import { syncDbRoles, splitSql, type AdminDb } from '../scripts/db-roles.js';
import { synthApp } from './helpers.js';

/** Finds a secret's logical id from its (token) ARN as the data stack resolves it. */
function logicalIdOf(app: ReturnType<typeof synthApp>, arn: string): string {
  const resolved = app.stacks.data.resolve(arn) as { Ref?: string };
  expect(resolved.Ref).toBeTruthy();
  return resolved.Ref as string;
}

interface Principal {
  stack: string;
  /** Logical id of the Lambda or task definition. */
  id: string;
  roleId: string;
  /** JSON of the DB_SECRET_ARN environment value. */
  envSecret: string;
}

/** Every Lambda and ECS task that is given a DB_SECRET_ARN, with its IAM role's logical id. */
function dbPrincipals(app: ReturnType<typeof synthApp>): Principal[] {
  const out: Principal[] = [];
  for (const [stack, t] of Object.entries(app.templates)) {
    for (const [id, f] of Object.entries(t.findResources('AWS::Lambda::Function')) as any[]) {
      const secret = f.Properties.Environment?.Variables?.DB_SECRET_ARN;
      if (secret === undefined) continue;
      out.push({
        stack,
        id,
        roleId: f.Properties.Role['Fn::GetAtt'][0],
        envSecret: JSON.stringify(secret),
      });
    }
    for (const [id, d] of Object.entries(t.findResources('AWS::ECS::TaskDefinition')) as any[]) {
      for (const c of d.Properties.ContainerDefinitions as any[]) {
        const e = (c.Environment as any[]).find((v) => v.Name === 'DB_SECRET_ARN');
        if (!e) continue;
        out.push({
          stack,
          id,
          roleId: d.Properties.TaskRoleArn['Fn::GetAtt'][0],
          envSecret: JSON.stringify(e.Value),
        });
      }
    }
  }
  return out;
}

/** Resources of every GetSecretValue statement attached to a principal's role, as JSON strings. */
function secretResources(app: ReturnType<typeof synthApp>, p: Principal): string[] {
  const t = (app.templates as Record<string, any>)[p.stack];
  const out: string[] = [];
  for (const policy of Object.values(t.findResources('AWS::IAM::Policy')) as any[]) {
    if (!JSON.stringify(policy.Properties.Roles).includes(`"${p.roleId}"`)) continue;
    for (const st of policy.Properties.PolicyDocument.Statement) {
      const actions: string[] = Array.isArray(st.Action) ? st.Action : [st.Action];
      if (st.Effect !== 'Allow' || !actions.includes('secretsmanager:GetSecretValue')) continue;
      for (const r of Array.isArray(st.Resource) ? st.Resource : [st.Resource]) {
        out.push(JSON.stringify(r));
      }
    }
  }
  return out;
}

/** The database role each principal must use, by stack (and, in the api stack, by function). */
function expectedRole(stack: string, id: string, mode: 'inline' | 'queue'): DbRole {
  if (stack === 'api') return id.startsWith('IndexRepair') ? 'ops' : 'api';
  if (stack === 'ingest') return mode === 'inline' ? 'ingestInline' : 'ingest';
  if (stack === 'fanout') return id.startsWith('OutboxDrainer') ? 'ops' : 'fanout';
  if (stack === 'delivery' || stack === 'sender') return 'sender';
  throw new Error(`no expected database role for ${stack}/${id}: add it to this test`);
}

describe.each([
  ['dev', 'inline'],
  ['prod', 'queue'],
] as const)('database roles wiring (%s)', (stage, mode) => {
  const app = synthApp(stage);
  const principals = dbPrincipals(app);
  const roleIds = Object.fromEntries(
    (Object.keys(DB_ROLES) as DbRole[]).map((r) => [
      r,
      logicalIdOf(app, app.stacks.data.roleSecretArns[r]),
    ]),
  ) as Record<DbRole, string>;
  const adminId = logicalIdOf(app, app.stacks.data.secretArn);

  it('covers every compute principal that talks to the database', () => {
    // api, index repair, ingest, planner, shard, outbox drainer, delivery worker, sender
    expect(principals.map((p) => p.id.replace(/[0-9A-F]{8}$/, '')).sort()).toHaveLength(8);
  });

  it('gives each principal exactly one DB secret, its own role, in IAM and in DB_SECRET_ARN', () => {
    for (const p of principals) {
      const role = expectedRole(p.stack, p.id, mode);
      const granted = secretResources(app, p).filter((r) =>
        Object.values(roleIds).some((id) => r.includes(id)),
      );
      expect(granted, `${p.stack}/${p.id}`).toHaveLength(1);
      expect(granted[0], `${p.stack}/${p.id} is granted the wrong role secret`).toContain(
        roleIds[role],
      );
      expect(p.envSecret, `${p.stack}/${p.id} DB_SECRET_ARN`).toContain(roleIds[role]);
      for (const other of Object.keys(roleIds) as DbRole[]) {
        if (other !== role) expect(p.envSecret).not.toContain(roleIds[other]);
      }
    }
  });

  it('lets no Lambda or task role read the admin secret, and never passes it to one', () => {
    for (const p of principals) {
      expect(secretResources(app, p).join(' ')).not.toContain(adminId);
      expect(p.envSecret).not.toContain(adminId);
    }
    // Stronger: the admin secret is not referenced by any stack but its own.
    for (const [name, t] of Object.entries(app.templates)) {
      if (name === 'data') continue;
      expect(JSON.stringify(t.toJSON()), name).not.toContain(adminId);
    }
  });

  it('keeps rds-data on the cluster for every principal', () => {
    for (const p of principals) {
      const t = (app.templates as Record<string, any>)[p.stack];
      const actions = (Object.values(t.findResources('AWS::IAM::Policy')) as any[])
        .filter((pol) => JSON.stringify(pol.Properties.Roles).includes(`"${p.roleId}"`))
        .flatMap((pol) => pol.Properties.PolicyDocument.Statement)
        .flatMap((st: any) => (Array.isArray(st.Action) ? st.Action : [st.Action]));
      expect(actions, `${p.stack}/${p.id}`).toContain('rds-data:ExecuteStatement');
    }
  });
});

describe('DataStack role secrets', () => {
  const t = synthApp('dev').templates.data;

  it('creates one named secret per role, with the role as its fixed username, plus the admin one', () => {
    t.resourceCountIs('AWS::SecretsManager::Secret', 1 + Object.keys(DB_ROLES).length);
    for (const role of Object.keys(DB_ROLES) as DbRole[]) {
      t.hasResourceProperties('AWS::SecretsManager::Secret', {
        Name: dbRoleSecretName('dev', role),
        GenerateSecretString: Match.objectLike({
          SecretStringTemplate: Match.stringLikeRegexp(`"username":"${DB_ROLES[role]}"`),
          GenerateStringKey: 'password',
        }),
      });
    }
  });

  it('attaches every secret to the cluster so the Data API accepts it', () => {
    t.resourceCountIs(
      'AWS::SecretsManager::SecretTargetAttachment',
      1 + Object.keys(DB_ROLES).length,
    );
  });

  it('names secrets per stage', () => {
    expect(dbRoleSecretName('prod', 'api')).toBe('relay/prod/db/relay_api');
  });
});

// ---------- the operator script, against a real Postgres (PGlite) ----------

const MIGRATIONS = fileURLToPath(new URL('../../backend/migrations/', import.meta.url));

async function migratedDb(): Promise<{ pg: PGlite; admin: AdminDb; grantsSql: string }> {
  const pg = new PGlite();
  for (const file of readdirSync(MIGRATIONS)
    .filter((f) => f.endsWith('.sql'))
    .sort()) {
    if (file.startsWith('0001_')) continue; // PostGIS is not bundled with PGlite
    for (const s of splitSql(readFileSync(MIGRATIONS + file, 'utf8'))) await pg.query(s);
  }
  const admin: AdminDb = {
    async query(sql, params) {
      return (await pg.query<Record<string, unknown>>(sql, params)).rows;
    },
  };
  return { pg, admin, grantsSql: readFileSync(MIGRATIONS + '0011_roles.sql', 'utf8') };
}

const passwordOf = (role: string) => `pw-of-${role}-'quoted"\\-${'x'.repeat(20)}`;
const secretsFor =
  (override: Record<string, unknown> = {}) =>
  async (name: string) => {
    const role = name.split('/').pop() ?? '';
    return { username: role, password: passwordOf(role), ...override };
  };

describe('scripts/db-roles.ts', () => {
  it('enables LOGIN and sets each role password from its own secret', async () => {
    const { pg, admin, grantsSql } = await migratedDb();
    const logs: string[] = [];
    const enabled = await syncDbRoles({
      db: admin,
      stage: 'dev',
      grantsSql,
      readSecret: secretsFor(),
      log: (m) => logs.push(m),
    });
    expect(enabled.sort()).toEqual(Object.values(DB_ROLES).sort());
    const rows = (
      await pg.query<{ rolname: string; rolcanlogin: boolean; ok: boolean }>(
        `SELECT rolname, rolcanlogin, rolpassword IS NOT NULL AS ok
           FROM pg_authid WHERE rolname LIKE 'relay\\_%'`,
      )
    ).rows;
    expect(rows).toHaveLength(Object.keys(DB_ROLES).length);
    expect(rows.every((r) => r.rolcanlogin && r.ok)).toBe(true);
    // Nothing logged contains a password.
    expect(logs.join('\n')).not.toContain('pw-of-');
  });

  it('survives a quote, backslash or $1 inside a password, and a second run', async () => {
    const { pg, admin, grantsSql } = await migratedDb();
    const tricky = 'p\'a"s\\s $1 :p1; DROP TABLE devices; --xxxx';
    for (let i = 0; i < 2; i++) {
      await syncDbRoles({
        db: admin,
        stage: 'dev',
        grantsSql,
        readSecret: secretsFor({ password: tricky }),
      });
    }
    // The table is still there and the roles are still enabled.
    await pg.query('SELECT 1 FROM devices');
    const login = await pg.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM pg_roles WHERE rolname LIKE 'relay\\_%' AND rolcanlogin",
    );
    expect(login.rows[0]?.n).toBe(Object.keys(DB_ROLES).length);
  });

  it('refuses a secret whose username is not the role, without enabling anything', async () => {
    const { pg, admin } = await migratedDb();
    await expect(
      syncDbRoles({
        db: admin,
        stage: 'dev',
        readSecret: secretsFor({ username: 'relay_admin' }),
      }),
    ).rejects.toThrow(/username is not relay_/);
    const login = await pg.query<{ n: number }>(
      "SELECT count(*)::int AS n FROM pg_roles WHERE rolname LIKE 'relay\\_%' AND rolcanlogin",
    );
    expect(login.rows[0]?.n).toBe(0);
  });

  it('refuses a missing or short password, and never echoes one in an error', async () => {
    const { admin } = await migratedDb();
    await expect(
      syncDbRoles({ db: admin, stage: 'dev', readSecret: secretsFor({ password: 'short' }) }),
    ).rejects.toThrow(/password is missing or too short/);
    // A database failure while running ALTER ROLE does not leak the statement (it holds the password).
    const failing: AdminDb = {
      async query(sql, params) {
        if (sql.startsWith('ALTER ROLE')) throw new Error(`syntax error near ${sql}`);
        return admin.query(sql, params);
      },
    };
    const err: unknown = await syncDbRoles({
      db: failing,
      stage: 'dev',
      readSecret: secretsFor(),
    }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toMatch(/ALTER ROLE failed/);
    expect((err as Error).message).not.toContain('pw-of-');
  });

  it('splits dollar-quoted blocks whole (the migration relies on it)', () => {
    const parts = splitSql(readFileSync(MIGRATIONS + '0011_roles.sql', 'utf8'));
    expect(parts.filter((s) => s.startsWith('DO $$'))).toHaveLength(2);
    expect(parts.every((s) => !s.endsWith(';'))).toBe(true);
  });
});
