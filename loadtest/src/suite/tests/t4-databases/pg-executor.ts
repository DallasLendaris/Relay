/**
 * SqlExecutor on node-postgres, for the real Postgres container of test 4. The backend runs on the
 * RDS Data API in AWS (`DataApiExecutor`); stores only see the `SqlExecutor` interface, so this is a
 * drop-in. Same contract: positional $n parameters, `transaction()` shares one connection and
 * commits or rolls back, no nesting.
 *
 * `role`: every pooled connection runs `SET ROLE <role>` once when it is opened, so a component
 * (api, ops, ...) is held to the grants of migration 0011 exactly as its Lambda would be.
 */
import pg from 'pg';
import { readdir } from 'node:fs/promises';
import { runMigrations, type SqlExecutor } from '../../../../../backend/src/db/sql.js';

export interface PgExecutorOptions {
  /** Postgres role to SET ROLE to on every new connection (the login user must be allowed). */
  role?: string;
  /** Pool size. Default 10. */
  max?: number;
}

/**
 * jsonb/json values come back parsed from node-postgres, as the stores expect (`parseJson` accepts
 * both). int8 and numeric stay strings (count(*) results are cast to int in the SQL, or read with
 * Number()). Timestamps are formatted in SQL (`isoUtc`), so no date parsing is involved.
 */
export class PgExecutor implements SqlExecutor {
  readonly pool: pg.Pool;

  constructor(connectionString: string, opts: PgExecutorOptions = {}) {
    this.pool = new pg.Pool({ connectionString, max: opts.max ?? 10 });
    // Idle-client errors (server restart) must not crash the process.
    this.pool.on('error', () => undefined);
    if (opts.role) {
      const role = opts.role.replace(/[^a-z0-9_]/gi, '');
      this.pool.on('connect', (client) => {
        // pg queues this ahead of the first query on the connection.
        void client.query(`SET ROLE ${role}`).catch(() => undefined);
      });
    }
  }

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    const res = await this.pool.query(sql, params as unknown[]);
    return res.rows as T[];
  }

  async transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    const tx: SqlExecutor = {
      query: async <R>(sql: string, params: unknown[] = []) =>
        (await client.query(sql, params as unknown[])).rows as R[],
      transaction: () => Promise.reject(new Error('nested transactions are not supported')),
    };
    try {
      await client.query('BEGIN');
      const out = await fn(tx);
      await client.query('COMMIT');
      return out;
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch {
        /* keep the original error */
      }
      throw err;
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

/** Same URL with another database name. */
export function withDatabase(url: string, database: string): string {
  const u = new URL(url);
  u.pathname = `/${database}`;
  return u.toString();
}

/** Drops and recreates a database (connected to the maintenance database of `adminUrl`). */
export async function recreateDatabase(adminUrl: string, database: string): Promise<void> {
  const admin = new PgExecutor(adminUrl, { max: 1 });
  try {
    const name = database.replace(/[^a-z0-9_]/gi, '');
    await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    await admin.query(`CREATE DATABASE ${name}`);
  } finally {
    await admin.close();
  }
}

export interface MigrationTiming {
  name: string;
  ms: number;
}

/**
 * Runs every migration in name order with the repo's own runner, ONE FILE PER CALL so each can be
 * timed (the runner's `skip` leaves out every file but the wanted one; applied files are recorded in
 * schema_migrations as usual, so the end state is identical to a single run).
 */
export async function runMigrationsTimed(
  exec: SqlExecutor,
  dir: string,
  opts: { skip?: (name: string) => boolean } = {},
): Promise<MigrationTiming[]> {
  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  const timings: MigrationTiming[] = [];
  for (const file of files) {
    if (opts.skip?.(file)) continue;
    const t0 = performance.now();
    const applied = await runMigrations(exec, dir, { skip: (n) => n !== file });
    if (applied.length !== 1 || applied[0] !== file) {
      throw new Error(`migration ${file} was not applied (got ${JSON.stringify(applied)})`);
    }
    timings.push({ name: file, ms: performance.now() - t0 });
  }
  return timings;
}

/** All table-level and column-level grants to the relay_* roles, as a sorted list of strings. */
export async function grantSnapshot(exec: SqlExecutor): Promise<string[]> {
  const t = await exec.query<{ s: string }>(
    `SELECT grantee || ' ' || table_name || ' ' || privilege_type AS s
       FROM information_schema.role_table_grants
      WHERE grantee LIKE 'relay\\_%'`,
    [],
  );
  const c = await exec.query<{ s: string }>(
    `SELECT grantee || ' ' || table_name || '.' || column_name || ' ' || privilege_type AS s
       FROM information_schema.role_column_grants
      WHERE grantee LIKE 'relay\\_%'`,
    [],
  );
  return [...t, ...c].map((r) => r.s).sort();
}

/**
 * Re-runs one migration file that was already applied (forgets it in schema_migrations first), the
 * way infra/scripts/db-roles.ts re-applies 0011 after later migrations. Returns the ms it took.
 */
export async function rerunMigration(
  exec: SqlExecutor,
  dir: string,
  file: string,
): Promise<number> {
  await exec.query('DELETE FROM schema_migrations WHERE name = $1', [file]);
  const t0 = performance.now();
  const applied = await runMigrations(exec, dir, { skip: (n) => n !== file });
  if (applied.length !== 1) throw new Error(`re-run of ${file} did not apply`);
  return performance.now() - t0;
}
