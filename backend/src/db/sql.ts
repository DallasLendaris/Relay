/**
 * Minimal SQL access layer. Stores depend on SqlExecutor only, so the same code
 * runs on Aurora (via the RDS Data API) in AWS and on PGlite in tests.
 */
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import {
  BeginTransactionCommand,
  CommitTransactionCommand,
  ExecuteStatementCommand,
  RollbackTransactionCommand,
  type RDSDataClient,
  type SqlParameter,
} from '@aws-sdk/client-rds-data';

export interface SqlExecutor {
  /** Run one statement with positional $1, $2... parameters. Returns rows (empty for no rows). */
  query<T>(sql: string, params: unknown[]): Promise<T[]>;
  /**
   * Run fn with an executor whose statements all share one transaction. Commits when fn
   * returns, rolls back (and rethrows) when it throws. Do not use the outer executor
   * inside fn: those statements would run outside the transaction.
   */
  transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T>;
}

/** Convert one JS value into a Data API typed field. Column types are fixed with SQL casts (e.g. $1::uuid). */
export function toDataApiParam(name: string, value: unknown): SqlParameter {
  let field: SqlParameter['value'];
  if (value === null || value === undefined) field = { isNull: true };
  else if (typeof value === 'string') field = { stringValue: value };
  else if (typeof value === 'boolean') field = { booleanValue: value };
  else if (typeof value === 'number') {
    field = Number.isInteger(value) ? { longValue: value } : { doubleValue: value };
  } else if (value instanceof Date) field = { stringValue: value.toISOString() };
  else field = { stringValue: JSON.stringify(value) }; // objects/arrays: cast to jsonb in SQL
  return { name, value: field };
}

/** Waits this many milliseconds. Injectable so tests do not really wait. */
export type Sleep = (ms: number) => Promise<void>;

const realSleep: Sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Retries after an Aurora auto-pause resume: 0.5 s, 1, 2, 4, 5, 5 (about 17 s in total). */
export const RESUME_MAX_RETRIES = 6;
const RESUME_FIRST_DELAY_MS = 500;
const RESUME_MAX_DELAY_MS = 5000;

/** True while Aurora Serverless v2 is resuming from auto-pause (the first call after idle). */
function isResuming(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  return err.name === 'DatabaseResumingException' || /resuming/i.test(err.message);
}

/** Executes SQL through the RDS Data API (HTTPS), so Lambdas need no VPC. */
export class DataApiExecutor implements SqlExecutor {
  constructor(
    private readonly client: RDSDataClient,
    private readonly clusterArn: string,
    private readonly secretArn: string,
    private readonly database: string,
    private readonly sleep: Sleep = realSleep,
  ) {}

  /** Runs one Data API call, waiting and retrying while the database resumes. */
  private async withResumeRetry<R>(call: () => Promise<R>): Promise<R> {
    let delay = RESUME_FIRST_DELAY_MS;
    for (let retry = 0; ; retry++) {
      try {
        return await call();
      } catch (err) {
        if (!isResuming(err) || retry >= RESUME_MAX_RETRIES) throw err;
        await this.sleep(delay);
        delay = Math.min(delay * 2, RESUME_MAX_DELAY_MS);
      }
    }
  }

  query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    return this.run<T>(sql, params);
  }

  async transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
    const target = {
      resourceArn: this.clusterArn,
      secretArn: this.secretArn,
      database: this.database,
    };
    const { transactionId } = await this.withResumeRetry(() =>
      this.client.send(new BeginTransactionCommand(target)),
    );
    if (!transactionId) throw new Error('Data API returned no transaction id');
    // Statements inside carry the transaction id; nesting is not supported.
    const tx: SqlExecutor = {
      query: <R>(sql: string, params: unknown[] = []) => this.run<R>(sql, params, transactionId),
      transaction: () => Promise.reject(new Error('nested transactions are not supported')),
    };
    try {
      const result = await fn(tx);
      await this.client.send(new CommitTransactionCommand({ ...target, transactionId }));
      return result;
    } catch (err) {
      try {
        await this.client.send(
          new RollbackTransactionCommand({
            resourceArn: this.clusterArn,
            secretArn: this.secretArn,
            transactionId,
          }),
        );
      } catch {
        // Keep the original error; the Data API also rolls back idle transactions itself.
      }
      throw err;
    }
  }

  private async run<T>(sql: string, params: unknown[], transactionId?: string): Promise<T[]> {
    const command = new ExecuteStatementCommand({
      resourceArn: this.clusterArn,
      secretArn: this.secretArn,
      database: this.database,
      ...(transactionId ? { transactionId } : {}),
      // Data API uses :name placeholders instead of $n.
      sql: sql.replace(/\$(\d+)/g, ':p$1'),
      parameters: params.map((v, i) => toDataApiParam(`p${i + 1}`, v)),
      formatRecordsAs: 'JSON',
    });
    const out = await this.withResumeRetry(() => this.client.send(command));
    // formattedRecords is a JSON array of row objects; absent when the statement returns no rows.
    return out.formattedRecords ? (JSON.parse(out.formattedRecords) as T[]) : [];
  }
}

/** jsonb columns come back as JSON text from the Data API but as objects from other drivers. */
export function parseJson<T>(value: unknown): T {
  return (typeof value === 'string' ? JSON.parse(value) : value) as T;
}

/**
 * Split a migration file into statements on ";". Dollar-quoted bodies ($$ ... $$ or $tag$ ... $tag$,
 * as in DO blocks) are kept whole, so ";" inside them does not split. Still deliberately simple:
 * ";" inside ordinary string literals is not supported, and "--" comment lines are dropped first
 * (so a dollar-quoted body must not contain a line starting with "--").
 */
export function splitStatements(sql: string): string[] {
  const text = sql
    .split('\n')
    .filter((line) => !line.trim().startsWith('--'))
    .join('\n');
  const out: string[] = [];
  let start = 0;
  let tag: string | undefined; // the open dollar-quote delimiter, e.g. "$$" or "$body$"
  for (let i = 0; i < text.length; i++) {
    if (tag === undefined) {
      if (text[i] === '$') {
        const m = /^\$(?:[A-Za-z_]\w*)?\$/.exec(text.slice(i));
        // "$1" parameters never match: a tag cannot start with a digit.
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

/**
 * Apply every *.sql file in dir (in name order) that is not yet recorded in
 * schema_migrations. Returns the names applied this run. `skip` lets tests leave out
 * migrations their database cannot run (e.g. the PostGIS one).
 */
export async function runMigrations(
  exec: SqlExecutor,
  dir: string,
  opts: { skip?: (name: string) => boolean } = {},
): Promise<string[]> {
  await exec.query(
    'CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())',
    [],
  );
  const done = new Set(
    (await exec.query<{ name: string }>('SELECT name FROM schema_migrations', [])).map(
      (r) => r.name,
    ),
  );
  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  const applied: string[] = [];
  for (const file of files) {
    if (done.has(file) || opts.skip?.(file)) continue;
    for (const statement of splitStatements(await readFile(join(dir, file), 'utf8'))) {
      await exec.query(statement, []);
    }
    await exec.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
    applied.push(file);
  }
  return applied;
}
