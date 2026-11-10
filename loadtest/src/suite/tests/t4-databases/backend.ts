/** The real Backend: DynamoDB Local through the AWS SDK, Postgres through node-postgres. */
import type { SqlExecutor } from '../../../../../backend/src/db/sql.js';
import type { Backend } from './kernels.js';
import { PgExecutor } from './pg-executor.js';
import { makeDynamoClients, type TableNames } from './tables.js';

export interface RealBackendOptions {
  dynamoEndpoint: string;
  postgresUrl: string;
  names: TableNames;
  scratchDir: string;
  /** Connections per role pool. Default 10. */
  poolSize?: number;
}

export interface RealBackend extends Backend {
  close(): Promise<void>;
  /** The admin (superuser, no SET ROLE) executor. */
  admin: PgExecutor;
}

export function makeRealBackend(o: RealBackendOptions): RealBackend {
  const { doc, raw } = makeDynamoClients(o.dynamoEndpoint);
  const pools = new Map<string, PgExecutor>();
  const sql = (role?: string): SqlExecutor => {
    const key = role ?? '';
    let p = pools.get(key);
    if (!p) {
      p = new PgExecutor(o.postgresUrl, { max: o.poolSize ?? 10, ...(role ? { role } : {}) });
      pools.set(key, p);
    }
    return p;
  };
  return {
    doc,
    names: o.names,
    sql,
    scratchDir: o.scratchDir,
    admin: sql() as PgExecutor,
    async close() {
      await Promise.all([...pools.values()].map((p) => p.close()));
      raw.destroy();
    },
  };
}
