import { beforeEach, describe, expect, it } from 'vitest';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import type { SqlExecutor } from '../../src/db/sql.js';
import { alert, freshDb, type PgliteExecutor } from './helpers.js';

/**
 * Wraps an executor and records every statement, tagged by whether it ran inside a
 * transaction. Inside a transaction it also records the isolation level the reads saw.
 */
function spy(db: PgliteExecutor) {
  const log: { sql: string; inTx: boolean }[] = [];
  const isolation: string[] = [];
  let transactions = 0;
  const exec: SqlExecutor = {
    query: async <T>(sql: string, params: unknown[]) => {
      log.push({ sql, inTx: false });
      return db.query<T>(sql, params);
    },
    transaction: <T>(fn: (tx: SqlExecutor) => Promise<T>) => {
      transactions++;
      return db.transaction(async (tx) =>
        fn({
          query: async <R>(sql: string, params: unknown[]) => {
            log.push({ sql, inTx: true });
            const rows = await tx.query<R>(sql, params);
            // After the record read, ask the database which isolation level applies.
            if (/SELECT (a\.)?record/.test(sql)) {
              const [row] = await tx.query<{ transaction_isolation: string }>(
                'SHOW transaction_isolation',
                [],
              );
              isolation.push(row?.transaction_isolation ?? '');
            }
            return rows;
          },
          transaction: tx.transaction,
        }),
      );
    },
  };
  return { exec, log, isolation, transactions: () => transactions };
}

describe('PostgresAlertStore consistent reads', () => {
  let db: PgliteExecutor;
  beforeEach(async () => {
    db = await freshDb();
  });

  const cells = ['872830828ffffff', '872830829ffffff', '87283082affffff'];

  it('get reads the record and the cells in one REPEATABLE READ transaction', async () => {
    const a = alert({ area: { ...alert().area, h3Cells: cells } });
    await new PostgresAlertStore(db).save(a);

    const s = spy(db);
    const got = await new PostgresAlertStore(s.exec).get(a.id);
    expect(got?.area.h3Cells).toEqual(cells);
    expect(s.transactions()).toBe(1);
    expect(s.log.every((l) => l.inTx)).toBe(true); // nothing ran outside the transaction
    expect(s.log[0]?.sql).toMatch(/^SET TRANSACTION ISOLATION LEVEL REPEATABLE READ/);
    expect(s.log.some((l) => l.sql.includes('FROM alert_cells'))).toBe(true);
    expect(s.isolation).toEqual(['repeatable read']);
  });

  it('findBySourceId does the same', async () => {
    const a = alert({ area: { ...alert().area, h3Cells: cells } });
    await new PostgresAlertStore(db).save(a);

    const s = spy(db);
    const got = await new PostgresAlertStore(s.exec).findBySourceId(a.sourceId);
    expect(got?.id).toBe(a.id);
    expect(got?.area.h3Cells).toEqual(cells);
    expect(s.transactions()).toBe(1);
    expect(s.log.every((l) => l.inTx)).toBe(true);
    expect(s.isolation).toEqual(['repeatable read']);
  });

  it('returns null for a missing alert without error', async () => {
    const store = new PostgresAlertStore(db);
    expect(await store.get('99999999-9999-4999-8999-999999999999')).toBeNull();
    expect(await store.findBySourceId('urn:nope')).toBeNull();
  });
});
