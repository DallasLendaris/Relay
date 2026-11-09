import {
  BeginTransactionCommand,
  CommitTransactionCommand,
  ExecuteStatementCommand,
  RDSDataClient,
  RollbackTransactionCommand,
} from '@aws-sdk/client-rds-data';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { DataApiExecutor, toDataApiParam } from '../../src/db/sql.js';

const rds = mockClient(RDSDataClient);
beforeEach(() => rds.reset());

describe('toDataApiParam', () => {
  it('maps JS values to Data API fields', () => {
    expect(toDataApiParam('p1', 'x').value).toEqual({ stringValue: 'x' });
    expect(toDataApiParam('p1', 3).value).toEqual({ longValue: 3 });
    expect(toDataApiParam('p1', 3.5).value).toEqual({ doubleValue: 3.5 });
    expect(toDataApiParam('p1', true).value).toEqual({ booleanValue: true });
    expect(toDataApiParam('p1', null).value).toEqual({ isNull: true });
    expect(toDataApiParam('p1', undefined).value).toEqual({ isNull: true });
    expect(toDataApiParam('p1', new Date('2026-01-01T00:00:00Z')).value).toEqual({
      stringValue: '2026-01-01T00:00:00.000Z',
    });
    expect(toDataApiParam('p1', { a: 1 }).value).toEqual({ stringValue: '{"a":1}' });
  });
});

describe('DataApiExecutor', () => {
  const exec = () =>
    new DataApiExecutor(
      new RDSDataClient({ region: 'us-west-2' }),
      'arn:cluster',
      'arn:secret',
      'relay',
    );

  it('sends named parameters, rewrites $n placeholders and parses JSON rows', async () => {
    rds.on(ExecuteStatementCommand).resolves({
      formattedRecords: JSON.stringify([
        { id: 'a', n: 1 },
        { id: 'b', n: 2 },
      ]),
    });
    const rows = await exec().query<{ id: string; n: number }>(
      'SELECT id, n FROM t WHERE id = $1::uuid AND n > $2 AND ok = $3 AND x IS NOT DISTINCT FROM $4',
      ['a', 0.5, true, null],
    );
    expect(rows).toEqual([
      { id: 'a', n: 1 },
      { id: 'b', n: 2 },
    ]);

    const input = rds.commandCalls(ExecuteStatementCommand)[0]?.args[0].input;
    expect(input).toMatchObject({
      resourceArn: 'arn:cluster',
      secretArn: 'arn:secret',
      database: 'relay',
      formatRecordsAs: 'JSON',
      sql: 'SELECT id, n FROM t WHERE id = :p1::uuid AND n > :p2 AND ok = :p3 AND x IS NOT DISTINCT FROM :p4',
    });
    expect(input?.parameters).toEqual([
      { name: 'p1', value: { stringValue: 'a' } },
      { name: 'p2', value: { doubleValue: 0.5 } },
      { name: 'p3', value: { booleanValue: true } },
      { name: 'p4', value: { isNull: true } },
    ]);
  });

  it('returns an empty list when the statement returns no records', async () => {
    rds.on(ExecuteStatementCommand).resolves({});
    expect(await exec().query('DELETE FROM t', [])).toEqual([]);
  });

  it('runs statements inside one Data API transaction and commits', async () => {
    rds.on(BeginTransactionCommand).resolves({ transactionId: 'tx-1' });
    rds.on(ExecuteStatementCommand).resolves({});
    rds.on(CommitTransactionCommand).resolves({});
    const out = await exec().transaction(async (tx) => {
      await tx.query('DELETE FROM t', []);
      return 'done';
    });
    expect(out).toBe('done');
    expect(rds.commandCalls(ExecuteStatementCommand)[0]?.args[0].input.transactionId).toBe('tx-1');
    expect(rds.commandCalls(CommitTransactionCommand)[0]?.args[0].input.transactionId).toBe('tx-1');
    expect(rds.commandCalls(RollbackTransactionCommand)).toHaveLength(0);
  });

  it('rolls back and rethrows when the work fails', async () => {
    rds.on(BeginTransactionCommand).resolves({ transactionId: 'tx-2' });
    rds.on(RollbackTransactionCommand).resolves({});
    await expect(
      exec().transaction(async () => {
        throw new Error('nope');
      }),
    ).rejects.toThrow('nope');
    expect(rds.commandCalls(RollbackTransactionCommand)[0]?.args[0].input.transactionId).toBe(
      'tx-2',
    );
    expect(rds.commandCalls(CommitTransactionCommand)).toHaveLength(0);
  });
});
