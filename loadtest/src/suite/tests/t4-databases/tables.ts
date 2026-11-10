/**
 * DynamoDB table definitions for test 4, mirroring infra/lib/*-stack.ts (the unit test
 * `suite-t4.test.ts` parses the stack sources and fails when a key schema or index drifts).
 *
 * Streams are NOT enabled on the check-ins table: DynamoDB Local keeps stream records in memory,
 * and the test feeds the real aggregator with synthetic INSERT records instead (see kernels.ts).
 */
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  UpdateTimeToLiveCommand,
  type CreateTableCommandInput,
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { Agent } from 'node:http';

export type TableKey =
  'cellIndex' | 'cellIndexD' | 'idempotency' | 'checkins' | 'checkinTotals' | 'seen';

/** Physical table names of one test run. */
export type TableNames = Record<TableKey, string>;

export interface TableSpec {
  /** Physical table key; `cellIndexD` is a second copy of the cell-index table used by scenario d. */
  key: TableKey;
  /** CDK construct id and stack file the definition is derived from (checked by the unit test). */
  source: { file: string; constructId: string; tableVar?: string };
  hash: { name: string };
  range?: { name: string };
  ttl: string;
  gsis: Array<{ name: string; hash: string; projection: 'ALL' | 'KEYS_ONLY' }>;
}

const CELL_INDEX: Omit<TableSpec, 'key'> = {
  source: { file: 'fanout-stack.ts', constructId: 'CellIndexTable' },
  hash: { name: 'h3Cell' },
  range: { name: 'sk' },
  ttl: '',
  gsis: [],
};

export const TABLE_SPECS: readonly TableSpec[] = [
  { key: 'cellIndex', ...CELL_INDEX },
  { key: 'cellIndexD', ...CELL_INDEX },
  {
    key: 'idempotency',
    source: { file: 'delivery-stack.ts', constructId: 'IdempotencyTable' },
    hash: { name: 'pk' },
    ttl: 'ttl',
    gsis: [],
  },
  {
    key: 'checkins',
    source: { file: 'api-stack.ts', constructId: 'CheckinsTable', tableVar: 'checkins' },
    hash: { name: 'pk' },
    ttl: 'ttl',
    gsis: [{ name: 'byDevice', hash: 'deviceId', projection: 'KEYS_ONLY' }],
  },
  {
    key: 'checkinTotals',
    source: { file: 'api-stack.ts', constructId: 'CheckinTotalsTable', tableVar: 'checkinTotals' },
    hash: { name: 'pk' },
    ttl: 'ttl',
    gsis: [{ name: 'byAlert', hash: 'alertId', projection: 'ALL' }],
  },
  {
    key: 'seen',
    source: { file: 'ingest-stack.ts', constructId: 'StateTable' },
    hash: { name: 'sourceId' },
    ttl: 'expiresAt',
    gsis: [],
  },
];

export function tableNamesFor(runId: string): TableNames {
  const n = (s: string) => `t4-${s}-${runId}`;
  return {
    cellIndex: n('cell-index'),
    cellIndexD: n('cell-index-d'),
    idempotency: n('idempotency'),
    checkins: n('checkins'),
    checkinTotals: n('checkin-totals'),
    seen: n('seen'),
  };
}

/** CreateTable input for one spec (on-demand billing, like every table in the stacks). */
export function createTableInput(spec: TableSpec, tableName: string): CreateTableCommandInput {
  const attrs = new Set<string>([spec.hash.name]);
  if (spec.range) attrs.add(spec.range.name);
  for (const g of spec.gsis) attrs.add(g.hash);
  return {
    TableName: tableName,
    BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [...attrs].map((AttributeName) => ({
      AttributeName,
      AttributeType: 'S',
    })),
    KeySchema: [
      { AttributeName: spec.hash.name, KeyType: 'HASH' },
      ...(spec.range ? [{ AttributeName: spec.range.name, KeyType: 'RANGE' as const }] : []),
    ],
    ...(spec.gsis.length > 0
      ? {
          GlobalSecondaryIndexes: spec.gsis.map((g) => ({
            IndexName: g.name,
            KeySchema: [{ AttributeName: g.hash, KeyType: 'HASH' as const }],
            Projection: { ProjectionType: g.projection },
          })),
        }
      : {}),
  };
}

/** Creates every table (and enables TTL where the stack does; Local accepts but never expires). */
export async function createTables(
  client: Pick<DynamoDBClient, 'send'>,
  names: TableNames,
): Promise<string[]> {
  const notes: string[] = [];
  for (const spec of TABLE_SPECS) {
    const name = names[spec.key];
    await client.send(new CreateTableCommand(createTableInput(spec, name)));
    if (spec.ttl) {
      try {
        await client.send(
          new UpdateTimeToLiveCommand({
            TableName: name,
            TimeToLiveSpecification: { Enabled: true, AttributeName: spec.ttl },
          }),
        );
      } catch (e) {
        notes.push(`TTL on ${name} not enabled: ${e instanceof Error ? e.message : String(e)}`);
      }
    }
  }
  return notes;
}

export async function dropTables(
  client: Pick<DynamoDBClient, 'send'>,
  names: TableNames,
): Promise<void> {
  for (const name of Object.values(names)) {
    try {
      await client.send(new DeleteTableCommand({ TableName: name }));
    } catch {
      /* best effort */
    }
  }
}

/** Raw and document clients pointed at DynamoDB Local (dummy credentials, keep-alive sockets). */
export function makeDynamoClients(endpoint: string): {
  raw: DynamoDBClient;
  doc: DynamoDBDocumentClient;
} {
  const raw = new DynamoDBClient({
    endpoint,
    region: 'us-east-1',
    credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
    maxAttempts: 6, // DynamoDB Local drops sockets under heavy local concurrency; retry like AWS would
    requestHandler: new NodeHttpHandler({
      httpAgent: new Agent({ keepAlive: true, maxSockets: 256 }),
    }),
  });
  return { raw, doc: DynamoDBDocumentClient.from(raw) };
}
