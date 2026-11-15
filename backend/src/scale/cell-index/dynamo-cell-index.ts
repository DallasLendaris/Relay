import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommand,
  DeleteCommand,
  QueryCommand,
  UpdateCommand,
  type BatchWriteCommandInput,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { CellIndex, CellIndexEntry, CellIndexPage, PushTarget } from '../contracts.js';
import {
  DEFAULT_PAGE_LIMIT,
  decodeCursor,
  encodeCursor,
  mapLimit,
  plainTarget,
  sortKey,
} from './util.js';

/** DynamoDB BatchWriteItem accepts at most 25 requests. */
const BATCH_SIZE = 25;
const UPDATE_CONCURRENCY = 25;
const BATCH_CONCURRENCY = 10;
const MAX_RETRIES = 8;
const BASE_DELAY_MS = 50;
const MAX_DELAY_MS = 2000;

type Item = Record<string, unknown>;
type WriteRequests = NonNullable<BatchWriteCommandInput['RequestItems']>[string];

export interface DynamoCellIndexOptions {
  now?: () => Date;
  /** Waits this many ms. Injectable so tests do not really wait. */
  sleep?: (ms: number) => Promise<void>;
}

/** `deviceCells` as stored (a list of strings), or nothing for old entries. */
function deviceCellsOf(raw: unknown): { deviceCells?: string[] } {
  if (!Array.isArray(raw)) return {};
  const cells = raw.filter((c): c is string => typeof c === 'string');
  return cells.length > 0 ? { deviceCells: cells } : {};
}

const realSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/**
 * CellIndex on DynamoDB. Table: partition key `h3Cell` (S), sort key `sk` (S) =
 * `${deviceId}#${placeId}`. Attributes: deviceId, placeId, target ({platform, token, project?} or null),
 * language, deviceCells (list of strings, optional on old entries), updatedAt (ISO).
 */
export class DynamoCellIndex implements CellIndex {
  private readonly now: () => Date;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly tableName: string,
    options: DynamoCellIndexOptions = {},
  ) {
    this.now = options.now ?? (() => new Date());
    this.sleep = options.sleep ?? realSleep;
  }

  async put(entries: CellIndexEntry[]): Promise<void> {
    const updatedAt = this.now().toISOString();
    // A batch may not hold the same key twice; the last entry for a key wins.
    const byKey = new Map<string, Item>();
    for (const e of entries) {
      const sk = sortKey(e.deviceId, e.placeId);
      byKey.set(`${e.h3Cell}\u0000${sk}`, {
        h3Cell: e.h3Cell,
        sk,
        deviceId: e.deviceId,
        placeId: e.placeId,
        target: e.target ? plainTarget(e.target) : null,
        language: e.language,
        ...(e.deviceCells ? { deviceCells: [...e.deviceCells] } : {}),
        updatedAt,
      });
    }
    const items = [...byKey.values()];
    const batches: Item[][] = [];
    for (let i = 0; i < items.length; i += BATCH_SIZE) batches.push(items.slice(i, i + BATCH_SIZE));
    await mapLimit(batches, BATCH_CONCURRENCY, (batch) => this.writeBatch(batch));
  }

  /** Writes one batch, retrying whatever DynamoDB leaves unprocessed with exponential backoff. */
  private async writeBatch(items: Item[]): Promise<void> {
    let pending: WriteRequests = items.map((Item) => ({ PutRequest: { Item } }));
    for (let attempt = 0; ; attempt++) {
      const res = await this.doc.send(
        new BatchWriteCommand({ RequestItems: { [this.tableName]: pending } }),
      );
      const left = res.UnprocessedItems?.[this.tableName];
      if (!left || left.length === 0) return;
      if (attempt >= MAX_RETRIES) {
        throw new Error(`cell index: ${left.length} items still unprocessed after retries`);
      }
      pending = left;
      const delay = Math.min(BASE_DELAY_MS * 2 ** attempt, MAX_DELAY_MS);
      await this.sleep(Math.random() * delay); // full jitter
    }
  }

  async remove(h3Cell: string, deviceId: string, placeId: string): Promise<void> {
    await this.doc.send(
      new DeleteCommand({
        TableName: this.tableName,
        Key: { h3Cell, sk: sortKey(deviceId, placeId) },
      }),
    );
  }

  async updateDevice(
    deviceId: string,
    cells: Array<{ h3Cell: string; placeId: string }>,
    patch: { target: PushTarget | null; language: 'en' | 'es' },
  ): Promise<void> {
    const updatedAt = this.now().toISOString();
    const target = patch.target ? plainTarget(patch.target) : null;
    await mapLimit(cells, UPDATE_CONCURRENCY, async ({ h3Cell, placeId }) => {
      try {
        await this.doc.send(
          new UpdateCommand({
            TableName: this.tableName,
            Key: { h3Cell, sk: sortKey(deviceId, placeId) },
            UpdateExpression: 'SET #target = :target, #language = :language, #updatedAt = :at',
            // Never resurrect an entry whose place was removed in the meantime.
            ConditionExpression: 'attribute_exists(h3Cell)',
            ExpressionAttributeNames: {
              '#target': 'target',
              '#language': 'language',
              '#updatedAt': 'updatedAt',
            },
            ExpressionAttributeValues: {
              ':target': target,
              ':language': patch.language,
              ':at': updatedAt,
            },
          }),
        );
      } catch (err) {
        if (!(err instanceof ConditionalCheckFailedException)) throw err;
      }
    });
  }

  async listForCell(h3Cell: string, cursor?: string, limit?: number): Promise<CellIndexPage> {
    const size = limit ?? DEFAULT_PAGE_LIMIT;
    if (!Number.isInteger(size) || size < 1) throw new Error('limit must be a positive integer');
    let startKey: Record<string, unknown> | undefined;
    if (cursor !== undefined) {
      const c = decodeCursor(cursor);
      // A cursor is only valid for the cell it came from.
      if (
        typeof c !== 'object' ||
        c === null ||
        (c as Item)['h3Cell'] !== h3Cell ||
        typeof (c as Item)['sk'] !== 'string'
      ) {
        throw new Error('invalid cell index cursor');
      }
      startKey = { h3Cell, sk: (c as Item)['sk'] };
    }
    const res = await this.doc.send(
      new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: '#pk = :cell',
        ExpressionAttributeNames: { '#pk': 'h3Cell' },
        ExpressionAttributeValues: { ':cell': h3Cell },
        Limit: size,
        ...(startKey ? { ExclusiveStartKey: startKey } : {}),
      }),
    );
    const entries = (res.Items ?? []).map((item): CellIndexEntry => {
      const target = item['target'] as PushTarget | null | undefined;
      return {
        h3Cell: String(item['h3Cell']),
        deviceId: String(item['deviceId']),
        placeId: String(item['placeId']),
        target: target ? plainTarget(target) : null,
        language: item['language'] === 'es' ? 'es' : 'en',
        ...deviceCellsOf(item['deviceCells']),
      };
    });
    const last = res.LastEvaluatedKey;
    return last
      ? { entries, cursor: encodeCursor({ h3Cell: last['h3Cell'], sk: last['sk'] }) }
      : { entries };
  }
}
