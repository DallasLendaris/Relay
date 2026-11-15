import { QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { CellIndex, CellIndexEntry, CellIndexPage } from '../contracts.js';

/**
 * Read-only view of the cell-index table for the shard worker: one Query per page on
 * pk = h3Cell, so the worker's role needs only dynamodb:Query. Items are CellIndexEntry
 * fields plus pk (`h3Cell`) and sk (`${deviceId}#${placeId}`).
 * The full read/write DynamoCellIndex lives with the cell-index module; it can replace this.
 */
export class DynamoCellReader implements Pick<CellIndex, 'listForCell'> {
  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly tableName: string,
  ) {}

  async listForCell(h3Cell: string, cursor?: string, limit = 1_000): Promise<CellIndexPage> {
    const out = await this.doc.send(
      new QueryCommand({
        TableName: this.tableName,
        KeyConditionExpression: 'h3Cell = :c',
        ExpressionAttributeValues: { ':c': h3Cell },
        Limit: limit,
        ...(cursor
          ? { ExclusiveStartKey: JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) }
          : {}),
      }),
    );
    const entries = (out.Items ?? []).map((item): CellIndexEntry => ({
      h3Cell: String(item.h3Cell),
      deviceId: String(item.deviceId),
      placeId: String(item.placeId),
      target: (item.target as CellIndexEntry['target'] | undefined) ?? null,
      language: item.language === 'es' ? 'es' : 'en',
      ...(Array.isArray(item.deviceCells) && item.deviceCells.length > 0
        ? { deviceCells: item.deviceCells.filter((c): c is string => typeof c === 'string') }
        : {}),
    }));
    return out.LastEvaluatedKey
      ? {
          entries,
          cursor: Buffer.from(JSON.stringify(out.LastEvaluatedKey), 'utf8').toString('base64url'),
        }
      : { entries };
  }
}
