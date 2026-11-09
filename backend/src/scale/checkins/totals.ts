import { QueryCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { CellTotal } from './types.js';

export const TOTALS_BY_ALERT_INDEX = 'byAlert';
export const DEFAULT_TOTALS_PAGE = 500;

export interface TotalsPage {
  totals: CellTotal[];
  /** Opaque; pass back to get the next page. Absent on the last page. */
  cursor?: string;
}

/**
 * Reads per-cell check-in totals for the agency side. Queries the `byAlert` GSI (partition key
 * alertId, sort key pk) on the totals table, so it never touches the check-ins table. The index
 * is eventually consistent: totals trail the check-ins by the stream delay (seconds).
 * One alert is at most a few thousand cells, so a few pages cover the whole map.
 */
export class DynamoCheckInTotals {
  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly totalsTable: string,
  ) {}

  async listTotals(
    alertId: string,
    cursor?: string,
    limit = DEFAULT_TOTALS_PAGE,
  ): Promise<TotalsPage> {
    const out = await this.doc.send(
      new QueryCommand({
        TableName: this.totalsTable,
        IndexName: TOTALS_BY_ALERT_INDEX,
        KeyConditionExpression: 'alertId = :a',
        ExpressionAttributeValues: { ':a': alertId },
        Limit: limit,
        ...(cursor ? { ExclusiveStartKey: decodeCursor(cursor) } : {}),
      }),
    );
    const totals = (out.Items ?? []).map((i): CellTotal => ({
      alertId: String(i['alertId']),
      h3Cell: String(i['h3Cell']),
      checkedIn: Number(i['checkedIn'] ?? 0),
    }));
    return {
      totals,
      ...(out.LastEvaluatedKey ? { cursor: encodeCursor(out.LastEvaluatedKey) } : {}),
    };
  }
}

const encodeCursor = (key: Record<string, unknown>): string =>
  Buffer.from(JSON.stringify(key)).toString('base64url');

function decodeCursor(cursor: string): Record<string, unknown> {
  try {
    const v: unknown = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
      return v as Record<string, unknown>;
    }
  } catch {
    // fall through
  }
  throw new Error('Invalid totals cursor');
}
