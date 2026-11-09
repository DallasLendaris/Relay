import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { GetCommand, PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { SeenStore } from '../../ports.js';

const TTL_SECONDS = 30 * 24 * 60 * 60;

/** Dedupe store. pk = sourceId; `expiresAt` (epoch seconds) is the table's TTL attribute. */
export class DynamoSeenStore implements SeenStore {
  constructor(
    private readonly db: DynamoDBDocumentClient,
    private readonly table: string,
    private readonly nowMs: () => number = Date.now,
  ) {}

  async has(sourceId: string): Promise<boolean> {
    const res = await this.db.send(
      new GetCommand({ TableName: this.table, Key: { sourceId }, ConsistentRead: true }),
    );
    return res.Item !== undefined;
  }

  async mark(sourceId: string, sent: string): Promise<void> {
    try {
      await this.db.send(
        new PutCommand({
          TableName: this.table,
          Item: {
            sourceId,
            sent,
            expiresAt: Math.floor(this.nowMs() / 1000) + TTL_SECONDS,
          },
          // Do not overwrite: marking twice is harmless.
          ConditionExpression: 'attribute_not_exists(sourceId)',
        }),
      );
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) return;
      throw err;
    }
  }
}
