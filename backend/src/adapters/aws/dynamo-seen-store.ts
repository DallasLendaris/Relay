import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { GetCommand, PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { EndedMarker, SeenStore } from '../../ports.js';

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

  /** Reconciliation records share the table; the key cannot collide with a NWS source id. */
  private endedKey(alertId: string): string {
    return `ended#${alertId}`;
  }

  async markEnded(alertId: string, marker: EndedMarker): Promise<void> {
    await this.db.send(
      new PutCommand({
        TableName: this.table,
        Item: {
          sourceId: this.endedKey(alertId),
          ...marker,
          restored: false,
          expiresAt: Math.floor(this.nowMs() / 1000) + TTL_SECONDS,
        },
      }),
    );
  }

  async getEnded(alertId: string): Promise<EndedMarker | null> {
    const res = await this.db.send(
      new GetCommand({
        TableName: this.table,
        Key: { sourceId: this.endedKey(alertId) },
        ConsistentRead: true,
      }),
    );
    const item = res.Item;
    if (!item || item['restored'] === true || item['endedBy'] !== 'feed_absence') return null;
    const priorState = item['priorState'] === 'updated' ? 'updated' : 'active';
    return { endedBy: 'feed_absence', priorState, endedAt: String(item['endedAt'] ?? '') };
  }

  /** Overwrites the record as restored (a delete would need another IAM action). */
  async clearEnded(alertId: string): Promise<void> {
    await this.db.send(
      new PutCommand({
        TableName: this.table,
        Item: {
          sourceId: this.endedKey(alertId),
          restored: true,
          expiresAt: Math.floor(this.nowMs() / 1000) + TTL_SECONDS,
        },
      }),
    );
  }
}
