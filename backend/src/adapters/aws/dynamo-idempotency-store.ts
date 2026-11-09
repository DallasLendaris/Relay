import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { IdempotencyStore } from '../../ports.js';

/**
 * IdempotencyStore on DynamoDB. Table has partition key `pk` (string) and TTL
 * enabled on attribute `ttl` (epoch seconds), so old keys clean themselves up.
 */
export class DynamoIdempotencyStore implements IdempotencyStore {
  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly tableName: string,
  ) {}

  async claim(key: string, expiresAt: Date): Promise<boolean> {
    try {
      await this.doc.send(
        new PutCommand({
          TableName: this.tableName,
          Item: { pk: key, ttl: Math.floor(expiresAt.getTime() / 1000) },
          // Only the first writer wins.
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      );
      return true;
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) return false;
      throw err;
    }
  }

  async release(_key: string): Promise<void> {
    // TODO(wave 4): implemented by the Postgres follow-up; the stub keeps the port contract compiling.
    throw new Error('not implemented yet');
  }
}
