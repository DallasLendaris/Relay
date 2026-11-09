import { randomUUID } from 'node:crypto';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { Clock, IdempotencyStore } from '../../ports.js';

const DAY_SECONDS = 24 * 60 * 60;

/**
 * IdempotencyStore on DynamoDB. Table has partition key `pk` (string) and TTL
 * enabled on attribute `ttl` (epoch seconds), so old keys clean themselves up.
 * Item: { pk, state: 'leased' | 'sent', attemptId, leaseUntil (epoch ms, while leased), ttl }.
 * Each claim writes a fresh attemptId, remembered in this instance, so release and markSent
 * only touch the item if it is still this attempt's (a later attempt may own it after our
 * lease ran out). The port has no attempt parameter, hence the per-instance map.
 * Items written before leases existed have no `state`; they count as sent.
 */
export class DynamoIdempotencyStore implements IdempotencyStore {
  private readonly attempts = new Map<string, string>();

  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly tableName: string,
    private readonly clock: Clock = { now: () => new Date() },
  ) {}

  async claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'> {
    const attemptId = randomUUID();
    try {
      await this.doc.send(
        new PutCommand({
          TableName: this.tableName,
          Item: {
            pk: key,
            state: 'leased',
            attemptId,
            leaseUntil: leaseUntil.getTime(),
            // An abandoned lease is cleaned up a day after it runs out.
            ttl: Math.floor(leaseUntil.getTime() / 1000) + DAY_SECONDS,
          },
          // Win if nobody has the key, or the previous holder's lease ran out unsent.
          ConditionExpression:
            'attribute_not_exists(pk) OR (#state = :leased AND leaseUntil < :now)',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: { ':leased': 'leased', ':now': this.clock.now().getTime() },
        }),
      );
      this.attempts.set(key, attemptId);
      return 'claimed';
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
    }
    // Lost the race: find out whether it is already sent or another attempt is in flight.
    const { Item } = await this.doc.send(
      new GetCommand({ TableName: this.tableName, Key: { pk: key }, ConsistentRead: true }),
    );
    // Item gone in between (released): report 'leased' so SQS retries shortly.
    if (!Item || Item['state'] === 'leased') return 'leased';
    return 'sent';
  }

  async markSent(key: string, expiresAt: Date): Promise<void> {
    const attemptId = this.attempts.get(key);
    this.attempts.delete(key);
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: key },
          UpdateExpression: 'SET #state = :sent, #ttl = :ttl REMOVE leaseUntil',
          // Don't overwrite a key that a different attempt already marked sent.
          ConditionExpression:
            'attribute_not_exists(pk) OR #state <> :sent' +
            (attemptId ? ' OR attemptId = :attempt' : ''),
          ExpressionAttributeNames: { '#state': 'state', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':sent': 'sent',
            ':ttl': Math.floor(expiresAt.getTime() / 1000),
            ...(attemptId ? { ':attempt': attemptId } : {}),
          },
        }),
      );
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err; // already sent: fine
    }
  }

  async release(key: string): Promise<void> {
    const attemptId = this.attempts.get(key);
    this.attempts.delete(key);
    try {
      await this.doc.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: { pk: key },
          // Only give back a lease that is still ours; never delete a sent key or a newer lease.
          ConditionExpression: '#state = :leased' + (attemptId ? ' AND attemptId = :attempt' : ''),
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: {
            ':leased': 'leased',
            ...(attemptId ? { ':attempt': attemptId } : {}),
          },
        }),
      );
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err; // not ours any more
    }
  }
}
