import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import {
  BatchWriteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { CheckIn } from '@relay/shared';
import type { CheckInStore } from '../../ports.js';
import { checkInTtlSeconds, type StoredCheckIn } from '../../scale/checkins/types.js';

/** The GSI that lists a device's check-ins (partition key deviceId, keys only). */
export const BY_DEVICE_INDEX = 'byDevice';

/**
 * CheckInStore on DynamoDB, built for a burst of a million check-ins in minutes.
 *
 * checkinsTable: partition key "pk" (S) = `${alertId}#${deviceId}`, no sort key. Every device is
 * its own partition, so no key is hot however many phones answer one alert. Attributes: alertId,
 * deviceId, h3Cell, checkedInAt, idempotencyKey, ttl (epoch seconds: the alert's end plus the
 * retention period; DynamoDB TimeToLive deletes the row after it). The table has a stream
 * (NEW_IMAGE); the aggregator in src/scale/checkins turns INSERT records into per-cell totals.
 * GSI byDevice (partition key deviceId, keys only) lists one device's rows for erasure.
 *
 * The request path is ONE conditional PutItem: no transaction and no counter (a counter row per
 * alert was the old bottleneck, about 1,000 writes/s).
 */
export class DynamoCheckInStore implements CheckInStore {
  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly checkinsTable: string,
    /** Retention used when a caller does not pass the alert's end (see checkInTtlSeconds). */
    private readonly retentionDays?: number,
  ) {}

  /**
   * Writes the check-in only if this device has not checked in for this alert. False = repeat.
   * `retainUntil` is the alert's end plus the retention period. Without it (a caller that does
   * not know the alert's end) the row expires the default retention after the check-in itself.
   */
  async save(checkIn: CheckIn, retainUntil?: Date): Promise<boolean> {
    const ttl = retainUntil
      ? Math.floor(retainUntil.getTime() / 1000)
      : checkInTtlSeconds(new Date(checkIn.checkedInAt), this.retentionDays);
    try {
      await this.doc.send(
        new PutCommand({
          TableName: this.checkinsTable,
          Item: {
            pk: `${checkIn.alertId}#${checkIn.deviceId}`,
            alertId: checkIn.alertId,
            deviceId: checkIn.deviceId,
            h3Cell: checkIn.h3Cell,
            checkedInAt: checkIn.checkedInAt,
            idempotencyKey: checkIn.idempotencyKey,
            ttl,
          },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      );
      return true;
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) return false;
      throw err;
    }
  }

  /** The device's stored check-in for this alert, if any. Not part of the port. */
  async get(alertId: string, deviceId: string): Promise<StoredCheckIn | null> {
    const out = await this.doc.send(
      new GetCommand({
        TableName: this.checkinsTable,
        Key: { pk: `${alertId}#${deviceId}` },
        ConsistentRead: true, // the caller just lost a write race; it must see the winner
      }),
    );
    const item = out.Item;
    if (!item) return null;
    return {
      alertId: String(item['alertId']),
      deviceId: String(item['deviceId']),
      h3Cell: String(item['h3Cell']),
      checkedInAt: String(item['checkedInAt']),
      idempotencyKey: String(item['idempotencyKey']),
    };
  }

  /**
   * Erases every check-in of one device (account deletion): lists the keys through the byDevice
   * index (a query by deviceId, never a table scan) and deletes them 25 at a time. Items the
   * table could not take (UnprocessedItems) are retried a few times; after that it throws, and
   * the caller retries the whole erasure (it is idempotent). Returns how many rows it deleted.
   * The index is eventually consistent, so a check-in written a moment ago can be missed; its
   * TTL still removes it.
   */
  async deleteForDevice(deviceId: string): Promise<number> {
    let deleted = 0;
    let startKey: Record<string, unknown> | undefined;
    do {
      const page = await this.doc.send(
        new QueryCommand({
          TableName: this.checkinsTable,
          IndexName: BY_DEVICE_INDEX,
          KeyConditionExpression: 'deviceId = :d',
          ExpressionAttributeValues: { ':d': deviceId },
          ProjectionExpression: 'pk',
          ...(startKey ? { ExclusiveStartKey: startKey } : {}),
        }),
      );
      const keys = (page.Items ?? []).map((i) => String(i['pk']));
      for (let i = 0; i < keys.length; i += 25) {
        let pending: Array<{ DeleteRequest: { Key: { pk: string } } }> = keys
          .slice(i, i + 25)
          .map((pk) => ({ DeleteRequest: { Key: { pk } } }));
        for (let attempt = 0; pending.length > 0; attempt++) {
          if (attempt >= 5) throw new Error('check-in delete left unprocessed items');
          const out = await this.doc.send(
            new BatchWriteCommand({ RequestItems: { [this.checkinsTable]: pending } }),
          );
          const left = (out.UnprocessedItems?.[this.checkinsTable] ?? []) as typeof pending;
          deleted += pending.length - left.length;
          pending = left;
        }
      }
      startKey = page.LastEvaluatedKey;
    } while (startKey);
    return deleted;
  }
}
