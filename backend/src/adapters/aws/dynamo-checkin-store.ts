import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { GetCommand, PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { CheckIn } from '@relay/shared';
import type { CheckInStore } from '../../ports.js';
import type { StoredCheckIn } from '../../scale/checkins/types.js';

/**
 * CheckInStore on DynamoDB, built for a burst of a million check-ins in minutes.
 *
 * checkinsTable: partition key "pk" (S) = `${alertId}#${deviceId}`, no sort key. Every device is
 * its own partition, so no key is hot however many phones answer one alert. Attributes: alertId,
 * deviceId, h3Cell, checkedInAt, idempotencyKey. The table has a stream (NEW_IMAGE); the
 * aggregator in src/scale/checkins turns INSERT records into per-cell totals.
 *
 * The request path is ONE conditional PutItem: no transaction and no counter (a counter row per
 * alert was the old bottleneck, about 1,000 writes/s).
 */
export class DynamoCheckInStore implements CheckInStore {
  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly checkinsTable: string,
  ) {}

  /** Writes the check-in only if this device has not checked in for this alert. False = repeat. */
  async save(checkIn: CheckIn): Promise<boolean> {
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
}
