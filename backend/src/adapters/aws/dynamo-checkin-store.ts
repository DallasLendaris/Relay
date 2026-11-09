import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import {
  GetCommand,
  TransactWriteCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { CheckInSchema, type CheckIn } from '@relay/shared';
import type { CheckInStore } from '../../ports.js';

/**
 * CheckInStore on DynamoDB.
 * - checkinsTable: pk "alertId" (S), sk "deviceId" (S). One item per device per alert; idempotencyKey is a plain attribute.
 * - totalsTable: pk "alertId" (S), sk "h3Cell" (S), numeric "checkedIn". The agency view reads it.
 */
export class DynamoCheckInStore implements CheckInStore {
  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly checkinsTable: string,
    private readonly totalsTable: string,
  ) {}

  /**
   * One transaction: record the check-in (only if this device has not checked in for this alert) and add 1 to the cell total.
   * Doing both together means a device is counted at most once, and never half-recorded.
   */
  async save(checkIn: CheckIn): Promise<boolean> {
    try {
      await this.doc.send(
        new TransactWriteCommand({
          TransactItems: [
            {
              Put: {
                TableName: this.checkinsTable,
                Item: { ...checkIn },
                ConditionExpression: 'attribute_not_exists(alertId)',
              },
            },
            {
              Update: {
                TableName: this.totalsTable,
                Key: { alertId: checkIn.alertId, h3Cell: checkIn.h3Cell },
                UpdateExpression: 'ADD checkedIn :one',
                ExpressionAttributeValues: { ':one': 1 },
              },
            },
          ],
        }),
      );
      return true;
    } catch (err) {
      // CancellationReasons line up with TransactItems: index 0 is the Put. Any other
      // reason (throttling, conflict with another transaction) is a real error to retry.
      if (
        err instanceof TransactionCanceledException &&
        err.CancellationReasons?.[0]?.Code === 'ConditionalCheckFailed'
      ) {
        return false;
      }
      throw err;
    }
  }

  /** The device's stored check-in for this alert, if any. Not part of the port. */
  async get(alertId: string, deviceId: string): Promise<CheckIn | null> {
    const out = await this.doc.send(
      new GetCommand({
        TableName: this.checkinsTable,
        Key: { alertId, deviceId },
        ConsistentRead: true, // the caller just lost a write race; it must see the winner
      }),
    );
    return out.Item ? CheckInSchema.parse(out.Item) : null;
  }
}
