import { UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import type { DeviceStore, PlaceStore } from '../../ports.js';
import type { PushTarget } from '../contracts.js';

/**
 * Builds the sender's onDeadTarget: a dead push token must stop being used.
 * - device store: forget the token (only Expo tokens live there; a no-op for native tokens)
 * - cell index: set `target` to null on every entry of this device (pk = h3Cell,
 *   sk = `${deviceId}#${placeId}`; the device's cells come from its places). UpdateItem only,
 *   guarded by attribute_exists so it can never create a stray row for a deleted place.
 */
export function createDeadTargetClearer(deps: {
  devices: Pick<DeviceStore, 'clearPushToken'>;
  places: Pick<PlaceStore, 'listForDevice'>;
  doc: Pick<DynamoDBDocumentClient, 'send'>;
  cellIndexTable: string;
}): (deviceId: string, target: PushTarget) => Promise<void> {
  return async (deviceId, target) => {
    if (target.platform === 'expo') await deps.devices.clearPushToken(target.token);
    const places = await deps.places.listForDevice(deviceId);
    await Promise.all(
      places.map(async (p) => {
        try {
          await deps.doc.send(
            new UpdateCommand({
              TableName: deps.cellIndexTable,
              Key: { pk: p.h3Cell, sk: `${deviceId}#${p.id}` },
              UpdateExpression: 'SET #target = :none',
              // Only clear the dead token: a newer registration must survive.
              ConditionExpression: 'attribute_exists(pk) AND #target.#token = :token',
              ExpressionAttributeNames: { '#target': 'target', '#token': 'token' },
              ExpressionAttributeValues: { ':none': null, ':token': target.token },
            }),
          );
        } catch (err) {
          if (!(err instanceof ConditionalCheckFailedException)) throw err;
        }
      }),
    );
  };
}
