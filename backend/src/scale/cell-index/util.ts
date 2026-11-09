import type { Device } from '../../ports.js';
import type { PushTarget } from '../contracts.js';

/** Runs fn over items with at most `limit` calls in flight. Rejects on the first failure. */
export async function mapLimit<T>(
  items: readonly T[],
  limit: number,
  fn: (item: T) => Promise<void>,
): Promise<void> {
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < items.length) {
      const item = items[next++];
      if (item !== undefined) await fn(item);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
}

/** The device's working push target, or null. Falls back to the legacy Expo token. */
export function targetOf(
  device: Pick<Device, 'pushPlatform' | 'pushToken' | 'expoPushToken'>,
): PushTarget | null {
  if (device.pushPlatform && device.pushToken) {
    return { platform: device.pushPlatform, token: device.pushToken };
  }
  if (device.pushPlatform === undefined && device.expoPushToken) {
    return { platform: 'expo', token: device.expoPushToken };
  }
  return null;
}

export const sortKey = (deviceId: string, placeId: string): string => `${deviceId}#${placeId}`;

export const DEFAULT_PAGE_LIMIT = 1000;

export const encodeCursor = (value: unknown): string =>
  Buffer.from(JSON.stringify(value), 'utf8').toString('base64url');

export function decodeCursor(cursor: string): unknown {
  try {
    return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    throw new Error('invalid cell index cursor');
  }
}
