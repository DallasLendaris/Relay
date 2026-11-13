import type { Alert } from '@relay/shared';
import { DENSE_PARTS } from '../contracts.js';
import { bucketFor, densePartFor } from './hash.js';

type AlertKey = Pick<Alert, 'id' | 'relaySeverity'>;

/** The key of a normal cell's bucket, which is also the tombstone of a dense cell's bucket. */
export const bucketKey = (alert: AlertKey, h3Cell: string, bucket: number): string =>
  `${alert.id}:${alert.relaySeverity}:${h3Cell}:${bucket}`;

/** The key of one part of one bucket of a dense cell. */
export const densePartKey = (
  alert: AlertKey,
  h3Cell: string,
  bucket: number,
  part: number,
): string => `${bucketKey(alert, h3Cell, bucket)}.${part}/${DENSE_PARTS}`;

/** Marks that a dense plan (not a normal batch) owns this bucket's tombstone. */
export const denseMarkerKey = (normalKey: string): string => `${normalKey}:dense`;

/**
 * Marks that this cell went dense for this alert version. Written before any bucket tombstone;
 * once it exists the cell stays dense for every later plan of the version (shard-worker.ts).
 */
export const denseCellKey = (alert: AlertKey, h3Cell: string): string =>
  `${alert.id}:${alert.relaySeverity}:${h3Cell}:dense`;

/**
 * The shard worker's record that it has decided a cell's batches for this alert version (G5): a
 * normal cell has one, `...:${h3Cell}:plan`; a dense cell one per part, `...:${h3Cell}:plan.${part}`.
 * It carries the planned recipients as a member set (member-set.ts), so late join knows whether
 * the planned read can still include a device (no record yet) or did not (not in the set).
 */
export const planMarkerKey = (alert: AlertKey, h3Cell: string, part?: number): string =>
  `${alert.id}:${alert.relaySeverity}:${h3Cell}:plan${part === undefined ? '' : `.${part}`}`;

/**
 * Marks that the batch with this key was over the byte budget and went out as `<key>:s<path>`
 * pieces (pack.ts splitToFit), so the key itself is never claimed. The shard worker writes it
 * before the pieces are enqueued.
 */
export const splitMarkerKey = (batchKey: string): string => `${batchKey}:split`;

/**
 * The batch key that carries this device in its cell: the normal (cell, bucket) key, or, when that
 * key is a dense tombstone, the dense part key. Used by late join to ask "was it planned?". A key
 * that was split into pieces reads as planned ('sent') through its split marker.
 */
export async function plannedKeyFor(
  alert: AlertKey,
  h3Cell: string,
  deviceId: string,
  peek: (key: string) => Promise<'absent' | 'leased' | 'sent'>,
): Promise<{ key: string; state: 'absent' | 'leased' | 'sent' }> {
  const bucket = bucketFor(deviceId);
  const normal = bucketKey(alert, h3Cell, bucket);
  const state = await peek(normal);
  if (state === 'absent') return withSplit(normal, peek);
  // A sent or leased normal key may be a dense tombstone (or one being written): then the device
  // is carried by its part, and only that part's state says whether it was planned.
  if ((await peek(denseMarkerKey(normal))) === 'sent') {
    const key = densePartKey(alert, h3Cell, bucket, densePartFor(deviceId));
    const partState = await peek(key);
    return partState === 'absent' ? withSplit(key, peek) : { key, state: partState };
  }
  return { key: normal, state };
}

/** An absent key whose batch was split is planned: its pieces carry the device. */
async function withSplit(
  key: string,
  peek: (key: string) => Promise<'absent' | 'leased' | 'sent'>,
): Promise<{ key: string; state: 'absent' | 'leased' | 'sent' }> {
  return (await peek(splitMarkerKey(key))) === 'sent'
    ? { key, state: 'sent' }
    : { key, state: 'absent' };
}
