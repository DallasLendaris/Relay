import type { CellIndex, CellIndexEntry, PushTarget } from '../contracts.js';

/**
 * What Aurora (the record) says about one device right now. `load` returns null when the device
 * no longer exists.
 */
export interface DeviceTruth {
  target: PushTarget | null;
  language: 'en' | 'es';
  places: Array<{ id: string; h3Cell: string }>;
}

export interface SyncDeviceCellsInput {
  deviceId: string;
  /** Reads the device and its places from Aurora. Called again after the writes to verify. */
  load(): Promise<DeviceTruth | null>;
  /** A place that was just added: its entry is written FIRST. */
  added?: { placeId: string };
  /** A place that was just removed: its entry is deleted LAST. */
  removed?: { h3Cell: string; placeId: string };
  /** Passes before giving up (a concurrent change keeps invalidating the read). Default 3. */
  maxPasses?: number;
}

/** The sorted, de-duplicated cells of a device's places: the `deviceCells` value. */
export function deviceCellsOf(places: ReadonlyArray<{ h3Cell: string }>): string[] {
  return [...new Set(places.map((p) => p.h3Cell))].sort();
}

/** Places plus push target and language: a change in any of them invalidates a pass. */
const signature = (t: DeviceTruth): string =>
  [
    t.target ? `${t.target.platform}:${t.target.token}` : '',
    t.language,
    ...t.places.map((p) => `${p.id}@${p.h3Cell}`).sort(),
  ].join('|');

/**
 * Makes every cell-index entry of one device carry the device's current `deviceCells`, in an order
 * that keeps the primary-cell rule safe at every intermediate step.
 *
 *   Invariant: a cell named in any entry's deviceCells has a live entry for this device.
 *
 *   add place     1. put the NEW entry (its deviceCells already include the new cell)
 *                 2. rewrite deviceCells on the device's other entries
 *   remove place  1. rewrite deviceCells on the remaining entries WITHOUT the removed cell
 *                 2. delete the removed entry
 *   repair/backfill (no added/removed): rewrite all entries from the truth.
 *
 * Why this is safe: an entry sends only if its own cell is the smallest area cell of ITS OWN list.
 * Take the smallest area cell that has a live entry of this device. By the invariant, no entry's
 * list can name a smaller area cell (that cell would need an entry), so that entry sends: at
 * least one entry always sends. The only imperfect states are lists that disagree (add step 1 to
 * 2, remove step 1 to 2), and disagreement can only make two entries both think they are primary,
 * so the worst case is a duplicate, never a miss.
 *
 * Residual window: two requests changing the same device at the same moment (an add and a remove
 * within milliseconds) each write from their own read of Aurora. A stale pass writes the entries
 * AND lists for every place in its read, so a cell it names always gets an entry (no miss); but it
 * can re-create the entry of a place the other request just deleted (a stale entry: briefly one
 * extra push for a removed place, or a duplicate). Each pass therefore re-reads Aurora after
 * writing and, when the device's places, push target or language changed, repeats with the fresh
 * truth and deletes entries it wrote for places that are gone. When it cannot settle in
 * `maxPasses` it throws and the caller enqueues a repair (which converges by the same method).
 * Non-concurrent changes, the normal case, never see this window.
 *
 * Writes are `put`s of whole entries from the truth (target and language included), so they are
 * idempotent. Throws when any index call or load fails; callers enqueue a repair message.
 */
export async function syncDeviceCells(
  index: Pick<CellIndex, 'put' | 'remove'>,
  input: SyncDeviceCellsInput,
): Promise<void> {
  const maxPasses = input.maxPasses ?? 3;
  /** Entries this call wrote in earlier passes (placeId -> cell), to undo a stale write. */
  const written = new Map<string, string>();
  for (let pass = 1; pass <= maxPasses; pass++) {
    const truth = await input.load();
    if (!truth) {
      // The device is gone (erased), and its places with it (cascade). Whatever an earlier pass of
      // this call wrote for it goes too: erasure racing a place add must not leave an entry that
      // points at a deleted device.
      for (const [placeId, h3Cell] of written) {
        await index.remove(h3Cell, input.deviceId, placeId);
      }
      written.clear();
      if (input.removed) {
        await index.remove(input.removed.h3Cell, input.deviceId, input.removed.placeId);
      }
      return;
    }
    const deviceCells = deviceCellsOf(truth.places);
    const entryFor = (p: { id: string; h3Cell: string }): CellIndexEntry => ({
      h3Cell: p.h3Cell,
      deviceId: input.deviceId,
      placeId: p.id,
      target: truth.target ? { ...truth.target } : null,
      language: truth.language,
      deviceCells: [...deviceCells],
    });
    const first = truth.places.filter((p) => p.id === input.added?.placeId);
    const rest = truth.places.filter((p) => p.id !== input.added?.placeId);
    if (first.length > 0) await index.put(first.map(entryFor));
    if (rest.length > 0) await index.put(rest.map(entryFor));
    // A place written by an earlier pass that Aurora no longer has (it was removed while we
    // wrote from a stale read): delete that entry too, after the lists were rewritten.
    const liveIds = new Set(truth.places.map((p) => p.id));
    for (const [placeId, h3Cell] of written) {
      if (!liveIds.has(placeId)) {
        await index.remove(h3Cell, input.deviceId, placeId);
        written.delete(placeId);
      }
    }
    for (const p of truth.places) written.set(p.id, p.h3Cell);
    // Delete last, and only when Aurora really no longer has the place.
    if (input.removed && !truth.places.some((p) => p.id === input.removed?.placeId)) {
      await index.remove(input.removed.h3Cell, input.deviceId, input.removed.placeId);
    }
    const after = await input.load();
    if (after && signature(after) === signature(truth)) return;
  }
  throw new Error('device places kept changing during cell index sync');
}
