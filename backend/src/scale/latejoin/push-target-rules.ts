import { createHash } from 'node:crypto';
import type { PushTarget } from '../contracts.js';

/**
 * Push-target move rules (SEC-A-1 / SEC-B-1).
 *
 * One phone can register unlimited anonymous devices (POST /devices has no proof of presence) and
 * move its single push token onto each of them. Each device is a check-in identity, so without a
 * limit one phone inflates a cell's check-in total. A push token that is attached to ANOTHER
 * device may move only when
 *   - that device has had no push-target update for INACTIVE_MS (an abandoned install), or
 *   - at most MAX_MOVES_PER_WINDOW moves of this token happened in the last MOVE_WINDOW_MS
 *     (counting this one).
 * Otherwise the move is refused with 409 `push_token_in_use`.
 *
 * Reinstall trade-off: a real reinstall is one move (the old install is gone and the new device
 * takes the token), so it always fits in the allowance. A person who reinstalls the app three or
 * more times in one day, or who restores a backup onto a second phone while the first is in
 * active use, gets 409 until the 24 hour window passes. Their old device keeps its token and
 * keeps receiving alerts, so the failure is safe: nobody loses delivery.
 *
 * Residual: two moves of one token that run at the same instant both read the same count, so a
 * burst can overshoot the allowance by the number of concurrent requests. It is still bounded
 * (a handful), and the per-alert check-in token claim in the late-join Lambda caps the late-join
 * tokens at one per push target regardless.
 */
export const MOVE_WINDOW_MS = 24 * 60 * 60 * 1000;
export const MAX_MOVES_PER_WINDOW = 2;
export const INACTIVE_MS = 24 * 60 * 60 * 1000;

/** Thrown by DeviceStore.assignPushTarget when the move is refused. The API answers 409. */
export class PushTokenInUseError extends Error {
  constructor() {
    super('push token is attached to another active device');
    this.name = 'PushTokenInUseError';
  }
}

/** Stable, non-reversible id of one physical push target. Never log the token itself. */
export const pushTargetHash = (target: PushTarget): string =>
  createHash('sha256')
    .update(target.platform + target.token)
    .digest('hex');

/**
 * The rule, shared by the memory store (the Postgres store applies the same rule in SQL).
 * `holdersLastUpdate` is the last push-target update (or creation) time of each OTHER device
 * that currently holds the token, as epoch milliseconds. `recentMoves` counts moves of this
 * token in the last MOVE_WINDOW_MS, not counting the one being decided.
 */
export function moveAllowed(input: {
  now: number;
  holdersLastUpdate: readonly number[];
  recentMoves: number;
}): boolean {
  if (input.holdersLastUpdate.length === 0) return true; // nothing to take it from
  if (input.recentMoves < MAX_MOVES_PER_WINDOW) return true;
  return input.holdersLastUpdate.every((t) => t <= input.now - INACTIVE_MS);
}

/** When a device's push target last changed; implemented by both device stores. */
export interface PushTargetMeta {
  /** Every assignPushTarget call (the inactivity rule reads this). */
  updatedAt: Date;
  /** Only when the target value changed, or went from none to some. */
  changedAt: Date;
}
