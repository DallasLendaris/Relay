import { alertEndsAtMs, type Alert } from '@relay/shared';
import type { AlertStore, Clock } from '../ports.js';

export type ApplyResult = {
  action: 'created' | 'updated' | 'cancelled' | 'ignored';
  alert: Alert;
  reason?: string;
};

/**
 * True when a stored cancelled alert was ended by feed reconciliation, not by a CAP Cancel: its
 * last applied message is not a Cancel. A real Cancel for it is still applied (and pushed).
 */
export const endedWithoutCancel = (a: Alert): boolean =>
  a.state === 'cancelled' && a.messageType !== 'Cancel';

/**
 * Current state of an alert at `now`. Stored state only says what the latest NWS
 * message was; expiry is time-based, so it is computed on read.
 * Expired once `expires` (or `ends`, if set and later) has passed, unless cancelled.
 * An urgency-Past message ignores `ends` (see alertEndsAtMs).
 */
export function effectiveState(alert: Alert, now: Date): Alert['state'] {
  if (alert.state === 'cancelled') return 'cancelled';
  return now.getTime() > alertEndsAtMs(alert) ? 'expired' : alert.state;
}

const withoutPds = (a: Alert): Alert => {
  const { pds: _pds, ...rest } = a;
  return rest;
};

/**
 * Apply one normalized NWS message to the alert store.
 * Updates and cancels keep the Relay id of the record they refer to.
 */
export async function applyMessage(
  incoming: Alert,
  deps: { store: AlertStore; clock: Clock },
): Promise<ApplyResult> {
  const { store } = deps;
  const ignored = (alert: Alert, reason: string): ApplyResult => ({
    action: 'ignored',
    alert,
    reason,
  });

  const type = incoming.messageType;
  if (type !== 'Alert' && type !== 'Update' && type !== 'Cancel') {
    return ignored(incoming, `unsupported messageType ${type}`);
  }

  // Replay: this exact message (or an older one in a known chain) was already applied.
  const known = await store.findBySourceId(incoming.sourceId);
  if (known) {
    return ignored(known, 'already applied (replay)');
  }

  // Find the record this message refers to, if any.
  let stored: Alert | null = null;
  for (const ref of incoming.references) {
    stored = await store.findBySourceId(ref.sourceId);
    if (stored) break;
  }

  const isCancel = type === 'Cancel';

  if (!stored) {
    if (isCancel) return ignored(incoming, 'cancel for unknown alert');
    // New alert, or an update whose original we never saw.
    const created: Alert = { ...incoming, state: 'active' };
    await store.save(created);
    return { action: 'created', alert: created };
  }

  // Cancelled is final, except that a real Cancel replaces an end by feed reconciliation (which
  // pushed nothing), so devices that were sent the alert hear that it was cancelled.
  if (stored.state === 'cancelled' && !(isCancel && endedWithoutCancel(stored))) {
    return ignored(stored, 'alert already cancelled');
  }
  if (Date.parse(incoming.sent) < Date.parse(stored.sent)) {
    return ignored(stored, 'message is older than stored data');
  }

  // Keep the whole chain: earlier ids, the previous current message, then the new refs.
  const chain = [
    ...stored.references,
    { sourceId: stored.sourceId, sent: stored.sent },
    ...incoming.references,
  ];
  const references = chain.filter(
    (r, i) =>
      r.sourceId !== incoming.sourceId && chain.findIndex((c) => c.sourceId === r.sourceId) === i,
  );

  const supersedes = { sourceId: stored.sourceId, relaySeverity: stored.relaySeverity };
  const next: Alert = isCancel
    ? {
        // A Cancel ends the stored version: keep its area (a Cancel may carry no polygon or
        // zones), severity and PDS flag, so the record still says what was cancelled and where,
        // and follow-ups reach the devices that were sent it (docs/cancellation.md).
        ...withoutPds(incoming),
        ...(stored.pds === true ? { pds: true } : {}),
        id: stored.id,
        references,
        area: stored.area,
        relaySeverity: stored.relaySeverity,
        state: 'cancelled',
        supersedes,
      }
    : { ...incoming, id: stored.id, references, state: 'updated', supersedes };
  await store.save(next);
  return { action: isCancel ? 'cancelled' : 'updated', alert: next };
}
