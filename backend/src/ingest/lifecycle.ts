import type { Alert } from '@relay/shared';
import type { AlertStore, Clock } from '../ports.js';

export type ApplyResult = {
  action: 'created' | 'updated' | 'cancelled' | 'ignored';
  alert: Alert;
  reason?: string;
};

/**
 * Current state of an alert at `now`. Stored state only says what the latest NWS
 * message was; expiry is time-based, so it is computed on read.
 * Expired once `expires` (or `ends`, if set and later) has passed, unless cancelled.
 */
export function effectiveState(alert: Alert, now: Date): Alert['state'] {
  if (alert.state === 'cancelled') return 'cancelled';
  const times = [alert.expires, alert.ends].filter((t): t is string => t !== null);
  const end = Math.max(...times.map((t) => Date.parse(t)));
  return now.getTime() > end ? 'expired' : alert.state;
}

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

  if (stored.state === 'cancelled') {
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

  const next: Alert = {
    ...incoming,
    id: stored.id,
    references,
    state: isCancel ? 'cancelled' : 'updated',
  };
  await store.save(next);
  return { action: isCancel ? 'cancelled' : 'updated', alert: next };
}
