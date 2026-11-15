import type { Alert } from '@relay/shared';

export type RelaySeverity = Alert['relaySeverity'];

export interface PushPolicy {
  /** False means no push at all. The alert still shows in the app. */
  push: boolean;
  priority: 'high' | 'normal';
  /** 'default' plays the device sound; null sends a quiet notification. */
  sound: 'default' | null;
}

/**
 * Product decision: how loud each severity is.
 * - emergency, warning: wake people up (high priority, sound). A Tornado or Flash Flood Warning
 *   with NWS damage threat CATASTROPHIC is an emergency; CONSIDERABLE (PDS) stays a warning.
 * - watch, advisory: visible but quiet (normal priority, no sound).
 * - statement: informational only, never pushed (the planner returns refused
 *   'informational'). The alert still appears in the app. The exception is a statement with
 *   Immediate urgency and Severe or Extreme CAP severity: relaySeverityFor (shared) promotes it to
 *   advisory when the alert is ingested, so it gets the quiet push of an advisory.
 */
export const PUSH_POLICY: Record<RelaySeverity, PushPolicy> = {
  emergency: { push: true, priority: 'high', sound: 'default' },
  warning: { push: true, priority: 'high', sound: 'default' },
  watch: { push: true, priority: 'normal', sound: null },
  advisory: { push: true, priority: 'normal', sound: null },
  statement: { push: false, priority: 'normal', sound: null },
};
