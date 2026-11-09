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
 * - emergency, warning: wake people up (high priority, sound).
 * - watch, advisory: visible but quiet (normal priority, no sound).
 * - statement: informational only, never pushed (the planner returns refused
 *   'informational'). The alert still appears in the app.
 */
export const PUSH_POLICY: Record<RelaySeverity, PushPolicy> = {
  emergency: { push: true, priority: 'high', sound: 'default' },
  warning: { push: true, priority: 'high', sound: 'default' },
  watch: { push: true, priority: 'normal', sound: null },
  advisory: { push: true, priority: 'normal', sound: null },
  statement: { push: false, priority: 'normal', sound: null },
};
