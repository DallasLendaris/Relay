import type { Alert } from '@relay/shared';
import { refusalFor, type Refusal } from '../../delivery/eligibility.js';
import type { Logger } from '../../delivery/types.js';
import type { AlertStore, Clock } from '../../ports.js';
import { QUEUE_FOR_SEVERITY, type SendQueueName } from '../contracts.js';

/** Why a planner or shard worker did nothing for a message. */
export type FanoutRefusal = Refusal | 'paused' | 'informational' | 'stale';

export interface EligibilityDeps {
  alerts: Pick<AlertStore, 'get'>;
  verifyAlert(alert: Alert): boolean;
  isDeliveryPaused(): Promise<boolean>;
  clock: Clock;
  logger: Logger;
}

export type EligibilityOutcome =
  { ok: true; alert: Alert; queue: SendQueueName } | { ok: false; refused: FanoutRefusal };

/**
 * Loads the alert and decides whether it may be fanned out right now. Used by the planner and,
 * once more, by every shard worker (defense in depth: the alert may have been cancelled or the
 * kill switch flipped since the planner ran).
 *
 * Refusals are returned, never thrown. A missing alert or a failed read throws, so the queue
 * message is retried.
 */
export async function checkEligibility(
  message: { alertId: string; sourceId: string },
  deps: EligibilityDeps,
  stage: 'planner' | 'shard',
): Promise<EligibilityOutcome> {
  const alert = await deps.alerts.get(message.alertId);
  if (!alert) throw new Error(`alert ${message.alertId} not found`);

  if (alert.sourceId !== message.sourceId) {
    // A newer version was saved after this message was published; it has its own message.
    deps.logger.warn('ignoring stale fan-out message', {
      stage,
      alertId: alert.id,
      messageSourceId: message.sourceId,
      currentSourceId: alert.sourceId,
    });
    return { ok: false, refused: 'stale' };
  }

  const refusal = refusalFor(alert, deps.clock.now(), deps.verifyAlert);
  if (refusal) {
    deps.logger.error('refusing to fan out alert', { stage, alertId: alert.id, reason: refusal });
    return { ok: false, refused: refusal };
  }

  const queue = QUEUE_FOR_SEVERITY[alert.relaySeverity];
  if (queue === null) {
    deps.logger.info('not pushing informational alert', { stage, alertId: alert.id });
    return { ok: false, refused: 'informational' };
  }

  if (await deps.isDeliveryPaused()) {
    deps.logger.warn('delivery paused', { stage, alertId: alert.id });
    return { ok: false, refused: 'paused' };
  }
  return { ok: true, alert, queue };
}
