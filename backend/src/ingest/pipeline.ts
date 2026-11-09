import { randomUUID } from 'node:crypto';
import { SigningAlertStore } from '../adapters/signing-alert-store.js';
import type { Keyring } from '../config/secrets.js';
import { planDelivery } from '../delivery/plan-delivery.js';
import type { Logger } from '../delivery/types.js';
import { NwsZoneProvider, cellsForAlertArea } from '../geo/index.js';
import type { AlertStore, Clock, DeliveryQueue, DeviceStore, PlaceStore } from '../ports.js';
import { createAlertVerifier } from '../signing/index.js';
import { parseFeed } from './feed.js';
import type { EngineDeps } from './handler.js';
import { applyMessage } from './lifecycle.js';
import { normalizeFeature } from './normalize.js';
import { DeliveryPausedError } from './poll.js';

export interface PipelineDeps {
  /** The real alert store (Postgres in AWS). It is wrapped so every save is signed. */
  alertStore: AlertStore;
  places: PlaceStore;
  devices: DeviceStore;
  queue: DeliveryQueue;
  alertKeys: Keyring;
  tokenKeys: Keyring;
  isDeliveryPaused(): Promise<boolean>;
  /** Used for zone-geometry lookups. */
  fetch: typeof fetch;
  userAgent: string;
  clock?: Clock;
  logger?: Logger;
}

const jsonLogger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

/** Wires normalize, geo, lifecycle, signing and delivery planning into the poll loop's engine deps. */
export function createIngestPipeline(deps: PipelineDeps): EngineDeps {
  const clock = deps.clock ?? { now: () => new Date() };
  const logger = deps.logger ?? jsonLogger;
  const store = new SigningAlertStore(deps.alertStore, deps.alertKeys.current);
  const verifyAlert = createAlertVerifier(deps.alertKeys.all);

  // Same fetch and User-Agent as the NWS client; NwsZoneProvider caches while warm.
  // The signal (5 s timeout from NwsZoneProvider) must reach fetch, or a hung zone lookup
  // could eat the whole Lambda budget.
  const zones = new NwsZoneProvider((url, init) =>
    deps.fetch(url, {
      headers: { ...init?.headers, 'User-Agent': deps.userAgent },
      signal: init?.signal,
    }),
  );

  return {
    normalize: (feature) =>
      normalizeFeature(feature, { newId: randomUUID, warn: (m, f) => logger.warn(m, f) }),
    parseFeed,
    cellsFor: (area) => cellsForAlertArea(area, zones, { warn: (m, f) => logger.warn(m, f) }),
    apply: (alert) => applyMessage(alert, { store, clock }),
    async onApplied(result) {
      if (result.action !== 'created' && result.action !== 'updated') return;
      try {
        // Reload so delivery sees exactly what was stored: signed, with summaries.
        const saved = await store.get(result.alert.id);
        if (!saved) {
          throw new Error(`saved alert ${result.alert.id} not found for delivery planning`);
        }
        // Re-planning is safe: dedupe happens in the delivery worker at send time
        // (claim alert:severity:device), so a retried plan never double-notifies.
        const plan = await planDelivery(saved, {
          places: deps.places,
          devices: deps.devices,
          queue: deps.queue,
          tokenKey: deps.tokenKeys.current,
          verifyAlert,
          isDeliveryPaused: deps.isDeliveryPaused,
          clock,
          logger,
        });
        if (plan.refused === 'paused') {
          // Not final: keep the alert unseen so it is planned again once delivery is unpaused.
          throw new DeliveryPausedError(saved.id);
        }
        // Every other refusal (unsigned, not_actual, inactive, expired, informational) is final:
        // log it and let pollOnce mark the alert seen.
        if (plan.refused) logger.warn('plan refused', { alertId: saved.id, ...plan });
        else logger.info('plan result', { alertId: saved.id, ...plan });
      } catch (err) {
        if (err instanceof DeliveryPausedError) throw err; // expected; poll.ts logs it as a warning
        // Log, then re-throw: pollOnce counts the error and does NOT mark the alert seen, so
        // the next poll plans delivery again.
        logger.error('delivery planning failed', { alertId: result.alert.id, error: String(err) });
        throw err;
      }
    },
  };
}
