import { randomUUID } from 'node:crypto';
import { SigningAlertStore } from '../adapters/signing-alert-store.js';
import type { Keyring } from '../config/secrets.js';
import { planDelivery } from '../delivery/plan-delivery.js';
import type { Logger } from '../delivery/types.js';
import { NwsZoneProvider, cellsForAlertArea } from '../geo/index.js';
import type {
  AlertStore,
  Clock,
  DeliveryQueue,
  DeviceStore,
  IdempotencyStore,
  PlaceStore,
} from '../ports.js';
import { createAlertVerifier } from '../signing/index.js';
import { parseFeed } from './feed.js';
import type { EngineDeps } from './handler.js';
import { applyMessage } from './lifecycle.js';
import { normalizeFeature } from './normalize.js';

export interface PipelineDeps {
  /** The real alert store (Postgres in AWS). It is wrapped so every save is signed. */
  alertStore: AlertStore;
  places: PlaceStore;
  devices: DeviceStore;
  idempotency: IdempotencyStore;
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
  const zones = new NwsZoneProvider((url, init) =>
    deps.fetch(url, { headers: { ...init?.headers, 'User-Agent': deps.userAgent } }),
  );

  return {
    normalize: (feature) => normalizeFeature(feature, { newId: randomUUID }),
    parseFeed,
    cellsFor: (area) => cellsForAlertArea(area, zones),
    apply: (alert) => applyMessage(alert, { store, clock }),
    async onApplied(result) {
      if (result.action !== 'created' && result.action !== 'updated') return;
      try {
        // Reload so delivery sees exactly what was stored: signed, with summaries.
        const saved = await store.get(result.alert.id);
        if (!saved) {
          logger.error('saved alert not found', { alertId: result.alert.id });
          return;
        }
        const plan = await planDelivery(saved, {
          places: deps.places,
          devices: deps.devices,
          idempotency: deps.idempotency,
          queue: deps.queue,
          tokenKey: deps.tokenKeys.current,
          verifyAlert,
          isDeliveryPaused: deps.isDeliveryPaused,
          clock,
          logger,
        });
        logger.info('plan result', { alertId: saved.id, ...plan });
      } catch (err) {
        // A delivery problem must not break polling; the alert is already stored.
        logger.error('delivery planning failed', { alertId: result.alert.id, error: String(err) });
      }
    },
  };
}
