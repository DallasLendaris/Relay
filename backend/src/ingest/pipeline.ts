import { randomUUID } from 'node:crypto';
import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { Alert } from '@relay/shared';
import { SigningAlertStore } from '../adapters/signing-alert-store.js';
import type { Keyring } from '../config/secrets.js';
import { refusalFor } from '../delivery/eligibility.js';
import { planDelivery } from '../delivery/plan-delivery.js';
import type { Logger } from '../delivery/types.js';
import {
  H3_RESOLUTION,
  NwsZoneProvider,
  cellsForAlertArea,
  type ZoneGeometryProvider,
} from '../geo/index.js';
import { LayeredZoneProvider } from '../geo/layered-zone-provider.js';
import { S3ZoneProvider } from '../geo/s3-zone-provider.js';
import type { AlertStore, Clock, DeliveryQueue, DeviceStore, PlaceStore } from '../ports.js';
import { QUEUE_FOR_SEVERITY } from '../scale/contracts.js';
import { SqsPlanQueue, type PlanQueue } from '../scale/fanout/queues.js';
import { AlertPublisher, type PublishOptions } from '../scale/publish/alert-publisher.js';
import { S3PublicObjectWriter } from '../scale/publish/s3-object-writer.js';
import { createAlertVerifier } from '../signing/alertSignature.js';
import type { AlertSigning } from '../signing/memory.js';
import { parseFeed } from './feed.js';
import type { EngineDeps } from './handler.js';
import { applyMessage } from './lifecycle.js';
import { normalizeFeature } from './normalize.js';
import { DeliveryPausedError, type ActiveAlertLister, type Reconciler } from './poll.js';

export interface PipelineDeps {
  /** The real alert store (Postgres in AWS). It is wrapped so every save is signed. */
  alertStore: AlertStore;
  places: PlaceStore;
  devices: DeviceStore;
  queue: DeliveryQueue;
  /** Signs saved alerts (KMS in AWS) and the public keys that verify them before delivery. */
  alertSigning: AlertSigning;
  tokenKeys: Keyring;
  isDeliveryPaused(): Promise<boolean>;
  /** Used for zone-geometry lookups. */
  fetch: typeof fetch;
  userAgent: string;
  clock?: Clock;
  logger?: Logger;
  /**
   * Environment to read FANOUT_MODE and PLAN_QUEUE_URL from (default process.env). With
   * FANOUT_MODE=queue, onApplied publishes AlertReady to the plan queue instead of planning
   * in-line. Anything else keeps the original in-line planDelivery path.
   */
  env?: Record<string, string | undefined>;
  /** Replaces the SQS plan queue built from PLAN_QUEUE_URL (tests). */
  planQueue?: PlanQueue;
  /**
   * Builds the S3 client for the zone pack (no region) and for the public alerts bucket (its
   * region, PUBLIC_ALERTS_REGION). Default: a real S3Client. Tests inject fakes.
   */
  s3?: (region?: string) => S3Client;
  /** Limits for the post-AlertReady CDN publish (default: 16 in flight, 15 s total). */
  publishOptions?: PublishOptions;
}

/** CDN publish limits: shard index files in flight, and the total time cap for them. */
export const PUBLISH_CONCURRENCY = 16;
export const PUBLISH_DEADLINE_MS = 15_000;

/** Where the zone pack lives in ZONE_PACK_BUCKET (fixed by the pack builder). */
export const ZONE_PACK_KEY = 'zones/v1/california.json.gz';

const jsonLogger: Logger = {
  info: (msg, f) => console.log(JSON.stringify({ level: 'info', msg, ...f })),
  warn: (msg, f) => console.warn(JSON.stringify({ level: 'warn', msg, ...f })),
  error: (msg, f) => console.error(JSON.stringify({ level: 'error', msg, ...f })),
};

/** Wires normalize, geo, lifecycle, signing and delivery planning into the poll loop's engine deps. */
export function createIngestPipeline(deps: PipelineDeps): EngineDeps {
  const clock = deps.clock ?? { now: () => new Date() };
  const logger = deps.logger ?? jsonLogger;
  const verifyAlert = createAlertVerifier(deps.alertSigning.publicKeys);
  const env = deps.env ?? process.env;
  const queueMode = env.FANOUT_MODE === 'queue';
  let planQueue = deps.planQueue;
  if (queueMode && !planQueue) {
    if (!env.PLAN_QUEUE_URL) throw new Error('PLAN_QUEUE_URL must be set when FANOUT_MODE=queue');
    planQueue = new SqsPlanQueue(new SQSClient({}), env.PLAN_QUEUE_URL);
  }

  const s3For = deps.s3 ?? ((region?: string) => new S3Client(region ? { region } : {}));

  // Same fetch and User-Agent as the NWS client; NwsZoneProvider caches while warm.
  // The signal (5 s timeout from NwsZoneProvider) must reach fetch, or a hung zone lookup
  // could eat the whole Lambda budget.
  const nwsZones = new NwsZoneProvider((url, init) =>
    deps.fetch(url, {
      headers: { ...init?.headers, 'User-Agent': deps.userAgent },
      signal: init?.signal,
    }),
  );
  // With ZONE_PACK_BUCKET set, the pre-built pack answers first and NWS fills any miss or
  // pack failure. Without it, NWS only.
  const zones: ZoneGeometryProvider = env.ZONE_PACK_BUCKET
    ? new LayeredZoneProvider(
        [new S3ZoneProvider(env.ZONE_PACK_BUCKET, ZONE_PACK_KEY, s3For()), nwsZones],
        logger,
      )
    : nwsZones;

  // With PUBLIC_ALERTS_BUCKET set, every signed save is also published for the CDN. The bucket
  // may live in another region than the Lambda (CloudFront origins are often us-east-1).
  const publisher = env.PUBLIC_ALERTS_BUCKET
    ? new AlertPublisher({
        writer: new S3PublicObjectWriter(s3For(env.PUBLIC_ALERTS_REGION), env.PUBLIC_ALERTS_BUCKET),
        alerts: deps.alertStore,
        cellResolution: H3_RESOLUTION,
        logger,
        clock,
      })
    : undefined;
  const store = new SigningAlertStore(deps.alertStore, deps.alertSigning.signer);

  /** Publishes the alert JSON, then the shard indexes (bounded). Never throws. */
  async function publishPublic(saved: Alert): Promise<void> {
    if (!publisher) return;
    try {
      await publisher.publish(saved, {
        concurrency: PUBLISH_CONCURRENCY,
        deadlineMs: PUBLISH_DEADLINE_MS,
        ...deps.publishOptions,
      });
    } catch (err) {
      logger.error('publish.failed', {
        alertId: saved.id,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Queue mode: same checks and pause semantics as planDelivery, then hand the alert to the
   * planner. The alert counts as seen only after the publish succeeds (a throw leaves it
   * unseen, so the next poll publishes again; the planner and batch keys make repeats safe).
   */
  async function publishAlertReady(saved: Alert, queue: PlanQueue): Promise<void> {
    const refusal = refusalFor(saved, clock.now(), verifyAlert);
    if (refusal) {
      logger.error('refusing to publish alert', { alertId: saved.id, reason: refusal });
      logger.warn('plan refused', { alertId: saved.id, refused: refusal });
      return;
    }
    if (QUEUE_FOR_SEVERITY[saved.relaySeverity] === null) {
      logger.info('not pushing informational alert', { alertId: saved.id });
      return;
    }
    if (await deps.isDeliveryPaused()) {
      logger.warn('delivery paused', { alertId: saved.id });
      throw new DeliveryPausedError(saved.id);
    }
    await queue.publish({
      alertId: saved.id,
      sourceId: saved.sourceId,
      relaySeverity: saved.relaySeverity,
    });
    logger.info('alert ready published', { alertId: saved.id, severity: saved.relaySeverity });
  }

  // Feed reconciliation needs a store that can list every active NWS alert; the Postgres and
  // in-memory stores can. Without that method it is simply off.
  const lister = deps.alertStore as Partial<ActiveAlertLister>;
  const reconcile: Reconciler | undefined = lister.listActiveBySource
    ? {
        now: () => clock.now(),
        listActive: (now) => store.listActiveBySource('nws', now),
        async cancel(alert, reason) {
          logger.warn('alert.ended_by_reconciliation', {
            alertId: alert.id,
            sourceId: alert.sourceId,
            reason,
          });
          await store.save({ ...alert, state: 'cancelled' });
          // Reload so the public copy is exactly what was stored: signed, no summaries.
          const saved = await store.get(alert.id);
          if (saved) await publishPublic(saved);
        },
      }
    : undefined;

  return {
    ...(reconcile ? { reconcile } : {}),
    normalize: (feature) =>
      normalizeFeature(feature, { newId: randomUUID, warn: (m, f) => logger.warn(m, f) }),
    parseFeed,
    cellsFor: (area) => cellsForAlertArea(area, zones, { warn: (m, f) => logger.warn(m, f) }),
    apply: (alert) => applyMessage(alert, { store, clock }),
    async onApplied(result) {
      if (result.action === 'cancelled') {
        // Nothing to plan, but the alert must drop out of the public index.
        const saved = await store.get(result.alert.id);
        if (saved) await publishPublic(saved);
        return;
      }
      if (result.action !== 'created' && result.action !== 'updated') return;
      let saved: Alert | null = null;
      try {
        // Reload so delivery sees exactly what was stored: signed, with summaries.
        saved = await store.get(result.alert.id);
        if (!saved) {
          throw new Error(`saved alert ${result.alert.id} not found for delivery planning`);
        }
        if (queueMode && planQueue) {
          await publishAlertReady(saved, planQueue);
          return;
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
      } finally {
        // After AlertReady (or the inline plan): public data is never on the critical path.
        if (saved) await publishPublic(saved);
      }
    },
  };
}
