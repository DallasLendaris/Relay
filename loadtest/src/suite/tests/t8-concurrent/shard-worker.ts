/**
 * t8/t9 child: a shard worker (stands in for the shard Lambdas). Runs the REAL processShard over
 * the on-disk seed (plus, in t9, the overlay of devices the API registered), with the REAL fan-out
 * SqsSendQueue writing to the broker. Several jobs run at once per process, like several Lambda
 * instances sharing a core. Holds every alert of the run (the planner decides which one a job is
 * for).
 *
 * Control: init, scenario, shard, stop.
 */
import { readFileSync } from 'node:fs';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import type { CellIndexEntry, ShardJob } from '../../../../../backend/src/scale/contracts.js';
import { CachedAlertReader } from '../../../../../backend/src/scale/fanout/alert-cache.js';
import { SqsSendQueue } from '../../../../../backend/src/scale/fanout/queues.js';
import { processShard } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import type { AlertVerifier } from '../../../../../backend/src/signing/index.js';
import { createCheckInToken } from '../../../../../backend/src/tokens/checkInToken.js';
import { TOKEN_KEY, verifierFromPem } from '../t2-fanout/alert.js';
import { DiskCellIndex, SeedReader } from '../t2-fanout/seed-data.js';
import { BrokerClient, QUEUE_URLS, fakeSqs } from '../t2-fanout/wire.js';
import { call } from '../t5-chaos/common.js';
import { runCtlWorker } from './ctl.js';
import { LayeredCellIndex } from './layered-index.js';

runCtlWorker(async () => {
  let broker: BrokerClient | undefined;
  let verify: AlertVerifier | undefined;
  let alerts = new CachedAlertReader(new InMemoryAlertStore());
  let reader: SeedReader | undefined;
  let index: LayeredCellIndex | undefined;
  const logEvents = new Map<string, number>();
  const count = (msg: string): void => void logEvents.set(msg, (logEvents.get(msg) ?? 0) + 1);
  const logger: Logger = { info: () => undefined, warn: count, error: count };
  let jobs = 0;

  return {
    handlers: {
      init: async (p: { brokerPort: number; publicPem: string }) => {
        broker = await BrokerClient.connect(p.brokerPort);
        verify = verifierFromPem(p.publicPem);
        return { ok: true };
      },
      scenario: async (p: {
        alertsFile: string;
        seedDir: string;
        fcmProjects: string[];
        /** Control URL of the API process whose overlay entries the shards must also read. */
        overlay?: string;
      }) => {
        const store = new InMemoryAlertStore();
        for (const a of JSON.parse(readFileSync(p.alertsFile, 'utf8')) as Alert[]) {
          await store.save(a);
        }
        alerts = new CachedAlertReader(store);
        if (reader?.dir !== p.seedDir) {
          await reader?.close();
          reader = new SeedReader(p.seedDir);
        }
        const overlayBase = p.overlay;
        index = new LayeredCellIndex(
          new DiskCellIndex(reader, p.fcmProjects),
          overlayBase
            ? async (cell) =>
                (
                  await call<{ entries: CellIndexEntry[] }>(
                    overlayBase,
                    'POST',
                    '/overlay',
                    { cell },
                    120_000,
                  )
                ).entries
            : undefined,
        );
        logEvents.clear();
        jobs = 0;
        return { ok: true };
      },
      shard: async (p: { job: ShardJob }) => {
        if (!broker || !verify || !index) throw new Error('shard worker not initialised');
        const v = verify;
        const t0 = performance.now();
        const r = await processShard(p.job, {
          alerts,
          verifyAlert: (a) => v(a),
          isDeliveryPaused: async () => false,
          clock: { now: () => new Date() },
          logger,
          cellIndex: index,
          sendQueue: new SqsSendQueue(fakeSqs(broker, 'producer'), QUEUE_URLS),
          makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) =>
            createCheckInToken({ alertId, deviceId, expiresAt, h3Cell }, TOKEN_KEY),
        });
        jobs++;
        return { ...r, ms: performance.now() - t0 };
      },
      info: () => ({
        jobs,
        overlayEntries: index?.overlayEntries ?? 0,
        logEvents: Object.fromEntries(logEvents),
      }),
    },
    onStop: async () => {
      await reader?.close();
      broker?.close();
    },
  };
});
