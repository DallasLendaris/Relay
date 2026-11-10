/**
 * Shard-worker process (stands in for the shard Lambdas): runs the REAL processShard against the
 * on-disk cell index, with the REAL fan-out SqsSendQueue writing to the broker's send queues.
 * Several jobs run at once per process, like several Lambda instances on one core.
 * Also answers 'expected' for the verifier: the token hashes of every entry in some cells.
 */
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../../../backend/src/adapters/memory/alert-store.js';
import type { Logger } from '../../../../../backend/src/delivery/types.js';
import { CachedAlertReader } from '../../../../../backend/src/scale/fanout/alert-cache.js';
import { SqsSendQueue } from '../../../../../backend/src/scale/fanout/queues.js';
import { processShard } from '../../../../../backend/src/scale/fanout/shard-worker.js';
import type { ShardJob } from '../../../../../backend/src/scale/contracts.js';
import type { AlertVerifier } from '../../../../../backend/src/signing/index.js';
import { createCheckInToken } from '../../../../../backend/src/tokens/checkInToken.js';
import { TOKEN_KEY, verifierFromPem } from './alert.js';
import { serve } from './proc.js';
import { DiskCellIndex, SeedReader, parseLine } from './seed-data.js';
import { HashList } from './verify.js';
import { BrokerClient, QUEUE_URLS, fakeSqs } from './wire.js';

let broker: BrokerClient | undefined;
let verify: AlertVerifier | undefined;
let alerts = new CachedAlertReader(new InMemoryAlertStore());
let reader: SeedReader | undefined;
let cellIndex: DiskCellIndex | undefined;
const logEvents = new Map<string, number>();
const count = (msg: string) => logEvents.set(msg, (logEvents.get(msg) ?? 0) + 1);
const logger: Logger = { info: () => {}, warn: count, error: count };

serve({
  init: async (p: { brokerPort: number; publicPem: string }) => {
    broker = await BrokerClient.connect(p.brokerPort);
    verify = verifierFromPem(p.publicPem);
    return true;
  },
  scenario: async (p: { alert: Alert; seedDir: string; fcmProjects: string[] }) => {
    const store = new InMemoryAlertStore();
    await store.save(p.alert);
    alerts = new CachedAlertReader(store);
    if (reader?.dir !== p.seedDir) {
      await reader?.close();
      reader = new SeedReader(p.seedDir);
    }
    cellIndex = new DiskCellIndex(reader, p.fcmProjects);
    logEvents.clear();
    return true;
  },
  shard: async (p: { job: ShardJob }) => {
    if (!broker || !verify || !cellIndex) throw new Error('shard worker not initialised');
    const v = verify;
    const t0 = performance.now();
    const r = await processShard(p.job, {
      alerts,
      verifyAlert: (a) => v(a),
      isDeliveryPaused: async () => false,
      clock: { now: () => new Date() },
      logger,
      cellIndex,
      sendQueue: new SqsSendQueue(fakeSqs(broker, 'producer'), QUEUE_URLS),
      makeCheckInToken: (alertId, deviceId, h3Cell, expiresAt) =>
        createCheckInToken({ alertId, deviceId, expiresAt, h3Cell }, TOKEN_KEY),
    });
    return { ...r, ms: performance.now() - t0 };
  },
  expected: async (p: { seedDir: string; cells: string[] }) => {
    const rd = new SeedReader(p.seedDir);
    const out = new HashList();
    let entries = 0;
    for (const cell of p.cells) {
      for (const line of await rd.cellLines(cell)) {
        entries++;
        out.add(parseLine(line).token);
      }
    }
    await rd.close();
    return { hashes: out.toArray(), entries };
  },
  logEvents: () => Object.fromEntries(logEvents),
});
