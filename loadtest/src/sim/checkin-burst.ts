/**
 * Check-in burst: every delivered device checks in once, through the API's stateless check-in
 * route (in-memory check-in store, no database), and the resulting stream events go through the
 * real aggregator in batches of 1,000 against an in-memory stand-in for the totals table.
 */
import { randomUUID } from 'node:crypto';
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBStreamEvent } from 'aws-lambda';
import { MemoryCheckInStore } from '../../../backend/src/adapters/memory/checkin-store.js';
import { createApi, type ApiDeps } from '../../../backend/src/api/index.js';
import { createAggregator } from '../../../backend/src/scale/checkins/aggregator.js';
import { createDeviceCredential } from '../../../backend/src/tokens/deviceCredential.js';
import type { CheckIn } from '@relay/shared';
import { ALERT_ID, DEVICE_CRED_KEY, TOKEN_KEY } from './fanout-sim.js';

export interface BurstInput {
  /** Check-in tokens of delivered pushes (a device with two cells may appear twice). */
  checkInTokens: Iterable<string>;
  /** Distinct devices that were in the alert area but never got a push (dead tokens). */
  permanentFailureDevices: number;
  /** Devices delivered per the mock whose success the sender never saw (no token to check in with). */
  deliveredWithoutAck: number;
  devicesInArea: number;
  log?: (line: string) => void;
}

export interface BurstResult {
  checkIns: number;
  api: { ms: number; perSec: number; ok200: number; errors: number; p50Ms: number; p99Ms: number };
  aggregator: {
    batches: number;
    batchSize: number;
    ms: number;
    perSec: number;
    countersWritten: number;
    markers: number;
    failedItems: number;
  };
  verification: {
    ok: boolean;
    totalsSum: number;
    expectedSum: number;
    storeSum: number;
    perCellMismatches: number;
    cells: number;
    expectedFormula: string;
  };
}

/** Reads the device id and cell out of a v2 token body (the test harness already trusts it). */
function claims(token: string): { d: string; c: string } {
  const body = token.slice(0, token.indexOf('.'));
  return JSON.parse(Buffer.from(body, 'base64url').toString('utf8')) as { d: string; c: string };
}

type Item = Record<string, unknown>;

/** Just enough DynamoDB for the aggregator: atomic transactions, ADD, put-if-absent markers. */
class FakeTotalsTable {
  readonly rows = new Map<string, Item>();
  async send(cmd: unknown): Promise<object> {
    if (!(cmd instanceof TransactWriteCommand)) throw new Error('unexpected command');
    const tx = cmd.input.TransactItems ?? [];
    const reasons = tx.map((t) =>
      t.Put && this.rows.has(String((t.Put.Item as Item)['pk']))
        ? { Code: 'ConditionalCheckFailed' }
        : { Code: 'None' },
    );
    if (reasons.some((r) => r.Code !== 'None')) {
      throw new TransactionCanceledException({
        message: 'cancelled',
        $metadata: {},
        CancellationReasons: reasons,
      });
    }
    for (const t of tx) {
      if (t.Put) this.rows.set(String((t.Put.Item as Item)['pk']), t.Put.Item as Item);
      if (t.Update) {
        const pk = String((t.Update.Key as Item)['pk']);
        const v = t.Update.ExpressionAttributeValues as Item;
        const row = this.rows.get(pk) ?? { pk };
        row['checkedIn'] = Number(row['checkedIn'] ?? 0) + Number(v[':n']);
        row['alertId'] = v[':a'];
        row['h3Cell'] = v[':c'];
        this.rows.set(pk, row);
      }
    }
    return {};
  }
}

/** A check-in store that also remembers each saved event, standing in for the table's stream. */
class StreamingStore extends MemoryCheckInStore {
  readonly events: Array<{ deviceId: string; h3Cell: string }> = [];
  override async save(c: CheckIn): Promise<boolean> {
    const first = await super.save(c);
    if (first) this.events.push({ deviceId: c.deviceId, h3Cell: c.h3Cell });
    return first;
  }
}

const forbidden = (what: string): never => {
  throw new Error(`the stateless check-in route must not touch ${what}`);
};
const trap = (what: string) => new Proxy({}, { get: () => () => forbidden(what) }) as never;

export async function runCheckinBurst(input: BurstInput): Promise<BurstResult> {
  const log = input.log ?? (() => {});

  // One check-in per distinct delivered device (its first token), streamed from the sends.
  const seenDevices = new Set<string>();
  const expectedByCell = new Map<string, number>();
  const tokens = input.checkInTokens[Symbol.iterator]();
  let N = 0;
  const nextCheckIn = (): { deviceId: string; token: string; i: number } | undefined => {
    for (;;) {
      const r = tokens.next();
      if (r.done) return undefined;
      const c = claims(r.value);
      if (seenDevices.has(c.d)) continue;
      seenDevices.add(c.d);
      expectedByCell.set(c.c, (expectedByCell.get(c.c) ?? 0) + 1);
      return { deviceId: c.d, token: r.value, i: N++ };
    }
  };

  const store = new StreamingStore();
  const handler = createApi({
    devices: trap('the device store'),
    places: trap('the place store'),
    alerts: trap('the alert store'),
    geocoder: trap('the geocoder'),
    cellIndex: trap('the cell index'),
    checkins: store,
    tokenKeys: new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]),
    deviceCredentialKeys: new Map([[DEVICE_CRED_KEY.id, DEVICE_CRED_KEY.secret]]),
    clock: { now: () => new Date() },
    newId: randomUUID,
  } as ApiDeps);

  // ----- API: stateless check-in route -----
  const lat: number[] = [];
  let ok200 = 0;
  let errors = 0;
  const CONC = 200;
  const t0 = performance.now();
  const worker = async (): Promise<void> => {
    for (;;) {
      const e = nextCheckIn();
      if (!e) return;
      const { deviceId, token, i } = e;
      const s = performance.now();
      const res = await handler({
        rawPath: `/alerts/${ALERT_ID}/checkins`,
        headers: { authorization: `Bearer ${createDeviceCredential(deviceId, DEVICE_CRED_KEY)}` },
        body: JSON.stringify({ status: 'ok', token, idempotencyKey: `sim-${i}` }),
        requestContext: { http: { method: 'POST' } },
      });
      lat.push(performance.now() - s);
      if (res.statusCode === 200) ok200++;
      else errors++;
    }
  };
  await Promise.all(Array.from({ length: CONC }, worker));
  const apiMs = performance.now() - t0;
  const sortedLat = Float64Array.from(lat).sort();
  const q = (p: number) =>
    sortedLat[Math.min(sortedLat.length - 1, Math.ceil(p * sortedLat.length) - 1)] ?? 0;
  log(
    `API: ${N} check-ins in ${Math.round(apiMs)} ms = ${Math.round((N / apiMs) * 1000)}/s, errors ${errors}`,
  );

  // ----- aggregator in batches of 1,000 -----
  const table = new FakeTotalsTable();
  const aggregate = createAggregator({
    doc: table as never,
    totalsTable: 'sim-totals',
    sleep: async () => {},
  });
  seenDevices.clear();
  const origLog = console.log;
  console.log = () => {}; // the aggregator logs one line per batch
  let aggMs = 0;
  let failedItems = 0;
  const BATCH = 1000;
  let batches = 0;
  try {
    for (let i = 0; i < store.events.length; i += BATCH) {
      const records = store.events.slice(i, i + BATCH).map((e, k) => {
        const n = i + k;
        return {
          eventID: `evt-${n}`,
          eventName: 'INSERT' as const,
          eventSource: 'aws:dynamodb',
          dynamodb: {
            SequenceNumber: String(1_000_000 + n),
            NewImage: {
              pk: { S: `${ALERT_ID}#${e.deviceId}` },
              alertId: { S: ALERT_ID },
              deviceId: { S: e.deviceId },
              h3Cell: { S: e.h3Cell },
            },
          },
        };
      });
      const s = performance.now();
      const out = await aggregate({ Records: records } as unknown as DynamoDBStreamEvent);
      aggMs += performance.now() - s;
      failedItems += out.batchItemFailures.length;
      batches++;
    }
  } finally {
    console.log = origLog;
  }
  log(`aggregator: ${batches} batches in ${Math.round(aggMs)} ms`);

  // ----- verify -----
  let totalsSum = 0;
  let counters = 0;
  let markers = 0;
  const gotByCell = new Map<string, number>();
  for (const [pk, row] of table.rows) {
    if (pk.startsWith('rec#')) markers++;
    else {
      counters++;
      totalsSum += Number(row['checkedIn']);
      gotByCell.set(String(row['h3Cell']), Number(row['checkedIn']));
    }
  }
  let mismatches = 0;
  let storeSum = 0;
  for (const [cell, n] of expectedByCell) {
    if (gotByCell.get(cell) !== n) mismatches++;
    if (store.total(ALERT_ID, cell) !== n) mismatches++;
    storeSum += store.total(ALERT_ID, cell);
  }
  const expectedSum =
    input.devicesInArea - input.permanentFailureDevices - input.deliveredWithoutAck;
  return {
    checkIns: N,
    api: {
      ms: Math.round(apiMs),
      perSec: Math.round((N / apiMs) * 1000),
      ok200,
      errors,
      p50Ms: Math.round(q(0.5) * 100) / 100,
      p99Ms: Math.round(q(0.99) * 100) / 100,
    },
    aggregator: {
      batches,
      batchSize: BATCH,
      ms: Math.round(aggMs),
      perSec: Math.round((N / aggMs) * 1000),
      countersWritten: counters,
      markers,
      failedItems,
    },
    verification: {
      ok:
        errors === 0 &&
        mismatches === 0 &&
        totalsSum === expectedSum &&
        storeSum === expectedSum &&
        gotByCell.size === expectedByCell.size,
      totalsSum,
      expectedSum,
      storeSum,
      perCellMismatches: mismatches,
      cells: expectedByCell.size,
      expectedFormula: 'devices in area - permanent failures - delivered without an ack',
    },
  };
}
