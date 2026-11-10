/**
 * t3 aggregator process: the REAL stream aggregator (createAggregator) fed in batches of up to
 * 1,000 stream records (the DynamoDB Streams batch size) by the API processes, writing counters
 * into an in-memory stand-in for the totals table with transaction semantics (markers, ADD).
 * Every 20th batch is delivered a second time, as Lambda does after a retry: the per-record
 * markers must keep the totals exact.
 */
import net from 'node:net';
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBStreamEvent } from 'aws-lambda';
import { createAggregator } from '../../../../../backend/src/scale/checkins/aggregator.js';
import { ALERT_ID, nowMs, type AggDone } from './common.js';

type Item = Record<string, unknown>;

class FakeTotalsTable {
  readonly counters = new Map<string, Item>();
  readonly markers = new Set<string>();
  async send(cmd: unknown): Promise<object> {
    if (!(cmd instanceof TransactWriteCommand)) throw new Error('unexpected command');
    const tx = cmd.input.TransactItems ?? [];
    const reasons = tx.map((t) =>
      t.Put && this.markers.has(String((t.Put.Item as Item)['pk']))
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
      if (t.Put) this.markers.add(String((t.Put.Item as Item)['pk']));
      if (t.Update) {
        const pk = String((t.Update.Key as Item)['pk']);
        const v = t.Update.ExpressionAttributeValues as Item;
        const row = this.counters.get(pk) ?? { pk };
        row['checkedIn'] = Number(row['checkedIn'] ?? 0) + Number(v[':n']);
        row['h3Cell'] = v[':c'];
        this.counters.set(pk, row);
      }
    }
    return {};
  }
}

console.log = () => {}; // the aggregator logs one line per batch

function start(producers: number, aggIndex: number): void {
  const table = new FakeTotalsTable();
  const aggregate = createAggregator({
    doc: table as never,
    totalsTable: 't3-totals',
    sleep: async () => {},
  });

  const queue: string[][] = [];
  let maxQueue = 0;
  let ended = 0;
  let processing = false;
  let seq = 0;
  let batches = 0;
  let redelivered = 0;
  let failedItems = 0;
  let records = 0;
  let busyMs = 0;
  let lastBatchDoneMs = 0;
  let reported = false;

  const toEvent = (b: number, lines: string[]): DynamoDBStreamEvent =>
    ({
      Records: lines.map((line, k) => {
        const sp = line.indexOf(' ');
        const deviceId = line.slice(0, sp);
        const h3Cell = line.slice(sp + 1);
        return {
          eventID: `a${aggIndex}-${b}-${k}`,
          eventName: 'INSERT' as const,
          eventSource: 'aws:dynamodb',
          dynamodb: {
            SequenceNumber: String(b * 10_000 + k + 1),
            NewImage: {
              pk: { S: `${ALERT_ID}#${deviceId}` },
              alertId: { S: ALERT_ID },
              deviceId: { S: deviceId },
              h3Cell: { S: h3Cell },
            },
          },
        };
      }),
    }) as unknown as DynamoDBStreamEvent;

  const maybeFinish = (): void => {
    if (reported || ended < producers || queue.length > 0 || processing) return;
    reported = true;
    const totals: Array<[string, number]> = [];
    for (const row of table.counters.values())
      totals.push([String(row['h3Cell']), Number(row['checkedIn'])]);
    const done: AggDone = {
      type: 'done',
      totals,
      batches,
      redeliveredBatches: redelivered,
      failedItems,
      records,
      markers: table.markers.size,
      lastBatchDoneMs,
      busyMs: Math.round(busyMs),
      maxQueue,
      maxRssKb: process.resourceUsage().maxRSS,
    };
    process.send?.(done);
  };

  const drain = async (): Promise<void> => {
    if (processing) return;
    processing = true;
    for (let lines = queue.shift(); lines; lines = queue.shift()) {
      const b = seq++;
      const ev = toEvent(b, lines);
      const s = performance.now();
      const out = await aggregate(ev);
      failedItems += out.batchItemFailures.length;
      if (b % 20 === 19) {
        const again = await aggregate(ev);
        failedItems += again.batchItemFailures.length;
        redelivered++;
      }
      busyMs += performance.now() - s;
      batches++;
      records += lines.length;
      lastBatchDoneMs = nowMs();
    }
    processing = false;
    maybeFinish();
  };

  const server = net.createServer((sock) => {
    let rest = '';
    let cur: string[] = [];
    sock.setEncoding('utf8');
    sock.on('data', (chunk: string) => {
      const parts = (rest + chunk).split('\n');
      rest = parts.pop() ?? '';
      for (const line of parts) {
        if (line === '-') {
          queue.push(cur);
          cur = [];
          maxQueue = Math.max(maxQueue, queue.length);
        } else if (line === 'END') ended++;
        else if (line) cur.push(line);
      }
      void drain();
      maybeFinish();
    });
  });
  server.listen(0, '127.0.0.1', () => {
    const addr = server.address();
    process.send?.({ type: 'ready', port: typeof addr === 'object' && addr ? addr.port : 0 });
  });
}

process.on('disconnect', () => process.exit(0));
process.on('message', (m: { type: string; producers: number; index: number }) => {
  if (m.type === 'init') start(m.producers, m.index);
});
process.send?.({ type: 'loaded' });
