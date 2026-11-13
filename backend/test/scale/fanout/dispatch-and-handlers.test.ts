import { InvokeCommand } from '@aws-sdk/client-lambda';
import { DeleteMessageCommand, SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import type { SQSEvent } from 'aws-lambda';
import { describe, expect, it } from 'vitest';
import type { SendBatch, ShardJob } from '../../../src/scale/contracts.js';
import {
  buildPlannerHandler,
  buildShardHandler,
  runRecords,
  shardDispatcherFromEnv,
} from '../../../src/scale/fanout/handlers.js';
import {
  LambdaShardDispatcher,
  SqsSendQueue,
  SqsShardDispatcher,
  sendBodies,
} from '../../../src/scale/fanout/queues.js';
import { redrive, shardJobFromDlqBody } from '../../../src/scale/fanout/redrive.js';
import { countyCells, entry, setup, signed, target } from './helpers.js';

const noSleep = async () => {};

const job = (n: number): ShardJob => ({
  alertId: 'a',
  sourceId: 's',
  relaySeverity: 'warning',
  shardId: `p:${n}`,
  cells: ['c'],
});

describe('sendBodies (SQS batch send)', () => {
  it('sends at most 10 per request', async () => {
    const sizes: number[] = [];
    const sqs = {
      send: async (cmd: unknown) => {
        sizes.push((cmd as SendMessageBatchCommand).input.Entries?.length ?? 0);
        return {};
      },
    } as never;
    await sendBodies(
      sqs,
      'url',
      Array.from({ length: 25 }, (_, i) => `m${i}`),
      noSleep,
    );
    expect(sizes).toEqual([10, 10, 5]);
  });

  it('keeps one request under the 256 KB total by starting a new request', async () => {
    const totals: number[] = [];
    const sqs = {
      send: async (cmd: unknown) => {
        const entries = (cmd as SendMessageBatchCommand).input.Entries ?? [];
        totals.push(entries.reduce((n, e) => n + (e.MessageBody?.length ?? 0), 0));
        return {};
      },
    } as never;
    await sendBodies(
      sqs,
      'url',
      Array.from({ length: 6 }, () => 'x'.repeat(100_000)),
      noSleep,
    );
    expect(totals.every((t) => t <= 256 * 1024)).toBe(true);
    expect(totals.length).toBeGreaterThan(2);
  });

  it('retries only the entries SQS reports as failed', async () => {
    const seen: string[][] = [];
    let call = 0;
    const sqs = {
      send: async (cmd: unknown) => {
        const entries = (cmd as SendMessageBatchCommand).input.Entries ?? [];
        seen.push(entries.map((e) => e.MessageBody ?? ''));
        return ++call === 1 ? { Failed: [{ Id: '1', Code: 'Throttled', SenderFault: false }] } : {};
      },
    } as never;
    await sendBodies(sqs, 'url', ['a', 'b', 'c'], noSleep);
    expect(seen).toEqual([['a', 'b', 'c'], ['b']]);
  });

  it('throws after repeated per-entry failure, and at once for a sender fault', async () => {
    const always = {
      send: async () => ({ Failed: [{ Id: '0', Code: 'ServiceUnavailable', SenderFault: false }] }),
    } as never;
    await expect(sendBodies(always, 'url', ['a'], noSleep)).rejects.toThrow('rejected 1 message');
    let calls = 0;
    const bad = {
      send: async () => {
        calls++;
        return { Failed: [{ Id: '0', Code: 'InvalidParameterValue', SenderFault: true }] };
      },
    } as never;
    await expect(sendBodies(bad, 'url', ['a'], noSleep)).rejects.toThrow();
    expect(calls).toBe(1);
  });

  it('SqsSendQueue packs batches into messages for the right queue url', async () => {
    const sent: Array<{ url?: string; bodies: string[] }> = [];
    const sqs = {
      send: async (cmd: unknown) => {
        const i = (cmd as SendMessageBatchCommand).input;
        sent.push({ url: i.QueueUrl, bodies: (i.Entries ?? []).map((e) => e.MessageBody ?? '') });
        return {};
      },
    } as never;
    const q = new SqsSendQueue(sqs, { urgent: 'U', standard: 'S' }, noSleep);
    const batches: SendBatch[] = Array.from({ length: 12 }, (_, i) => ({
      alertId: 'a',
      sourceId: 's',
      relaySeverity: 'watch',
      batchKey: `k${i}`,
      attempt: 0,
      recipients: [{ deviceId: 'd', target: target('d'), language: 'en', checkInToken: 't' }],
    }));
    await q.enqueue('standard', batches);
    expect(sent.map((s) => s.url)).toEqual(['S']);
    const messages = sent.flatMap((s) => s.bodies);
    expect(messages).toHaveLength(2); // 10 + 2 batches
    expect((JSON.parse(messages[0] ?? '[]') as SendBatch[]).length).toBe(10);
  });
});

describe('ShardDispatcher', () => {
  it('LambdaShardDispatcher invokes asynchronously, one per job, at most 50 at once', async () => {
    let active = 0;
    let max = 0;
    const invoked: Array<{ type?: string; payload: ShardJob }> = [];
    const lambda = {
      send: async (cmd: unknown) => {
        const input = (cmd as InvokeCommand).input;
        active++;
        max = Math.max(max, active);
        await new Promise((r) => setImmediate(r));
        active--;
        invoked.push({
          type: input.InvocationType,
          payload: JSON.parse(Buffer.from(input.Payload as Uint8Array).toString()),
        });
        expect(input.FunctionName).toBe('shard-fn');
        return { StatusCode: 202 };
      },
    } as never;
    await new LambdaShardDispatcher(lambda, 'shard-fn', 50).dispatch(
      Array.from({ length: 300 }, (_, i) => job(i)),
    );
    expect(invoked).toHaveLength(300);
    expect(invoked.every((i) => i.type === 'Event')).toBe(true);
    expect(new Set(invoked.map((i) => i.payload.shardId)).size).toBe(300);
    expect(max).toBe(50);
  });

  it('LambdaShardDispatcher throws if any invoke fails, so the planner message retries', async () => {
    let n = 0;
    const lambda = {
      send: async () => {
        if (++n === 5) throw new Error('TooManyRequestsException');
        return { StatusCode: 202 };
      },
    } as never;
    await expect(
      new LambdaShardDispatcher(lambda, 'f', 4).dispatch(
        Array.from({ length: 20 }, (_, i) => job(i)),
      ),
    ).rejects.toThrow('TooManyRequests');
    const not202 = { send: async () => ({ StatusCode: 500 }) } as never;
    await expect(new LambdaShardDispatcher(not202, 'f').dispatch([job(1)])).rejects.toThrow('500');
  });

  it('defaults to Lambda invoke and uses SQS only when asked', () => {
    expect(shardDispatcherFromEnv({ SHARD_FUNCTION_NAME: 'f' })).toBeInstanceOf(
      LambdaShardDispatcher,
    );
    expect(
      shardDispatcherFromEnv({ SHARD_DISPATCH: 'sqs', SHARD_QUEUE_URL: 'https://q' }),
    ).toBeInstanceOf(SqsShardDispatcher);
    expect(() => shardDispatcherFromEnv({})).toThrow('SHARD_FUNCTION_NAME');
    expect(() => shardDispatcherFromEnv({ SHARD_DISPATCH: 'sqs' })).toThrow('SHARD_QUEUE_URL');
  });
});

const sqsEvent = (...bodies: unknown[]): SQSEvent =>
  ({
    Records: bodies.map((b, i) => ({
      messageId: `m${i}`,
      body: typeof b === 'string' ? b : JSON.stringify(b),
    })),
  }) as SQSEvent;

describe('handlers', () => {
  it('runRecords reports bad bodies, thrown errors and paused as failures; drops final refusals', async () => {
    const quiet = { info() {}, warn() {}, error() {} };
    const isNum = (v: unknown): v is number => typeof v === 'number';
    const res = await runRecords(
      sqsEvent('not json', 'a string', 1, 2, 3, 4),
      isNum,
      async (n) => {
        if (n === 1) throw new Error('boom');
        if (n === 2) return { refused: 'paused' };
        if (n === 3) return { refused: 'unsigned' };
        return {};
      },
      quiet,
    );
    expect(res.batchItemFailures.map((f) => f.itemIdentifier)).toEqual(['m0', 'm1', 'm2', 'm3']);
  });

  it('planner handler plans a good message, retries a paused one', async () => {
    const alert = signed({}, countyCells());
    const ok = setup(alert);
    const msg = { alertId: alert.id, sourceId: alert.sourceId, relaySeverity: 'warning' };
    const res = await buildPlannerHandler(ok.deps)(sqsEvent(msg));
    expect(res.batchItemFailures).toEqual([]);
    expect(ok.shardDispatcher.jobs.length).toBeGreaterThan(0);

    const paused = setup(alert, { isDeliveryPaused: async () => true });
    const res2 = await buildPlannerHandler(paused.deps)(sqsEvent(msg));
    expect(res2.batchItemFailures).toHaveLength(1);
  });

  it('shard handler as a direct async invoke: works, and throws when paused or on a bad event', async () => {
    const alert = signed();
    const cell = countyCells()[0] as string;
    const s = setup(alert);
    s.cellIndex.add(entry(cell, 'dev-1'));
    const direct: ShardJob = {
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      shardId: 'x:0',
      cells: [cell],
    };
    expect(await buildShardHandler(s.deps)(direct)).toBeUndefined();
    expect(s.sendQueue.batches('urgent')).toHaveLength(1);

    const p = setup(alert, { isDeliveryPaused: async () => true });
    await expect(buildShardHandler(p.deps)(direct)).rejects.toThrow('paused');
    await expect(buildShardHandler(s.deps)({ nope: 1 } as never)).rejects.toThrow('not a ShardJob');
  });

  it('shard handler from SQS returns batchItemFailures', async () => {
    const alert = signed();
    const cell = countyCells()[0] as string;
    const s = setup(alert);
    s.cellIndex.add(entry(cell, 'dev-1'));
    const good: ShardJob = {
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      shardId: 'x:0',
      cells: [cell],
    };
    const res = await buildShardHandler(s.deps)(sqsEvent(good, { junk: true }));
    expect(res).toEqual({ batchItemFailures: [{ itemIdentifier: 'm1' }] });
  });

  it('retries once with refreshed keys when the signature fails (key rotation)', async () => {
    const alert = signed();
    const cell = countyCells()[0] as string;
    const stale = setup(alert, { verifyAlert: () => false });
    stale.cellIndex.add(entry(cell, 'dev-1'));
    const fresh = setup(alert);
    fresh.cellIndex.add(entry(cell, 'dev-1'));
    const direct: ShardJob = {
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      shardId: 'x:0',
      cells: [cell],
    };
    await buildShardHandler(stale.deps, async () => fresh.deps)(direct);
    expect(fresh.sendQueue.batches('urgent')).toHaveLength(1);
  });
});

describe('redrive', () => {
  it('unwraps Lambda failure records, replays async, deletes only what it replayed', async () => {
    const j = job(1);
    const messages = [
      { Body: JSON.stringify({ requestPayload: j, responsePayload: {} }), ReceiptHandle: 'r1' },
      { Body: JSON.stringify(job(2)), ReceiptHandle: 'r2' },
      { Body: 'garbage', ReceiptHandle: 'r3' },
    ];
    let received = false;
    const deleted: string[] = [];
    const sqs = {
      send: async (cmd: unknown) => {
        if (cmd instanceof DeleteMessageCommand) {
          deleted.push(cmd.input.ReceiptHandle ?? '');
          return {};
        }
        const out = { Messages: received ? [] : messages };
        received = true;
        return out;
      },
    } as never;
    const invoked: ShardJob[] = [];
    const lambda = {
      send: async (cmd: unknown) => {
        const input = (cmd as InvokeCommand).input;
        expect(input.InvocationType).toBe('Event');
        invoked.push(JSON.parse(Buffer.from(input.Payload as Uint8Array).toString()));
        return { StatusCode: 202 };
      },
    } as never;
    const r = await redrive({ sqs, lambda, dlqUrl: 'dlq', shardFunctionName: 'f' });
    expect(r).toEqual({ replayed: 2, skipped: 1 });
    expect(invoked.map((x) => x.shardId)).toEqual(['p:1', 'p:2']);
    expect(deleted).toEqual(['r1', 'r2']);
    expect(shardJobFromDlqBody('{}')).toBeNull();
  });
});
