import {
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  type Message,
} from '@aws-sdk/client-sqs';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
  PushTarget,
  SendBatch,
  SendQueue,
  SendQueueName,
} from '../../../src/scale/contracts.js';
import type { SenderDeps } from '../../../src/scale/sender/sender.js';
import { signed, verifyAlert } from '../../delivery/fakes.js';

export const silent = { info() {}, warn() {}, error() {} };

export interface LogLine {
  level: string;
  msg: string;
  fields?: Record<string, unknown>;
}
export function recordingLogger() {
  const lines: LogLine[] = [];
  const mk = (level: string) => (msg: string, fields?: Record<string, unknown>) => {
    lines.push({ level, msg, ...(fields ? { fields } : {}) });
  };
  return { lines, logger: { info: mk('info'), warn: mk('warn'), error: mk('error') } };
}

export function makeBatch(over: Partial<SendBatch> = {}, n = 3): SendBatch {
  const alert = signed();
  return {
    alertId: alert.id,
    sourceId: alert.sourceId,
    relaySeverity: 'warning',
    batchKey: `${alert.id}:warning:cell1:7`,
    recipients: Array.from({ length: n }, (_, i) => ({
      deviceId: `dev${i}`,
      target: { platform: i % 2 ? 'android' : 'ios', token: `tok${i}` } as PushTarget,
      language: i === 1 ? 'es' : 'en',
      checkInToken: `ct${i}`,
    })),
    attempt: 0,
    ...over,
  };
}

/** Fake push sender. `decide` picks each message's result; default all ok. */
export class FakePush implements NativePushSender {
  calls: NativePushMessage[][] = [];
  inFlight = 0;
  maxInFlight = 0;
  constructor(
    private readonly decide: (m: NativePushMessage) => NativePushResult = () => ({ ok: true }),
    private readonly latencyMs = 0,
  ) {}
  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    this.calls.push(messages);
    this.inFlight++;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    if (this.latencyMs > 0) await new Promise((r) => setTimeout(r, this.latencyMs));
    this.inFlight--;
    return messages.map((m) => this.decide(m));
  }
}

export class FakeSendQueue implements SendQueue {
  enqueued: Array<{ queue: SendQueueName; batches: SendBatch[] }> = [];
  fail = false;
  /** Optional hook so the throughput test can feed retries back into its fake SQS. */
  onEnqueue?: (queue: SendQueueName, batches: SendBatch[]) => void;
  async enqueue(queue: SendQueueName, batches: SendBatch[]): Promise<void> {
    if (this.fail) throw new Error('sqs down');
    this.enqueued.push({ queue, batches });
    this.onEnqueue?.(queue, batches);
  }
}

export function makeDeps(over: Partial<SenderDeps> = {}) {
  const state = { now: new Date('2026-11-10T10:05:00Z') };
  const clock = { now: () => state.now };
  const alerts = new InMemoryAlertStore();
  const idempotency = new MemoryIdempotencyStore(clock);
  const sendQueue = new FakeSendQueue();
  const push = new FakePush();
  const dead: Array<{ deviceId: string; target: PushTarget }> = [];
  const events: string[] = [];
  const deps: SenderDeps = {
    idempotency,
    alerts,
    verifyAlert,
    isDeliveryPaused: async () => false,
    push,
    sendQueue,
    onDeadTarget: async (deviceId, target) => void dead.push({ deviceId, target }),
    clock,
    logger: silent,
    sleep: async () => {},
    ...over,
  };
  return { deps, state, alerts, idempotency, sendQueue, push, dead, events };
}

export async function seedAlert(
  alerts: InMemoryAlertStore,
  over: Partial<Alert> = {},
): Promise<Alert> {
  const a = signed(over);
  await alerts.save(a);
  return a;
}

// ---------- Fake SQS ----------

interface Stored {
  id: string;
  body: string;
  sentAt: number;
  invisibleUntil: number;
  receipt?: string;
  receives: number;
}

/** In-memory SQS that understands the three commands the consumer uses. */
export class FakeSqs {
  readonly queues = new Map<string, Stored[]>();
  receives: Array<{ queue: string; max: number; wait: number }> = [];
  extensions: Array<{ queue: string; seconds: number }> = [];
  deleteCalls: number[] = [];
  /** Receipt handles whose deletes should be reported as failed. */
  failDeletes = new Set<string>();
  private seq = 0;

  constructor(
    private readonly visibilityMs = 30,
    private readonly now: () => number = Date.now,
  ) {}

  add(queue: string, body: unknown, sentAt = this.now()): void {
    const list = this.queues.get(queue) ?? [];
    list.push({
      id: `m${this.seq++}`,
      body: typeof body === 'string' ? body : JSON.stringify(body),
      sentAt,
      invisibleUntil: 0,
      receives: 0,
    });
    this.queues.set(queue, list);
  }

  size(queue: string): number {
    return this.queues.get(queue)?.length ?? 0;
  }

  visible(queue: string): number {
    return (this.queues.get(queue) ?? []).filter((m) => m.invisibleUntil <= this.now()).length;
  }

  async send(command: unknown): Promise<unknown> {
    if (command instanceof ReceiveMessageCommand) {
      const { QueueUrl = '', MaxNumberOfMessages = 1, WaitTimeSeconds = 0 } = command.input;
      this.receives.push({ queue: QueueUrl, max: MaxNumberOfMessages, wait: WaitTimeSeconds });
      const take = (): Message[] => {
        const out: Message[] = [];
        for (const m of this.queues.get(QueueUrl) ?? []) {
          if (out.length >= MaxNumberOfMessages) break;
          if (m.invisibleUntil > this.now()) continue;
          m.invisibleUntil = this.now() + this.visibilityMs;
          m.receives++;
          m.receipt = `${m.id}#${m.receives}`;
          out.push({
            MessageId: m.id,
            ReceiptHandle: m.receipt,
            Body: m.body,
            Attributes: { SentTimestamp: String(m.sentAt) },
          });
        }
        return out;
      };
      let msgs = take();
      if (msgs.length === 0 && WaitTimeSeconds > 0) {
        await new Promise((r) => setTimeout(r, 5));
        msgs = take();
      } else {
        await Promise.resolve();
      }
      return { Messages: msgs };
    }
    if (command instanceof DeleteMessageBatchCommand) {
      const { QueueUrl = '', Entries = [] } = command.input;
      this.deleteCalls.push(Entries.length);
      const list = this.queues.get(QueueUrl) ?? [];
      const Failed: Array<{ Id: string }> = [];
      for (const e of Entries) {
        if (this.failDeletes.has(e.ReceiptHandle ?? '')) {
          Failed.push({ Id: e.Id ?? '' });
          continue;
        }
        const i = list.findIndex((m) => m.receipt === e.ReceiptHandle);
        if (i >= 0) list.splice(i, 1);
      }
      return { Failed };
    }
    if (command instanceof ChangeMessageVisibilityCommand) {
      const { QueueUrl = '', ReceiptHandle, VisibilityTimeout = 0 } = command.input;
      this.extensions.push({ queue: QueueUrl, seconds: VisibilityTimeout });
      const m = (this.queues.get(QueueUrl) ?? []).find((x) => x.receipt === ReceiptHandle);
      if (m) m.invisibleUntil = this.now() + VisibilityTimeout * 1000;
      return {};
    }
    throw new Error('unexpected command');
  }
}

export async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 2));
  }
}
