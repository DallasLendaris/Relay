/**
 * The broker wire protocol (TCP on 127.0.0.1) and the client-side adapters built on it:
 *  - `fakeSqs`: an object with SQSClient's `send(command)` that the REAL SqsSendQueue (fan-out and
 *    sender) and the REAL SenderConsumer use. Commands are recognised by their input shape.
 *  - `RemoteIdempotencyStore`: the IdempotencyStore port against the broker's shared store. Like
 *    DynamoIdempotencyStore, each instance remembers its own attempt per key, so release, extend
 *    and markSent only touch the claim this process made.
 *
 * Frame: u32 total length (bytes after this field), u32 header length, header JSON, payload bytes.
 * Message bodies travel as raw payload bytes (never re-encoded inside JSON).
 */
import net from 'node:net';
import type { IdempotencyStore, KeyInspection } from '../../../../../backend/src/ports.js';

export interface Frame {
  h: Record<string, unknown> & { id: number };
  p: Buffer;
}

export function encodeFrame(header: object, payload?: Buffer): Buffer {
  const hb = Buffer.from(JSON.stringify(header), 'utf8');
  const pl = payload?.length ?? 0;
  const out = Buffer.allocUnsafe(8 + hb.length + pl);
  out.writeUInt32LE(4 + hb.length + pl, 0);
  out.writeUInt32LE(hb.length, 4);
  hb.copy(out, 8);
  if (payload && pl > 0) payload.copy(out, 8 + hb.length);
  return out;
}

/** Reassembles frames from socket chunks. */
export class FrameReader {
  private chunks: Buffer[] = [];
  private len = 0;

  constructor(private readonly onFrame: (f: Frame) => void) {}

  push(chunk: Buffer): void {
    this.chunks.push(chunk);
    this.len += chunk.length;
    for (;;) {
      if (this.len < 4) return;
      let head = this.chunks[0] as Buffer;
      if (head.length < 4) {
        head = Buffer.concat(this.chunks);
        this.chunks = [head];
      }
      const total = head.readUInt32LE(0);
      if (this.len < 4 + total) return;
      const all = this.chunks.length === 1 ? head : Buffer.concat(this.chunks);
      const hlen = all.readUInt32LE(4);
      const h = JSON.parse(all.toString('utf8', 8, 8 + hlen)) as Frame['h'];
      const p = all.subarray(8 + hlen, 4 + total);
      const rest = all.subarray(4 + total);
      this.chunks = rest.length > 0 ? [rest] : [];
      this.len = rest.length;
      this.onFrame({ h, p });
    }
  }
}

export class BrokerClient {
  private seq = 0;
  private readonly pending = new Map<number, (f: Frame) => void>();
  private closed = false;

  private constructor(private readonly sock: net.Socket) {
    const reader = new FrameReader((f) => {
      const cb = this.pending.get(f.h.id);
      if (!cb) return;
      this.pending.delete(f.h.id);
      cb(f);
    });
    sock.on('data', (c: Buffer) => reader.push(c));
    sock.on('close', () => {
      this.closed = true;
      const err: Frame = { h: { id: 0, error: 'broker connection closed' }, p: Buffer.alloc(0) };
      for (const cb of this.pending.values()) cb(err);
      this.pending.clear();
    });
    sock.on('error', () => {});
  }

  static connect(port: number): Promise<BrokerClient> {
    return new Promise((resolve, reject) => {
      const sock = net.connect({ host: '127.0.0.1', port }, () => {
        sock.setNoDelay(true);
        resolve(new BrokerClient(sock));
      });
      sock.once('error', reject);
    });
  }

  async call(header: Record<string, unknown>, payload?: Buffer): Promise<Frame> {
    if (this.closed) throw new Error('broker connection closed');
    const id = ++this.seq;
    const f = await new Promise<Frame>((resolve) => {
      this.pending.set(id, resolve);
      this.sock.write(encodeFrame({ ...header, id }, payload));
    });
    if (typeof f.h['error'] === 'string') throw new Error(f.h['error']);
    return f;
  }

  close(): void {
    this.sock.destroy();
  }
}

// ---------- fake SQS client ----------

export const QUEUE_URLS = { urgent: 'sim://urgent', standard: 'sim://standard' } as const;

const queueOf = (url: unknown): string => {
  const s = String(url);
  if (s === QUEUE_URLS.urgent) return 'urgent';
  if (s === QUEUE_URLS.standard) return 'standard';
  throw new Error(`unknown queue url ${s}`);
};

interface SendEntry {
  Id: string;
  MessageBody?: string;
  DelaySeconds?: number;
  ReceiptHandle?: string;
}

/**
 * `role` 'producer' (shard workers) waits while the broker's backlog is over its memory cap;
 * 'sender' (retries from processBatch) never waits, so draining can never deadlock.
 */
export function fakeSqs(
  broker: BrokerClient,
  role: 'producer' | 'sender',
): { send(command: unknown): Promise<never> } {
  const send = async (command: unknown): Promise<unknown> => {
    const input = (command as { input?: Record<string, unknown> }).input ?? {};
    const entries = input['Entries'] as SendEntry[] | undefined;
    if (entries && entries.length > 0 && entries[0]?.MessageBody !== undefined) {
      const bodies = entries.map((e) => Buffer.from(e.MessageBody ?? '', 'utf8'));
      await broker.call(
        {
          op: 'send',
          role,
          queue: queueOf(input['QueueUrl']),
          entries: entries.map((e, i) => ({
            delay: e.DelaySeconds ?? 0,
            len: bodies[i]?.length ?? 0,
          })),
        },
        Buffer.concat(bodies),
      );
      return { Successful: entries.map((e) => ({ Id: e.Id })), Failed: [] };
    }
    if (entries && entries[0]?.ReceiptHandle !== undefined) {
      await broker.call({
        op: 'del',
        queue: queueOf(input['QueueUrl']),
        rhs: entries.map((e) => e.ReceiptHandle),
      });
      return { Successful: entries.map((e) => ({ Id: e.Id })), Failed: [] };
    }
    if (input['MaxNumberOfMessages'] !== undefined) {
      const f = await broker.call({
        op: 'recv',
        queue: queueOf(input['QueueUrl']),
        max: input['MaxNumberOfMessages'],
        wait: input['WaitTimeSeconds'] ?? 0,
      });
      const msgs = f.h['msgs'] as Array<{ mid: string; rh: string; sent: number; len: number }>;
      let at = 0;
      return {
        Messages: msgs.map((m) => {
          const Body = f.p.toString('utf8', at, at + m.len);
          at += m.len;
          return {
            MessageId: m.mid,
            ReceiptHandle: m.rh,
            Body,
            Attributes: { SentTimestamp: String(m.sent) },
          };
        }),
      };
    }
    if (input['ReceiptHandle'] !== undefined && input['VisibilityTimeout'] !== undefined) {
      await broker.call({
        op: 'vis',
        queue: queueOf(input['QueueUrl']),
        rh: input['ReceiptHandle'],
        vt: input['VisibilityTimeout'],
      });
      return {};
    }
    throw new Error('fakeSqs: unsupported command');
  };
  return { send: send as (command: unknown) => Promise<never> };
}

// ---------- shared idempotency store ----------

export class RemoteIdempotencyStore implements IdempotencyStore {
  private readonly attempts = new Map<string, number>();

  constructor(private readonly broker: BrokerClient) {}

  async claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'> {
    const f = await this.broker.call({ op: 'claim', key, until: leaseUntil.getTime() });
    const r = f.h['r'] as 'claimed' | 'sent' | 'leased';
    if (r === 'claimed') this.attempts.set(key, f.h['a'] as number);
    return r;
  }

  async markSent(key: string, expiresAt: Date, detail?: { members?: Uint8Array }): Promise<void> {
    const a = this.attempts.get(key) ?? 0;
    this.attempts.delete(key);
    const members = detail?.members ? Buffer.from(detail.members) : undefined;
    await this.broker.call(
      { op: 'markSent', key, until: expiresAt.getTime(), a, members: members !== undefined },
      members,
    );
  }

  /** The shard worker's plan record of a cell (G5); `members` null = set over its size cap. */
  async markPlanned(key: string, members: Uint8Array | null, expiresAt: Date): Promise<void> {
    const payload = members ? Buffer.from(members) : undefined;
    await this.broker.call(
      { op: 'markPlanned', key, until: expiresAt.getTime(), members: payload !== undefined },
      payload,
    );
  }

  async inspect(key: string): Promise<KeyInspection> {
    const f = await this.broker.call({ op: 'inspect', key });
    const state = f.h['r'] as 'absent' | 'leased' | 'sent';
    return f.h['members'] === true ? { state, members: new Uint8Array(f.p) } : { state };
  }

  async release(key: string): Promise<void> {
    const a = this.attempts.get(key);
    this.attempts.delete(key);
    if (a === undefined) return;
    await this.broker.call({ op: 'release', key, a });
  }

  async extend(key: string, leaseUntil: Date): Promise<boolean> {
    const a = this.attempts.get(key);
    if (a === undefined) return false;
    const f = await this.broker.call({ op: 'extend', key, until: leaseUntil.getTime(), a });
    return f.h['r'] === true;
  }

  async peek(key: string): Promise<'absent' | 'leased' | 'sent'> {
    const f = await this.broker.call({ op: 'peek', key });
    return f.h['r'] as 'absent' | 'leased' | 'sent';
  }
}
