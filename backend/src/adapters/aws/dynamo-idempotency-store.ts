import { randomUUID } from 'node:crypto';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { ChunkProgress, Clock, IdempotencyStore, KeyInspection } from '../../ports.js';

const DAY_SECONDS = 24 * 60 * 60;

/** DynamoDB / SDK error names that mean "slow down", not "this request is wrong". */
const THROTTLE_ERRORS = new Set([
  'ProvisionedThroughputExceededException',
  'ThrottlingException',
  'RequestLimitExceeded',
]);

export const isThrottleError = (err: unknown): boolean => {
  const e = err as { name?: unknown; __type?: unknown } | null;
  return (
    !!e &&
    ((typeof e.name === 'string' && THROTTLE_ERRORS.has(e.name)) ||
      (typeof e.__type === 'string' && THROTTLE_ERRORS.has(e.__type.split('#').pop() ?? '')))
  );
};

/**
 * claim() kept being throttled past its retry budget. `retryable` tells the sender to release
 * nothing (no claim was taken), leave the batch's SQS message for redelivery and carry on with
 * the other batches; it is never a reason to crash the process or fail a whole message early.
 */
export class IdempotencyThrottledError extends Error {
  readonly retryable = true;
  constructor(
    readonly attempts: number,
    cause: unknown,
  ) {
    super(`idempotency claim throttled after ${attempts} attempts`, { cause });
    this.name = 'IdempotencyThrottledError';
  }
}

export interface ThrottleRetryOptions {
  /** Total tries per DynamoDB call, first try included. Default 5. */
  maxAttempts?: number;
  /** First backoff ceiling in ms; it doubles each retry (full jitter). Default 50. */
  baseDelayMs?: number;
  /** Ceiling of one backoff in ms. Default 1,000. */
  maxDelayMs?: number;
  /** Total time spent sleeping before giving up, in ms. Default 2,500. */
  budgetMs?: number;
  sleep?: (ms: number) => Promise<void>;
  /** Uniform [0, 1). Default Math.random. */
  random?: () => number;
}

/**
 * IdempotencyStore on DynamoDB. Table has partition key `pk` (string) and TTL
 * enabled on attribute `ttl` (epoch seconds), so old keys clean themselves up.
 * Item: { pk, state: 'leased' | 'sent', attemptId, leaseUntil (epoch ms, while leased), ttl }.
 * Each claim writes a fresh attemptId, remembered in this instance, so release and markSent
 * only touch the item if it is still this attempt's (a later attempt may own it after our
 * lease ran out). The port has no attempt parameter, hence the per-instance map.
 * Items written before leases existed have no `state`; they count as sent.
 *
 * Sub-chunk progress (recordChunk) lives on the same item as a string set `sentChunks`, one
 * element per finished sub-chunk: `${set}@${chunk}:${redo offsets, comma-separated}` (set: the
 * recipient-list fingerprint the offsets refer to). A claim that takes
 * over an expired lease reads the old item back (PutItem ReturnValues ALL_OLD, so no extra read)
 * and writes its progress back onto the new lease; release() of a key with progress ends the
 * lease (leaseUntil 0) instead of deleting the item, so the next claim inherits it too.
 *
 * Member sets (G5, scale/fanout/member-set.ts) live in a binary attribute `members`: the sender's
 * markSent writes the recipients of the batch it sent (in the same UpdateItem, under the same
 * "never overwrite another attempt's sent" condition); markPlanned writes the shard worker's plan
 * record with one unconditional PutItem; inspect reads either back with the same GetItem peek does.
 */
export const encodeChunkProgress = (p: ChunkProgress): string =>
  `${p.set}@${p.chunk}:${p.redo.join(',')}`;

/** Undefined for anything malformed, including entries without a fingerprint (never trusted). */
export function decodeChunkProgress(s: string): ChunkProgress | undefined {
  const m = /^([0-9a-f]+)@(\d+):([\d,]*)$/.exec(s);
  if (!m) return undefined;
  const redo = (m[3] ?? '') === '' ? [] : (m[3] ?? '').split(',').map(Number);
  return { set: m[1] ?? '', chunk: Number(m[2]), redo };
}

const progressOf = (raw: unknown): string[] =>
  raw instanceof Set
    ? [...(raw as Set<unknown>)].filter((x): x is string => typeof x === 'string')
    : Array.isArray(raw)
      ? raw.filter((x): x is string => typeof x === 'string')
      : [];

export class DynamoIdempotencyStore implements IdempotencyStore {
  private readonly attempts = new Map<string, string>();
  /** Progress inherited by our claim (encoded), per key; also marks keys whose release must keep the item. */
  private readonly progress = new Map<string, string[]>();

  constructor(
    private readonly doc: Pick<DynamoDBDocumentClient, 'send'>,
    private readonly tableName: string,
    private readonly clock: Clock = { now: () => new Date() },
    private readonly retry: ThrottleRetryOptions = {},
  ) {}

  /**
   * Runs one DynamoDB call, retrying throttles with full-jitter exponential backoff inside a
   * bounded sleep budget (PERF-4b: a statewide send can briefly out-run a table's warm capacity).
   * When the budget is spent it throws IdempotencyThrottledError, which the sender turns into a
   * retryable outcome for that batch. Anything that is not a throttle is rethrown untouched.
   */
  private async send<T>(run: () => Promise<T>): Promise<T> {
    const maxAttempts = this.retry.maxAttempts ?? 5;
    const base = this.retry.baseDelayMs ?? 50;
    const cap = this.retry.maxDelayMs ?? 1_000;
    const budget = this.retry.budgetMs ?? 2_500;
    const sleep = this.retry.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
    const random = this.retry.random ?? Math.random;
    let slept = 0;
    for (let attempt = 1; ; attempt++) {
      try {
        return await run();
      } catch (err) {
        if (!isThrottleError(err)) throw err;
        const delay = Math.floor(random() * Math.min(cap, base * 2 ** (attempt - 1)));
        if (attempt >= maxAttempts || slept + delay > budget) {
          throw new IdempotencyThrottledError(attempt, err);
        }
        slept += delay;
        await sleep(delay);
      }
    }
  }

  async claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'> {
    const attemptId = randomUUID();
    let old: Record<string, unknown> | undefined;
    try {
      const out = await this.send(() =>
        this.doc.send(
          new PutCommand({
            TableName: this.tableName,
            Item: {
              pk: key,
              state: 'leased',
              attemptId,
              leaseUntil: leaseUntil.getTime(),
              // An abandoned lease is cleaned up a day after it runs out.
              ttl: Math.floor(leaseUntil.getTime() / 1000) + DAY_SECONDS,
            },
            // Win if nobody has the key, or the previous holder's lease ran out unsent.
            ConditionExpression:
              'attribute_not_exists(pk) OR (#state = :leased AND leaseUntil < :now)',
            ExpressionAttributeNames: { '#state': 'state' },
            ExpressionAttributeValues: { ':leased': 'leased', ':now': this.clock.now().getTime() },
            // The replaced item (an expired lease) carries the progress of the attempt before us.
            ReturnValues: 'ALL_OLD',
          }),
        ),
      );
      old = (out as { Attributes?: Record<string, unknown> } | undefined)?.Attributes;
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
      // Lost the race: find out whether it is already sent or another attempt is in flight.
      const { Item } = await this.send(() =>
        this.doc.send(
          new GetCommand({ TableName: this.tableName, Key: { pk: key }, ConsistentRead: true }),
        ),
      );
      // Item gone in between (released): report 'leased' so SQS retries shortly.
      if (!Item || Item['state'] === 'leased') return 'leased';
      return 'sent';
    }
    this.attempts.set(key, attemptId);
    this.progress.delete(key);
    const inherited = progressOf(old?.['sentChunks']);
    if (inherited.length > 0) {
      this.progress.set(key, inherited);
      // The put replaced the item: write the inherited progress back onto our lease, so a crash
      // of this attempt does not lose it. Best effort: this attempt has it in memory either way.
      await this.addProgress(key, attemptId, inherited).catch(() => false);
    }
    return 'claimed';
  }

  /** ADDs elements to sentChunks, only while the item is still our lease. */
  private async addProgress(key: string, attemptId: string, items: string[]): Promise<boolean> {
    try {
      await this.send(() =>
        this.doc.send(
          new UpdateCommand({
            TableName: this.tableName,
            Key: { pk: key },
            UpdateExpression: 'ADD sentChunks :p',
            // Same guard as extend: still leased, and still this attempt's.
            ConditionExpression: '#state = :leased AND attemptId = :attempt',
            ExpressionAttributeNames: { '#state': 'state' },
            ExpressionAttributeValues: {
              ':leased': 'leased',
              ':attempt': attemptId,
              ':p': new Set(items),
            },
          }),
        ),
      );
      return true;
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
      return false;
    }
  }

  async recordChunk(key: string, progress: ChunkProgress): Promise<boolean> {
    const attemptId = this.attempts.get(key);
    if (!attemptId) return false; // not claimed by this instance
    const ok = await this.addProgress(key, attemptId, [encodeChunkProgress(progress)]);
    // From now on release() must keep the item (see the class comment).
    if (ok && !this.progress.has(key)) this.progress.set(key, []);
    return ok;
  }

  async claimedProgress(key: string): Promise<ChunkProgress[]> {
    return (this.progress.get(key) ?? [])
      .map(decodeChunkProgress)
      .filter((p): p is ChunkProgress => p !== undefined);
  }

  async peek(key: string): Promise<'absent' | 'leased' | 'sent'> {
    return (await this.inspect(key)).state;
  }

  async inspect(key: string): Promise<KeyInspection> {
    const { Item } = await this.send(() =>
      this.doc.send(
        new GetCommand({ TableName: this.tableName, Key: { pk: key }, ConsistentRead: true }),
      ),
    );
    const state = stateOf(Item, this.clock.now().getTime());
    const members: unknown = Item?.['members'];
    return state === 'sent' && members instanceof Uint8Array ? { state, members } : { state };
  }

  async markPlanned(key: string, members: Uint8Array | null, expiresAt: Date): Promise<void> {
    await this.send(() =>
      this.doc.send(
        new PutCommand({
          TableName: this.tableName,
          Item: {
            pk: key,
            state: 'sent',
            ttl: Math.floor(expiresAt.getTime() / 1000),
            ...(members ? { members } : {}),
          },
        }),
      ),
    );
  }

  async markSent(key: string, expiresAt: Date, detail?: { members?: Uint8Array }): Promise<void> {
    const attemptId = this.attempts.get(key);
    this.attempts.delete(key);
    this.progress.delete(key);
    const members = detail?.members;
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: key },
          UpdateExpression:
            'SET #state = :sent, #ttl = :ttl' +
            (members ? ', members = :members' : '') +
            ' REMOVE leaseUntil',
          // Don't overwrite a key that a different attempt already marked sent.
          ConditionExpression:
            'attribute_not_exists(pk) OR #state <> :sent' +
            (attemptId ? ' OR attemptId = :attempt' : ''),
          ExpressionAttributeNames: { '#state': 'state', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':sent': 'sent',
            ':ttl': Math.floor(expiresAt.getTime() / 1000),
            ...(attemptId ? { ':attempt': attemptId } : {}),
            ...(members ? { ':members': members } : {}),
          },
        }),
      );
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err; // already sent: fine
    }
  }

  async release(key: string): Promise<void> {
    const attemptId = this.attempts.get(key);
    this.attempts.delete(key);
    const keep = this.progress.has(key);
    this.progress.delete(key);
    // Only give back a lease that is still ours; never delete a sent key or a newer lease.
    const guard = {
      ConditionExpression: '#state = :leased' + (attemptId ? ' AND attemptId = :attempt' : ''),
      ExpressionAttributeNames: { '#state': 'state' },
    };
    const values = { ':leased': 'leased', ...(attemptId ? { ':attempt': attemptId } : {}) };
    try {
      await this.doc.send(
        keep
          ? // Progress was recorded: end the lease but keep the item, so the next claim inherits it.
            new UpdateCommand({
              TableName: this.tableName,
              Key: { pk: key },
              UpdateExpression: 'SET leaseUntil = :zero',
              ...guard,
              ExpressionAttributeValues: { ...values, ':zero': 0 },
            })
          : new DeleteCommand({
              TableName: this.tableName,
              Key: { pk: key },
              ...guard,
              ExpressionAttributeValues: values,
            }),
      );
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err; // not ours any more
    }
  }

  async extend(key: string, leaseUntil: Date): Promise<boolean> {
    const attemptId = this.attempts.get(key);
    if (!attemptId) return false; // not claimed by this instance
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: key },
          UpdateExpression: 'SET leaseUntil = :until, #ttl = :ttl',
          // Same guard as release: still leased, and still this attempt's.
          ConditionExpression: '#state = :leased AND attemptId = :attempt',
          ExpressionAttributeNames: { '#state': 'state', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':leased': 'leased',
            ':attempt': attemptId,
            ':until': leaseUntil.getTime(),
            ':ttl': Math.floor(leaseUntil.getTime() / 1000) + DAY_SECONDS,
          },
        }),
      );
      return true;
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
      return false;
    }
  }
}

/**
 * The same reading of an item as claim(): a lease that ran out unsent is claimable, so absent.
 * Items without a state were written before leases existed and count as sent.
 */
function stateOf(
  item: Record<string, unknown> | undefined,
  nowMs: number,
): 'absent' | 'leased' | 'sent' {
  if (!item) return 'absent';
  if (item['state'] === 'leased') {
    return typeof item['leaseUntil'] === 'number' && item['leaseUntil'] < nowMs
      ? 'absent'
      : 'leased';
  }
  return 'sent';
}
