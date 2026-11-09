import { randomUUID } from 'node:crypto';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { Clock, IdempotencyStore } from '../../ports.js';

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
 */
export class DynamoIdempotencyStore implements IdempotencyStore {
  private readonly attempts = new Map<string, string>();

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
    try {
      await this.send(() =>
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
          }),
        ),
      );
      this.attempts.set(key, attemptId);
      return 'claimed';
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
    }
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

  async markSent(key: string, expiresAt: Date): Promise<void> {
    const attemptId = this.attempts.get(key);
    this.attempts.delete(key);
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: { pk: key },
          UpdateExpression: 'SET #state = :sent, #ttl = :ttl REMOVE leaseUntil',
          // Don't overwrite a key that a different attempt already marked sent.
          ConditionExpression:
            'attribute_not_exists(pk) OR #state <> :sent' +
            (attemptId ? ' OR attemptId = :attempt' : ''),
          ExpressionAttributeNames: { '#state': 'state', '#ttl': 'ttl' },
          ExpressionAttributeValues: {
            ':sent': 'sent',
            ':ttl': Math.floor(expiresAt.getTime() / 1000),
            ...(attemptId ? { ':attempt': attemptId } : {}),
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
    try {
      await this.doc.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: { pk: key },
          // Only give back a lease that is still ours; never delete a sent key or a newer lease.
          ConditionExpression: '#state = :leased' + (attemptId ? ' AND attemptId = :attempt' : ''),
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: {
            ':leased': 'leased',
            ...(attemptId ? { ':attempt': attemptId } : {}),
          },
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
