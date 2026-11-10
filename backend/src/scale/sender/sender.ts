/**
 * processBatch: sends one SendBatch to APNs / FCM and settles it.
 *
 * Order of operations (the order is the safety argument):
 *   1. claim batchKey with a 45 s lease ('sent' = already done, 'leased' = someone is on it).
 *      The consumer's heartbeat keeps extending that lease while we run (see BatchControl).
 *   2. re-read the alert and re-check eligibility and the kill switch
 *   3. send all recipients in sub-chunks of SENDER_CHUNK_SIZE, a few chunks in flight at once.
 *      Before each chunk starts we check BatchControl: if the lease was lost or shutdown began we
 *      stop starting chunks. In a batch of more than one chunk, each finished chunk is recorded
 *      on our claim (IdempotencyStore.recordChunk: what went out, which offsets must be redone).
 *      A re-claim after a crash (lease expiry) or a release skips what went out and sends only
 *      the rest, so a crashed sender duplicates at most the chunks it had in flight. Progress is
 *      bound to the exact recipient list (recipientFingerprint): a re-plan under the same key
 *      with other recipients ignores it and sends everything (a duplicate, never a miss).
 *   4. classify results. Permanent failures are dead targets, unless the circuit breaker is
 *      open (see breaker.ts), in which case they are retried and nothing is cleared. Retryable
 *      failures are re-queued as a NEW batch (`${base}:r${attempt + 1}`, only those recipients)
 *      with a delay: exponential with full jitter, never shorter than the largest Retry-After.
 *      There is no attempt cap; a retry is dropped (counted as GaveUp) once it would run after
 *      the alert ends or more than 30 minutes after the batch was first processed.
 *   5. markSent(batchKey)
 *   6. only now, hand dead targets to the gate (dead-target-gate.ts): it holds them until the
 *      breaker has seen enough results, then queues them for cleanup, or (breaker open)
 *      re-queues them as a retry batch `${batchKey}:h${attempt + 1}` (see retryKey and
 *      heldRetryKey: all derived keys are distinct). Cleanup is asynchronous (dead-target-queue.ts) and never delays sending or extends
 *      a batch past its lease.
 *
 * Accepted trade-offs, all of which can mean ONE duplicate push to recipients in chunks that had
 * already been sent but not yet recorded (in flight, or a single-chunk batch). Both pushes share
 * the alert's collapse key, so a phone shows one notification, and a duplicate alert is far
 * better than a lost one:
 *   - step 4's re-enqueue fails: we release the claim and throw, the message is redelivered.
 *   - the lease is lost mid-batch: we stop and leave the message to whoever now holds the key.
 *   - shutdown deadline (SIGTERM + 25 s): unfinished batches stop starting chunks, settle
 *     nothing (only their chunk progress stays), release their claims, and the messages become
 *     visible again.
 *   - the sender process dies: the lease runs out and the redelivered message re-claims the key.
 *   - the kill switch flips while we are sending: we release and the message retries later.
 */
import { createHash } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { alertEndTime, claimExpiry, refusalFor } from '../../delivery/eligibility.js';
import { PUSH_POLICY } from '../../delivery/policy.js';
import { alertPushText } from '../../delivery/send-jobs.js';
import type { Logger } from '../../delivery/types.js';
import type { AlertStore, ChunkProgress, Clock, IdempotencyStore } from '../../ports.js';
import {
  QUEUE_FOR_SEVERITY,
  SENDER_CHUNK_SIZE,
  type NativePushMessage,
  type NativePushResult,
  type NativePushSender,
  type PushTarget,
  type SendBatch,
  type SendQueue,
} from '../contracts.js';
import { errorFields } from '../../delivery/redact.js';
import { publicAlertDocUrl } from '../publish/paths.js';
import { PermanentBreaker, platformKeyOf, type BreakerVerdict } from './breaker.js';
import { DeadTargetGate, type DeadTargetGateOptions, type HeldGroup } from './dead-target-gate.js';
import { DeadTargetQueue } from './dead-target-queue.js';
import { consoleMetricWriter, emfLine, type MetricWriter } from './metrics.js';

/** How long a claim holds. Must outlast a slow batch; the consumer extends it while the batch runs. */
export const SENDER_LEASE_MS = 45_000;
/** Retries stop this long after the batch was first processed (or at the alert's end, if sooner). */
export const RETRY_BUDGET_MS = 30 * 60 * 1000;
/** Retry delay: min(900, 2 s x 2^attempt) with full jitter. 900 s is the SQS per-message cap. */
export const RETRY_BASE_SECONDS = 2;
export const RETRY_MAX_DELAY_SECONDS = 900;
/** Wait before re-reading an alert whose signature failed (an update may have just committed). */
const REREAD_DELAY_MS = 200;
/** Default number of sub-chunks in flight per batch. */
const DEFAULT_CHUNK_CONCURRENCY = 8;
/** Result stand-in for a recipient an earlier attempt of the same batch already sent. */
const ALREADY_SENT: NativePushResult = { ok: true };

/**
 * Shared mutable flags between processBatch and its caller (the consumer). The consumer's
 * heartbeat sets `leaseLost` when extending the claim fails and `abort` at the shutdown deadline;
 * processBatch checks them before every chunk and before settling.
 */
export interface BatchControl {
  /** True from the moment the claim is ours until the batch settles. The heartbeat only extends active batches. */
  active: boolean;
  leaseLost: boolean;
  /** Shutdown deadline reached: stop starting chunks, persist nothing, release. */
  abort: boolean;
  /** Gives the claim back; set once claimed. The consumer calls it for batches that will not settle. */
  release?: () => Promise<void>;
}

export const newBatchControl = (): BatchControl => ({
  active: false,
  leaseLost: false,
  abort: false,
});

export interface SenderDeps {
  idempotency: IdempotencyStore;
  alerts: Pick<AlertStore, 'get'>;
  /** True only if the alert's signature is valid. */
  verifyAlert(alert: Alert): boolean;
  /** Kill switch. While paused nothing is sent and the message is retried later. */
  isDeliveryPaused(): Promise<boolean>;
  push: NativePushSender;
  sendQueue: SendQueue;
  /**
   * Clears a dead target in the device store and the cell index. Runs in the background queue
   * (see `deadTargets`), never on the send path. Failures are logged, never thrown.
   */
  onDeadTarget(deviceId: string, target: PushTarget): Promise<void>;
  /** The bounded cleanup queue. Default: one created lazily around `onDeadTarget`. */
  deadTargets?: Pick<DeadTargetQueue, 'enqueue'>;
  /** Process-wide permanent-failure breaker. Default: one per `deps` object. */
  breaker?: PermanentBreaker;
  /**
   * Holds dead targets until the breaker has enough evidence (dead-target-gate.ts), then hands
   * them to `deadTargets`. Default: one per `deps` object around `breaker` and `deadTargets`; a
   * gate passed in must be built on the same breaker.
   */
  deadTargetGate?: Pick<DeadTargetGate, 'submit'>;
  clock: Clock;
  logger: Logger;
  /** Where EMF metric lines go. Default console.log. */
  writeMetric?: MetricWriter;
  /** Drops any cached copy of the alert before the signature re-read. */
  invalidateAlert?(id: string): void;
  /** Reloads the alert keyring so a newly rotated key is picked up (called at most once per batch). */
  refreshKeys?(): Promise<void>;
  /** Injectable so tests don't wait. */
  sleep?: (ms: number) => Promise<void>;
  /** Uniform [0, 1) for retry jitter. Default Math.random. */
  random?: () => number;
  /** Sub-chunks in flight per batch. Default 8. */
  chunkConcurrency?: number;
  /**
   * CDN origin (env PUBLIC_ALERT_BASE_URL, e.g. https://dxxxx.cloudfront.net). When set, every
   * push carries `data.alertUrl`, the cell-free alert document, so phones open the alert from the
   * CDN instead of the API. Unset: the field is left out and the app uses the API.
   */
  publicAlertBaseUrl?: string;
}

export type BatchReason =
  | 'sent'
  | 'duplicate'
  | 'leased'
  | 'claim_throttled'
  /** claim() failed with something other than a throttle; the message is left for redelivery. */
  | 'claim_failed'
  | 'alert_missing'
  | 'unsigned'
  | 'paused'
  | 'not_actual'
  | 'inactive'
  | 'expired'
  | 'informational'
  | 'stale_severity'
  | 'lease_lost'
  | 'shutdown';

export interface BatchOutcome {
  /** True: this batch is finished (the SQS message may be deleted). False: leave it to retry. */
  ack: boolean;
  reason: BatchReason;
  sent: number;
  permanentFailed: number;
  retried: number;
  gaveUp: number;
}

const outcome = (
  ack: boolean,
  reason: BatchReason,
  over: Partial<BatchOutcome> = {},
): BatchOutcome => ({
  ack,
  reason,
  sent: 0,
  permanentFailed: 0,
  retried: 0,
  gaveUp: 0,
  ...over,
});

/** `${alertId}:${severity}:${cell}:${bucket}` without any `:r<n>` retry suffix. */
export const baseBatchKey = (batchKey: string): string => batchKey.replace(/:r\d+$/, '');

/**
 * Seconds to wait before retry number `attempt + 1`: full jitter over min(900, 2 x 2^attempt),
 * raised to at least `retryAfterSeconds` (the largest Retry-After seen), and never above the
 * 900 s SQS cap.
 */
export function retryDelaySeconds(
  attempt: number,
  retryAfterSeconds: number,
  random: () => number,
): number {
  const cap = Math.min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_SECONDS * 2 ** Math.min(attempt, 30));
  const jittered = random() * cap;
  return Math.min(RETRY_MAX_DELAY_SECONDS, Math.ceil(Math.max(jittered, retryAfterSeconds)));
}

type Loaded = { alert: Alert } | { fail: BatchReason };

async function loadEligible(batch: SendBatch, deps: SenderDeps, now: Date): Promise<Loaded> {
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  let alert = await deps.alerts.get(batch.alertId);
  if (!alert) return { fail: 'alert_missing' }; // maybe read lag: retry
  let refusal = refusalFor(alert, now, deps.verifyAlert);
  if (refusal === 'unsigned') {
    // Torn read: an update may have committed between our reads. Look once more.
    await sleep(REREAD_DELAY_MS);
    deps.invalidateAlert?.(batch.alertId);
    alert = (await deps.alerts.get(batch.alertId)) ?? alert;
    refusal = refusalFor(alert, now, deps.verifyAlert);
  }
  if (refusal === 'unsigned' && deps.refreshKeys) {
    // Maybe our cached keyring lacks a newly rotated key. Reload once and check again.
    await deps.refreshKeys();
    refusal = refusalFor(alert, now, deps.verifyAlert);
  }
  // A signature failure is never acked: a real alert signed with a key we don't know yet must
  // not be lost. SQS retries, and a truly tampered row ends up in the DLQ for a human.
  if (refusal) return { fail: refusal };
  return { alert };
}

export function buildMessages(
  batch: SendBatch,
  alert: Alert,
  now: Date,
  publicAlertBaseUrl?: string,
): NativePushMessage[] {
  const policy = PUSH_POLICY[alert.relaySeverity];
  const end = alertEndTime(alert);
  const ttlSeconds = Math.max(1, Math.floor((end.getTime() - now.getTime()) / 1000));
  const text = { en: alertPushText(alert, 'en'), es: alertPushText(alert, 'es') };
  const alertUrl = publicAlertDocUrl(publicAlertBaseUrl, alert);
  return batch.recipients.map((r) => ({
    target: r.target,
    title: text[r.language].title,
    body: text[r.language].body,
    data: {
      alertId: alert.id,
      checkInToken: r.checkInToken,
      expiresAt: end.toISOString(),
      ...(alertUrl ? { alertUrl } : {}),
    },
    priority: policy.priority,
    sound: policy.sound !== null,
    collapseKey: alert.id,
    ttlSeconds,
    category: 'alert-checkin' as const,
  }));
}

/**
 * Sends the messages in sub-chunks, `chunkConcurrency` at a time. Returns one result per message;
 * `undefined` for messages whose chunk was never started because the control flags said stop.
 */
async function sendAll(
  messages: NativePushMessage[],
  deps: SenderDeps,
  batchKey: string,
  control: BatchControl,
  done: Map<number, Set<number>>,
  set: string,
): Promise<Array<NativePushResult | undefined>> {
  const chunks: NativePushMessage[][] = [];
  for (let i = 0; i < messages.length; i += SENDER_CHUNK_SIZE) {
    chunks.push(messages.slice(i, i + SENDER_CHUNK_SIZE));
  }
  const perChunk: Array<Array<NativePushResult | undefined>> = chunks.map((c, idx) => {
    // A chunk an earlier attempt finished: what went out counts as sent; only `redo` is sent again.
    const redo = done.get(idx);
    return c.map((_m, i) => (redo && !redo.has(i) ? ALREADY_SENT : undefined));
  });
  // Progress is recorded only for multi-chunk batches: for a single chunk it would save nothing
  // but the short gap between the push and markSent, at the cost of a write per batch.
  const record =
    chunks.length > 1 ? deps.idempotency.recordChunk?.bind(deps.idempotency) : undefined;
  let next = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      if (control.leaseLost || control.abort) return; // do not start another chunk
      const idx = next++;
      const chunk = chunks[idx];
      if (!chunk) return;
      const redo = done.get(idx);
      const offsets = redo ? [...redo].sort((a, b) => a - b) : chunk.map((_m, i) => i);
      if (offsets.length === 0) continue; // all of it went out before
      const toSend = offsets.map((i) => chunk[i] as NativePushMessage);
      const results = perChunk[idx] as Array<NativePushResult | undefined>;
      try {
        const res = await deps.push.send(toSend);
        // Fewer results than messages is an adapter bug: treat the missing ones as retryable.
        offsets.forEach((o, i) => {
          results[o] = res[i] ?? { ok: false, kind: 'retryable', error: 'NoResult' };
        });
      } catch (err) {
        deps.logger.error('sender.chunk_threw', {
          batchKey,
          size: toSend.length,
          // Name only: the message text could carry a token.
          error: err instanceof Error ? err.name : 'Error',
        });
        for (const o of offsets) results[o] = { ok: false, kind: 'retryable', error: 'SendThrew' };
      }
      if (record) {
        // Everything not ok (except message-scoped failures, which are final) must be redone by
        // whoever re-claims this batch after a crash: retries and dead targets are only settled
        // at the end of the batch.
        const again = offsets.filter((o) => {
          const r = results[o];
          return !r?.ok && !(r?.kind === 'permanent' && r.scope === 'message');
        });
        try {
          if (!(await record(batchKey, { set, chunk: idx, redo: again }))) {
            control.leaseLost = true;
          }
        } catch (err) {
          // Without the record a crash re-sends this chunk; nothing else is lost.
          deps.logger.warn('sender.record_chunk_failed', { batchKey, ...errorFields(err) });
        }
      }
    }
  };
  const n = Math.max(
    1,
    Math.min(deps.chunkConcurrency ?? DEFAULT_CHUNK_CONCURRENCY, chunks.length),
  );
  await Promise.all(Array.from({ length: n }, worker));
  return perChunk.flat();
}

const defaultBreakers = new WeakMap<object, PermanentBreaker>();
const defaultQueues = new WeakMap<object, DeadTargetQueue>();

function breakerFor(deps: SenderDeps): PermanentBreaker {
  if (deps.breaker) return deps.breaker;
  let b = defaultBreakers.get(deps);
  if (!b) defaultBreakers.set(deps, (b = new PermanentBreaker()));
  return b;
}

const defaultGates = new WeakMap<object, DeadTargetGate>();

/**
 * The gate for these deps, built on their breaker and cleanup queue, re-queueing held recipients
 * through their send queue. `extra` overrides gate options (tests: timer, holdMs).
 */
export function createDeadTargetGate(
  deps: SenderDeps,
  extra: Partial<DeadTargetGateOptions> = {},
): DeadTargetGate {
  return new DeadTargetGate({
    breaker: breakerFor(deps),
    queue: deadTargetsFor(deps),
    requeue: (groups) => requeueHeld(groups, deps),
    now: () => deps.clock.now().getTime(),
    logger: deps.logger,
    ...(deps.writeMetric ? { writeMetric: deps.writeMetric } : {}),
    ...extra,
  });
}

function gateFor(deps: SenderDeps): Pick<DeadTargetGate, 'submit'> {
  if (deps.deadTargetGate) return deps.deadTargetGate;
  let g = defaultGates.get(deps);
  if (!g) defaultGates.set(deps, (g = createDeadTargetGate(deps)));
  return g;
}

/*
 * Derived batch keys. Every batch can spawn two children, both at attempt + 1:
 *   retry  R(K) = baseBatchKey(K) + `:r<attempt+1>`  (its failed recipients; the existing scheme)
 *   held   H(K) = K + `:h<attempt+1>`               (recipients the dead-target gate held)
 * They are distinct by construction:
 *   - R(K) ends in `:r<n>` and H(K) in `:h<n>`, so a retry key never equals a held key;
 *   - H keeps all of K, so H(K1) = H(K2) only if K1 = K2;
 *   - R strips only a trailing `:r<n>`, so R(K1) = R(K2) needs K1 and K2 in one lineage L (L, or
 *     L:r<n>) and at the same attempt. Within a lineage the attempt identifies the key: L itself
 *     is a planned key (attempt 0) or ends in `:h<a>` (attempt a), and L:r<n> is at attempt n > a.
 * Nothing ever strips or rewrites an `:h<n>` segment. test/scale/sender/derived-keys.test.ts
 * checks this on random derivation chains.
 */

/** Key of the retry of a batch's failed recipients. */
export const retryKey = (batch: SendBatch): string =>
  `${baseBatchKey(batch.batchKey)}:r${batch.attempt + 1}`;

/** Key of the re-queue of a batch's held recipients (see the derivation comment above). */
export const heldRetryKey = (batch: Pick<SendBatch, 'batchKey' | 'attempt'>): string =>
  `${batch.batchKey}:h${batch.attempt + 1}`;

const REQUEUE_TRIES = 3;

/**
 * Re-queues recipients the gate held when the breaker found the "dead" tokens to be a
 * misconfiguration: one retry batch per source batch, on its severity's queue, attempt + 1,
 * firstAttemptAt kept (same 30-minute budget), the usual retry delay. Tokens are not touched.
 * Resolves with how many recipients were re-queued.
 */
async function requeueHeld(groups: HeldGroup[], deps: SenderDeps): Promise<number> {
  const random = deps.random ?? Math.random;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  let requeued = 0;
  for (const g of groups) {
    const batch = g.source;
    const delay = retryDelaySeconds(batch.attempt, 0, random);
    const nowMs = deps.clock.now().getTime();
    if (nowMs + delay * 1000 >= g.deadlineMs) {
      deps.logger.error('delivery.gave_up', {
        alertId: batch.alertId,
        batchKey: batch.batchKey,
        attempt: batch.attempt,
        count: g.recipients.length,
        reason: nowMs >= g.deadlineMs ? 'budget_spent' : 'retry_would_miss_budget',
        held: true,
      });
      continue;
    }
    const retryBatch: SendBatch = {
      ...batch,
      batchKey: heldRetryKey(batch),
      recipients: g.recipients,
      attempt: batch.attempt + 1,
      firstAttemptAt: new Date(g.firstAttemptMs).toISOString(),
    };
    const queue = QUEUE_FOR_SEVERITY[batch.relaySeverity] ?? 'standard';
    for (let i = 1; ; i++) {
      try {
        await deps.sendQueue.enqueue(queue, [retryBatch], { delaySeconds: delay });
        requeued += g.recipients.length;
        break;
      } catch (err) {
        if (i >= REQUEUE_TRIES) {
          deps.logger.error('sender.held_requeue_failed', {
            alertId: batch.alertId,
            batchKey: retryBatch.batchKey,
            count: g.recipients.length,
            ...errorFields(err),
          });
          break;
        }
        await sleep(200 * i);
      }
    }
  }
  return requeued;
}

/**
 * Fingerprint of what chunk progress offsets point at: the batch's recipients in order (device
 * id and push target) and the chunk size. Order matters, not just the set: offsets are positions,
 * so the same devices in another order would make an offset name a different device. The push
 * target is in it too, so a device whose token changed is sent again rather than skipped.
 */
export function recipientFingerprint(recipients: SendBatch['recipients']): string {
  const h = createHash('sha256').update(`chunk=${SENDER_CHUNK_SIZE}\n`);
  for (const r of recipients) h.update(`${r.deviceId}\t${r.target.platform}\t${r.target.token}\n`);
  return h.digest('hex').slice(0, 24);
}

/**
 * Turns progress recorded by earlier attempts into chunk -> offsets still to send. Only entries
 * recorded for this exact recipient list (`set`) count; others are ignored, so a re-planned batch
 * with the same key but different recipients is sent in full (a possible duplicate, never a
 * miss). Several entries for a chunk intersect: an offset is done once any attempt sent it.
 */
export function progressByChunk(progress: ChunkProgress[], set: string): Map<number, Set<number>> {
  const done = new Map<number, Set<number>>();
  for (const p of progress) {
    if (p.set !== set) continue;
    const prev = done.get(p.chunk);
    const redo = new Set(p.redo);
    done.set(p.chunk, prev ? new Set([...prev].filter((o) => redo.has(o))) : redo);
  }
  return done;
}

function deadTargetsFor(deps: SenderDeps): Pick<DeadTargetQueue, 'enqueue'> {
  if (deps.deadTargets) return deps.deadTargets;
  let q = defaultQueues.get(deps);
  if (!q) {
    q = new DeadTargetQueue({
      process: deps.onDeadTarget,
      logger: deps.logger,
      ...(deps.writeMetric ? { writeMetric: deps.writeMetric } : {}),
    });
    defaultQueues.set(deps, q);
  }
  return q;
}

/** Counts error labels (e.g. 'Apns400:BadDeviceToken'); labels never carry tokens. */
function topErrors(results: NativePushResult[]): Record<string, number> {
  const counts = new Map<string, number>();
  for (const r of results) if (!r.ok) counts.set(r.error, (counts.get(r.error) ?? 0) + 1);
  return Object.fromEntries([...counts].sort((a, b) => b[1] - a[1]).slice(0, 3));
}

const TRANSIENT_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ECONNABORTED',
  'ETIMEDOUT',
  'EPIPE',
  'EAI_AGAIN',
  'UND_ERR_SOCKET',
  'UND_ERR_CONNECT_TIMEOUT',
]);
const TRANSIENT_NAMES = new Set([
  'TimeoutError',
  'AbortError',
  'RequestTimeout',
  'RequestTimeoutException',
  'NetworkingError',
  'ServiceUnavailable',
  'InternalServerError',
]);

/**
 * A network or server-side failure worth simply retrying later: socket errors, timeouts, an AWS
 * SDK error marked retryable or answered with a 5xx. Not a bug, a validation or a permission error.
 */
export function isTransientError(err: unknown, depth = 0): boolean {
  if (err === null || typeof err !== 'object') return false;
  const e = err as {
    name?: unknown;
    code?: unknown;
    message?: unknown;
    status?: unknown;
    $retryable?: unknown;
    $metadata?: { httpStatusCode?: unknown };
    cause?: unknown;
  };
  if (typeof e.code === 'string' && TRANSIENT_CODES.has(e.code)) return true;
  if (typeof e.name === 'string' && TRANSIENT_NAMES.has(e.name)) return true;
  if (e.$retryable !== undefined && e.$retryable !== null) return true;
  const status = typeof e.status === 'number' ? e.status : e.$metadata?.httpStatusCode;
  if (typeof status === 'number' && status >= 500) return true;
  if (typeof e.message === 'string' && /socket hang up|\btimed? ?out\b/i.test(e.message)) {
    return true;
  }
  return depth < 4 && e.cause !== undefined && isTransientError(e.cause, depth + 1);
}

export async function processBatch(
  batch: SendBatch,
  deps: SenderDeps,
  control: BatchControl = newBatchControl(),
): Promise<BatchOutcome> {
  const { idempotency, logger } = deps;
  const write = deps.writeMetric ?? consoleMetricWriter;
  let claim: Awaited<ReturnType<typeof idempotency.claim>>;
  try {
    claim = await idempotency.claim(
      batch.batchKey,
      new Date(deps.clock.now().getTime() + SENDER_LEASE_MS),
    );
  } catch (err) {
    // The store retried its own throttles and gave up (PERF-4b). No claim was taken, so there is
    // nothing to release: leave this batch's message to be redelivered, never crash the message.
    if ((err as { retryable?: unknown } | null)?.retryable === true) {
      logger.warn('sender.claim_throttled', { batchKey: batch.batchKey, ...errorFields(err) });
      return outcome(false, 'claim_throttled');
    }
    // A transient transport failure (connection reset, timeout, a 5xx). Whether a claim was taken
    // is unknown, so do not release: if it was, it runs out with its lease and the redelivered
    // message re-claims it (inheriting any progress). Leaving the message un-acked is what a
    // throw would do, without counting the batch as failed. Anything else (a bug, a wrong table,
    // no permission) still throws, loudly, as before.
    if (!isTransientError(err)) throw err;
    logger.error('sender.claim_failed', { batchKey: batch.batchKey, ...errorFields(err) });
    write(emfLine('ClaimFailed', 1, deps.clock.now().getTime()));
    return outcome(false, 'claim_failed');
  }
  if (claim === 'sent') return outcome(true, 'duplicate');
  if (claim === 'leased') return outcome(false, 'leased'); // another attempt is live; retry later

  const release = async () => {
    await idempotency.release(batch.batchKey).catch((err: unknown) => {
      logger.error('sender.release_failed', { batchKey: batch.batchKey, ...errorFields(err) });
    });
  };
  control.release = release;
  control.active = true;

  try {
    const now = deps.clock.now();
    const loaded = await loadEligible(batch, deps, now);
    if ('fail' in loaded) {
      await release();
      const reason = loaded.fail;
      const final = reason === 'not_actual' || reason === 'inactive' || reason === 'expired';
      logger.error(final ? 'sender.alert_refused' : 'sender.alert_unavailable', {
        alertId: batch.alertId,
        batchKey: batch.batchKey,
        reason,
      });
      return outcome(final, reason);
    }
    const { alert } = loaded;
    if (!PUSH_POLICY[alert.relaySeverity].push) {
      await release();
      return outcome(true, 'informational');
    }
    if (alert.relaySeverity !== batch.relaySeverity) {
      // The alert was re-severitied after this batch was planned; the upgrade has its own
      // fan-out and keys. Sending this stale batch would use the wrong urgency.
      await release();
      logger.warn('sender.stale_batch', { alertId: alert.id, batchKey: batch.batchKey });
      return outcome(true, 'stale_severity');
    }
    if (await deps.isDeliveryPaused()) {
      await release();
      logger.warn('sender.paused', { batchKey: batch.batchKey });
      return outcome(false, 'paused');
    }

    const messages = buildMessages(batch, alert, now, deps.publicAlertBaseUrl);
    // A re-claim after a crashed attempt skips the sub-chunks that attempt finished, but only
    // if it was sending this very recipient list (see recipientFingerprint).
    const set = recipientFingerprint(batch.recipients);
    let done = new Map<number, Set<number>>();
    if (deps.idempotency.claimedProgress) {
      try {
        const inherited = await deps.idempotency.claimedProgress(batch.batchKey);
        done = progressByChunk(inherited, set);
        const foreign = inherited.filter((p) => p.set !== set).length;
        if (foreign > 0) {
          logger.warn('sender.progress_ignored', {
            batchKey: batch.batchKey,
            entries: foreign,
            reason: 'recipients_changed',
          });
        }
      } catch (err) {
        logger.warn('sender.progress_read_failed', {
          batchKey: batch.batchKey,
          ...errorFields(err),
        });
      }
      if (done.size > 0) {
        logger.info('sender.batch_resumed', {
          batchKey: batch.batchKey,
          chunksDone: done.size,
          redo: [...done.values()].reduce((n, s) => n + s.size, 0),
        });
      }
    }
    const results = await sendAll(messages, deps, batch.batchKey, control, done, set);

    // The lease went away or shutdown began while sending: settle nothing.
    if (control.leaseLost) {
      logger.error('delivery.lease_lost', {
        alertId: alert.id,
        batchKey: batch.batchKey,
        sentChunks: results.filter((r) => r !== undefined).length,
        total: results.length,
      });
      return outcome(false, 'lease_lost'); // not ours any more: nothing to release
    }
    if (control.abort) {
      await release();
      logger.warn('sender.shutdown_abandoned', { batchKey: batch.batchKey });
      return outcome(false, 'shutdown');
    }

    const settled = results.map(
      (r): NativePushResult => r ?? { ok: false, kind: 'retryable', error: 'NoResult' },
    );
    let sent = 0;
    const permanent: SendBatch['recipients'] = [];
    const failed: SendBatch['recipients'] = [];
    let messageFailed = 0;
    let resumed = 0; // sent by an earlier attempt: not new evidence for the breaker
    let retryAfter = 0;
    settled.forEach((r, i) => {
      const rec = batch.recipients[i];
      if (!rec) return;
      if (r === ALREADY_SENT) resumed++;
      if (r.ok) sent++;
      else if (r.kind === 'permanent' && r.scope === 'message') messageFailed++;
      else if (r.kind === 'permanent') permanent.push(rec);
      else {
        failed.push(rec);
        retryAfter = Math.max(retryAfter, r.retryAfterSeconds ?? 0);
      }
    });

    // Circuit breaker: when permanent failures look systemic (wrong bundle id, sandbox/prod
    // mismatch) do not clear anyone; treat them as retryable. Message-scoped failures are not
    // evidence about tokens, so they stay out of the rate.
    let dead = permanent;
    let verdict: BreakerVerdict | undefined;
    if (settled.length > 0) {
      verdict = breakerFor(deps).record(settled.length - messageFailed - resumed, permanent.length);
      if (verdict.open && permanent.length > 0) {
        logger.error('delivery.permanent_breaker_open', {
          alertId: alert.id,
          batchKey: batch.batchKey,
          reason: verdict.reason,
          permanent: permanent.length,
          total: settled.length,
          batchRate: Number(verdict.batchRate.toFixed(4)),
          rollingRate: Number(verdict.rollingRate.toFixed(4)),
          errors: topErrors(settled),
        });
        write(emfLine('PermanentBreakerOpen', 1, deps.clock.now().getTime()));
        failed.push(...permanent);
        dead = [];
      }
    }
    // Per-platform breaker (breaker.ts PlatformWindows): every token result counts toward its
    // platform's windows; permanents of a platform that now looks misconfigured are retried, not
    // treated as dead. This catches a single platform (an APNs key, one FCM project) that the
    // process-wide rules dilute, and an onset in small batches.
    const platforms = breakerFor(deps).platforms;
    const since: Record<string, number> = {};
    for (const rec of batch.recipients) {
      const key = platformKeyOf(rec.target);
      since[key] ??= platforms.seq(key) + 1;
    }
    settled.forEach((r, i) => {
      const rec = batch.recipients[i];
      if (
        !rec ||
        r === ALREADY_SENT ||
        (!r.ok && r.kind === 'permanent' && r.scope === 'message')
      ) {
        return;
      }
      platforms.record(platformKeyOf(rec.target), !r.ok && r.kind === 'permanent');
    });
    if (dead.length > 0) {
      const systemic = dead.filter((d) => platforms.systemic(platformKeyOf(d.target)));
      if (systemic.length > 0) {
        const keys = [...new Set(systemic.map((d) => platformKeyOf(d.target)))];
        logger.error('delivery.platform_breaker_open', {
          alertId: alert.id,
          batchKey: batch.batchKey,
          permanent: systemic.length,
          platforms: keys.map((k) => ({ platform: k, ...platforms.stats(k) })),
        });
        write(emfLine('PlatformBreakerOpen', 1, deps.clock.now().getTime()));
        failed.push(...systemic);
        const moved = new Set(systemic);
        dead = dead.filter((d) => !moved.has(d));
      }
    }

    let retried = 0;
    let gaveUp = messageFailed;
    if (messageFailed > 0) {
      logger.error('delivery.payload_too_large', {
        alertId: alert.id,
        batchKey: batch.batchKey,
        count: messageFailed,
      });
    }
    const firstMs = Date.parse(batch.firstAttemptAt ?? '');
    const firstAttemptMs = Number.isNaN(firstMs) ? now.getTime() : firstMs;
    const deadline = Math.min(alertEndTime(alert).getTime(), firstAttemptMs + RETRY_BUDGET_MS);
    if (failed.length > 0) {
      const random = deps.random ?? Math.random;
      const delay = retryDelaySeconds(batch.attempt, retryAfter, random);
      const nowMs = deps.clock.now().getTime();
      if (nowMs + delay * 1000 >= deadline) {
        gaveUp += failed.length;
        logger.error('delivery.gave_up', {
          alertId: alert.id,
          batchKey: batch.batchKey,
          attempt: batch.attempt,
          count: failed.length,
          reason: nowMs >= deadline ? 'budget_spent' : 'retry_would_miss_budget',
        });
      } else {
        const retryBatch: SendBatch = {
          ...batch,
          batchKey: retryKey(batch),
          recipients: failed,
          attempt: batch.attempt + 1,
          firstAttemptAt: new Date(firstAttemptMs).toISOString(),
        };
        // See the file header: a failure here releases the claim and fails the whole message.
        await deps.sendQueue.enqueue(
          QUEUE_FOR_SEVERITY[batch.relaySeverity] ?? 'standard',
          [retryBatch],
          { delaySeconds: delay },
        );
        retried = failed.length;
      }
    }

    // Only now is the batch finished for good. If this fails the pushes already went out;
    // ack anyway (failing the message would re-send once the lease runs out).
    await idempotency.markSent(batch.batchKey, claimExpiry(alert)).catch((err: unknown) => {
      logger.error('sender.mark_sent_failed', { batchKey: batch.batchKey, ...errorFields(err) });
    });

    // Cleanup comes after markSent and is asynchronous: submit is synchronous and cheap. The
    // gate holds every clear for a short delay and re-checks the target's platform before it
    // clears (dead-target-gate.ts). Held recipients carry their batch so the gate can re-queue
    // them if the platform turns out misconfigured.
    if (dead.length > 0) {
      gateFor(deps).submit({
        // Only the batch's identity, not its recipient list: the gate may hold this a while.
        source: {
          alertId: batch.alertId,
          sourceId: batch.sourceId,
          relaySeverity: batch.relaySeverity,
          batchKey: batch.batchKey,
          attempt: batch.attempt,
        },
        firstAttemptMs,
        deadlineMs: deadline,
        recipients: dead,
        since,
      });
    }

    logger.info('sender.batch_done', {
      alertId: alert.id,
      batchKey: batch.batchKey,
      attempt: batch.attempt,
      sent,
      permanentFailed: dead.length,
      retried,
      gaveUp,
    });
    return outcome(true, 'sent', { sent, permanentFailed: dead.length, retried, gaveUp });
  } catch (err) {
    // Re-enqueue failed, or the alert read / kill switch read threw. Give the claim back so
    // the redelivered message can take it immediately, and fail the message.
    await release();
    throw err;
  } finally {
    control.active = false;
  }
}
