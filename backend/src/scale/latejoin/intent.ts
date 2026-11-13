import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';

/**
 * "This device may have missed an active alert." The API sends this when a place is added or a
 * push target changes. It is a hint only: the late-join Lambda reads `deviceId`, re-reads the
 * device, its places and the active alerts from Aurora, and ignores everything else here. An
 * intent that names the wrong place, or a made-up one, therefore cannot choose a recipient,
 * an alert or a cell. See latejoin.ts.
 */
export interface LateJoinIntent {
  deviceId: string;
  placeId?: string;
  /**
   * 'outbox': the cell-index outbox drainer has just put this device in the index, so the planned
   * fan-out may not have seen it whatever its place's age says; the "place predates the alert"
   * skip is off for it (the per-target claim still prevents duplicates).
   */
  reason: 'place_added' | 'push_target' | 'outbox';
  /**
   * ISO time the intent was FIRST enqueued. Set when the Lambda parks an intent during a pause,
   * so the 24 h give-up counts from the original request, not from the latest re-enqueue.
   */
  enqueuedAt?: string;
  /**
   * ISO time late join FIRST deferred this intent because the planned fan-out might still carry
   * the device (G5). Set by the Lambda when it re-enqueues the intent; the wait is given up
   * PLAN_WAIT_GIVE_UP_MS after it (then the device is sent, at worst a duplicate).
   */
  waitingSince?: string;
}

/**
 * The only way to cause a late-join send. The API, the outbox drainer and the late-join Lambda
 * itself (to park an intent while delivery is paused) have permission to send to it.
 */
export interface LateJoinQueue {
  /** `delaySeconds` (SQS maximum 900) hides the message for that long. */
  enqueue(intent: LateJoinIntent, opts?: { delaySeconds?: number }): Promise<void>;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Only the device id is trusted to be present; everything else is advisory. */
export function isLateJoinIntent(v: unknown): v is LateJoinIntent {
  const o = v as Partial<LateJoinIntent> | null;
  return (
    typeof o === 'object' && o !== null && typeof o.deviceId === 'string' && UUID.test(o.deviceId)
  );
}

/**
 * How long an intent waits for the planned fan-out to settle (G5) before late join sends anyway.
 * A cell's plan record follows its read by seconds, and a planned batch is sent within about a
 * minute of being queued even in a 1M-device fan-out; 5 minutes leaves room for a backlog.
 */
export const PLAN_WAIT_GIVE_UP_MS = 5 * 60 * 1000;
/** The longest delay between two looks while waiting for the planned fan-out (the cap). */
export const PLAN_WAIT_DELAY_SECONDS = 15;
/** The delay before the first look again: most waits end within seconds (see planWaitDelaySeconds). */
export const PLAN_WAIT_FIRST_DELAY_SECONDS = 2;

/**
 * Delay before deciding a deferred intent again, given how long it has waited so far: half the
 * time waited, at least `first` and at most `cap` (2, 2, 2, 3, 5, 7, 11, 15, 15 ... s with the
 * defaults). A device whose planned batch goes out at time T after its first deferral is decided
 * by about T + max(first, T / 2), never later than T + cap, instead of up to a whole cap late:
 * a cell's plan record follows its read by seconds and a queued batch is usually sent within
 * seconds, so a fixed 15 s first delay was most of the wait. Later looks back off to the cap, so
 * a long wait costs about as many re-runs as before (about 25 in the 5 minute bound instead of 20).
 */
export function planWaitDelaySeconds(
  waitedMs: number,
  cap: number = PLAN_WAIT_DELAY_SECONDS,
  first: number = PLAN_WAIT_FIRST_DELAY_SECONDS,
): number {
  const half = Math.ceil(Math.max(0, waitedMs) / 2000);
  return Math.min(cap, MAX_DELAY_SECONDS, Math.max(first, half));
}

/** How long a paused intent keeps being parked before it is given up on. */
export const PARK_GIVE_UP_MS = 24 * 60 * 60 * 1000;

export class SqsLateJoinQueue implements LateJoinQueue {
  constructor(
    private readonly sqs: Pick<SQSClient, 'send'>,
    private readonly queueUrl: string,
  ) {}
  async enqueue(intent: LateJoinIntent, opts?: { delaySeconds?: number }): Promise<void> {
    await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(intent),
        ...(opts?.delaySeconds
          ? {
              DelaySeconds: Math.min(MAX_DELAY_SECONDS, Math.max(0, Math.floor(opts.delaySeconds))),
            }
          : {}),
      }),
    );
  }
}

/** SQS caps a message delay at 15 minutes. */
export const MAX_DELAY_SECONDS = 900;

/** For tests and local runs. */
export class MemoryLateJoinQueue implements LateJoinQueue {
  readonly intents: LateJoinIntent[] = [];
  /** Every enqueue ever made, with its delay (never consumed, unlike `intents`). */
  readonly log: Array<{ intent: LateJoinIntent; delaySeconds: number }> = [];
  async enqueue(intent: LateJoinIntent, opts?: { delaySeconds?: number }): Promise<void> {
    this.intents.push(intent);
    this.log.push({ intent, delaySeconds: opts?.delaySeconds ?? 0 });
  }
}
