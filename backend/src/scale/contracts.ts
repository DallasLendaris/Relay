/**
 * Statewide-scale contracts. These are the shared shapes between the pieces that let one alert
 * reach 1,000,000+ phones in about a minute and absorb the check-in burst that follows.
 * See docs/scale-plan.md for targets and the data flow.
 *
 * Pieces (each owned by one module):
 *   cell index (src/scale/cell-index) → fan-out planner (src/scale/fanout) → send queues →
 *   sender service on Fargate (src/scale/sender) → APNs / FCM adapters (src/adapters/push) →
 *   phones → check-in API (src/api) → check-in stream aggregator (src/scale/checkins).
 *
 * Changing anything here affects several modules. Keep changes small and update every
 * implementation in the same pull request.
 */
import type { Alert } from '@relay/shared';

// ---------- Push targets ----------

/**
 * Where a phone receives pushes. Production sends directly to Apple (APNs) and Google (FCM);
 * Expo stays for development builds only, because its push service is limited to about
 * 600 notifications per second per project.
 */
export type PushPlatform = 'ios' | 'android' | 'expo';

export interface PushTarget {
  platform: PushPlatform;
  /** APNs device token (hex), FCM registration token, or ExponentPushToken[...]. */
  token: string;
  /**
   * Android only: id of the Firebase project the FCM token was registered with (see
   * adapters/push/fcm-projects.ts). A token only works with its own project, so the sender
   * routes by it. Absent means the first (default) configured project.
   */
  project?: string;
}

// ---------- Cell index ----------

/**
 * One saved place of one device, indexed by its H3 cell (resolution 7). This is the only
 * data the hot fan-out path reads, so it carries everything needed to send.
 * DynamoDB table: pk = h3Cell, sk = `${deviceId}#${placeId}`.
 */
export interface CellIndexEntry {
  h3Cell: string;
  deviceId: string;
  placeId: string;
  /** Null when the device has no working push target yet; such entries are skipped. */
  target: PushTarget | null;
  language: 'en' | 'es';
  /**
   * The sorted, de-duplicated H3 cells of ALL of this device's saved places (at most 5), the same
   * list on every one of the device's entries. The shard worker uses it to send a multi-cell
   * device from exactly one cell per alert (the smallest cell of the alert area, see
   * fanout/primary-cell.ts). Optional for backward compatibility: an entry without it is
   * always sent (fail open: a duplicate push is better than a missed one).
   */
  deviceCells?: string[];
}

export interface CellIndexPage {
  entries: CellIndexEntry[];
  /** Opaque; pass back to get the next page. Absent on the last page. */
  cursor?: string;
}

export interface CellIndex {
  /** Insert or replace entries (write-through from the places API, and backfill). */
  put(entries: CellIndexEntry[]): Promise<void>;
  /** Remove one place's entry. */
  remove(h3Cell: string, deviceId: string, placeId: string): Promise<void>;
  /**
   * Update the push target and language on every entry for this device
   * (the caller passes the device's cells, from its places).
   */
  updateDevice(
    deviceId: string,
    cells: Array<{ h3Cell: string; placeId: string }>,
    patch: { target: PushTarget | null; language: 'en' | 'es' },
  ): Promise<void>;
  /** All entries in one cell, paged (DynamoDB Query). Default limit 1,000. */
  listForCell(h3Cell: string, cursor?: string, limit?: number): Promise<CellIndexPage>;
}

// ---------- Fan-out messages ----------

/**
 * "This alert is signed, saved and ready to deliver." Published by ingest after planning
 * succeeds today's checks. Small on purpose: the planner re-reads and re-verifies the alert.
 */
export interface AlertReady {
  alertId: string;
  /** Alert.sourceId at the time of publishing, so a stale message for an old version is ignored. */
  sourceId: string;
  relaySeverity: Alert['relaySeverity'];
}

/**
 * One shard of an alert's area: the alert cells that share one coarse H3 parent
 * (resolution SHARD_RESOLUTION). One shard worker handles one ShardJob.
 */
export interface ShardJob {
  alertId: string;
  sourceId: string;
  relaySeverity: Alert['relaySeverity'];
  shardId: string;
  cells: string[];
  /**
   * The kill switch as the planner read it, so hundreds of shard workers do not each read SSM
   * (PERF-4c). A worker uses it only while it is younger than the worker's own cache TTL
   * (KILL_SWITCH_TTL_MS) and reads SSM itself when it is absent or older. `readAtMs` is epoch ms.
   */
  killSwitch?: { paused: boolean; readAtMs: number };
}

/** Coarse H3 resolution used to shard alert areas (a res-4 cell is about 1,770 km²). */
export const SHARD_RESOLUTION = 4;

/**
 * Each H3 cell's devices are split into this many stable buckets (bucket = hash(deviceId) mod 64).
 * In a normal cell one SendBatch is exactly one (cell, bucket), so its key never changes when
 * devices are added or removed elsewhere: re-planning can never double-send or skip a device
 * because batch boundaries moved. 64 keeps cells of up to about 20,000 devices near 300
 * recipients per batch, well inside BATCH_BYTE_BUDGET. Denser cells: see DENSE_CELL_RECIPIENTS.
 */
export const CELL_BUCKETS = 64;

/**
 * A cell with more sendable devices than this (or any bucket whose batch would exceed
 * BATCH_BYTE_BUDGET) is "dense". Real metro cells (LA, SF, San Diego) hold 75,000-150,000 devices:
 * one (cell, bucket) batch of that is about 2,300 recipients, 0.5 MB, over the SQS message limit.
 *
 * A dense cell splits every bucket into DENSE_PARTS fixed parts by the first character of the
 * device id (fanout/hash.ts densePartFor: balanced for UUIDs, and monotone in cell-index sort
 * order, so the shard worker streams the cell part by part instead of holding it in memory).
 * Key: `${alertId}:${relaySeverity}:${h3Cell}:${bucket}.${part}/${DENSE_PARTS}`.
 *
 * Exactly-once when a cell changes tier between two plans of the same alert version (devices
 * added or removed): there are only two tiers, and before any part of bucket b is enqueued the
 * shard worker claims the bucket's normal key `...:${bucket}` and marks it sent (a tombstone),
 * after first marking `...:${bucket}:dense`. So
 *  - normal plan, then dense re-plan: if the normal batch was already sent (or is being sent) the
 *    claim fails and the bucket is skipped (it is covered); if it is still queued, the tombstone
 *    makes the sender drop it as a duplicate and the parts carry its devices.
 *  - dense plan, then normal re-plan: the normal key is tombstoned, so the sender drops it.
 *  - dense, then dense again: part keys depend only on (bucket, first character), so they repeat.
 * A cell marker written before the tombstones keeps the cell dense for every later plan of the
 * version, so a dense attempt that dies before enqueuing its parts is finished by the next run.
 * A crash between marks fails toward a duplicate push, never a miss. Late join follows a
 * tombstoned normal key to the device's part key (fanout/keys.ts plannedKeyFor). See shard-worker.ts.
 */
export const DENSE_CELL_RECIPIENTS = 20_000;
export const DENSE_PARTS = 16;

/**
 * Serialized-size budget of one SendBatch (JSON, check-in tokens included). The shard worker
 * measures batches against it; it leaves room under the 256 KB SQS limit for the message array.
 */
export const BATCH_BYTE_BUDGET = 200 * 1024;

/** The sender sends a batch's recipients in sub-chunks of this size (concurrent HTTP/2 streams). */
export const SENDER_CHUNK_SIZE = 100;

/**
 * Several small SendBatches may share one SQS message to cut message count in sparse areas. Packing
 * (fanout/pack.ts) also caps a message by measured bytes, not only by this count.
 */
export const MAX_BATCHES_PER_MESSAGE = 10;

/**
 * A batch of recipients for one alert. Batches are deterministic: the same alert version,
 * cell and bucket (and, in a dense cell, part) always produce the same batchKey, so re-planning
 * never double-sends. The sender claims `batchKey` (lease → markSent) in the IdempotencyStore
 * before sending.
 */
export interface SendBatch {
  alertId: string;
  sourceId: string;
  relaySeverity: Alert['relaySeverity'];
  /**
   * Normal cell: `${alertId}:${relaySeverity}:${h3Cell}:${bucket}`. Dense cell (DENSE_CELL_RECIPIENTS):
   * `...:${bucket}.${part}/${DENSE_PARTS}`. A batch still over the byte budget (pathological: about
   * 800+ recipients in one part) is halved into `<key>:s<path>` (path of 0/1 digits; not stable if
   * devices are added). Late join: `${alertId}:${relaySeverity}:late:<hash>`. A retry of failed
   * recipients is any of these plus `:r<attempt>`.
   */
  batchKey: string;
  recipients: Array<{
    deviceId: string;
    target: PushTarget;
    language: 'en' | 'es';
    /** Signed check-in token for this device and alert, with the h3Cell bound in (token v2). */
    checkInToken: string;
  }>;
  /** 0 for the first attempt; failed recipients are re-queued as a new batch with attempt + 1. */
  attempt: number;
  /** ISO time the batch was first processed; retries carry it so the 30 minute retry budget is shared. */
  firstAttemptAt?: string;
}

/** Send queues are split by severity so emergencies never wait behind advisories. */
export type SendQueueName = 'urgent' | 'standard';
export const QUEUE_FOR_SEVERITY: Record<Alert['relaySeverity'], SendQueueName | null> = {
  emergency: 'urgent',
  warning: 'urgent',
  watch: 'standard',
  advisory: 'standard',
  statement: null, // in-app only, see delivery/policy.ts
};

export interface SendQueue {
  /** `delaySeconds` hides the messages from consumers for that long (SQS caps it at 900). */
  enqueue(
    queue: SendQueueName,
    batches: SendBatch[],
    opts?: { delaySeconds?: number },
  ): Promise<void>;
}

// ---------- Native push ----------

/** A push as sent to APNs or FCM directly. */
export interface NativePushMessage {
  target: PushTarget;
  title: string;
  body: string;
  /** Delivered to the app; values are strings. Includes alertId, checkInToken, expiresAt. */
  data: Record<string, string>;
  /** 'high' wakes the phone and plays sound; 'normal' is quiet (see delivery/policy.ts). */
  priority: 'high' | 'normal';
  sound: boolean;
  /** Collapse/thread id so updates to the same alert replace each other on the lock screen. */
  collapseKey: string;
  /** Seconds after which APNs/FCM stop trying (the alert's end time). */
  ttlSeconds: number;
  /** iOS category / Android click action that shows the "I'm okay" button. */
  category: 'alert-checkin';
}

export type NativePushResult =
  | { ok: true }
  | {
      ok: false;
      /** permanent: the token is dead (unregistered / invalid). retryable: anything else. */
      kind: 'permanent' | 'retryable';
      error: string;
      /**
       * Only with kind 'permanent': 'message' means this message can never be sent (for example the
       * payload is too large) but the token is fine, so the target must NOT be cleared. Absent
       * means the token itself is dead.
       */
      scope?: 'message';
      /** Server-requested wait (Retry-After) in seconds, on retryable failures. */
      retryAfterSeconds?: number;
    };

export interface NativePushSender {
  /** One result per message, in order. Never throws for per-message failures. */
  send(messages: NativePushMessage[]): Promise<NativePushResult[]>;
}

// ---------- Check-ins at scale ----------

/**
 * A check-in event as written to the check-ins table (pk = `${alertId}#${deviceId}`).
 * DynamoDB Streams feed the aggregator, which adds up totals per `${alertId}#${h3Cell}` in
 * batches, so a million check-ins never hit one hot counter row.
 */
export interface CheckInEvent {
  alertId: string;
  deviceId: string;
  h3Cell: string;
  checkedInAt: string;
}
