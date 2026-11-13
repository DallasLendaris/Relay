import { alertEndsAtMs, type Alert } from '@relay/shared';
import { cellToChildren, cellToParent } from 'h3-js';
import { noopLogger, type Logger } from '../../delivery/types.js';
import type { AlertStore } from '../../ports.js';
import { SHARD_RESOLUTION } from '../contracts.js';
import {
  activeShardKey,
  publicAlertDocKey,
  publicAlertPointerKey,
  PUBLIC_CACHE_CONTROL,
  PUBLIC_DOC_CACHE_CONTROL,
} from './paths.js';
import { buildPointer, buildPublicDocument } from './public-document.js';

/** Where public objects go. S3 in AWS; an in-memory fake in tests. */
export interface PublicObjectWriter {
  putJson(key: string, body: string, cacheControl: string): Promise<void>;
}

/** Limits for one publish. Defaults: 16 shard files in flight, 15 s for the whole shard fan-out. */
export interface PublishOptions {
  concurrency?: number;
  /** Total time cap for the shard index files (the alert JSON itself is not capped). */
  deadlineMs?: number;
}

export const DEFAULT_PUBLISH_CONCURRENCY = 16;
export const DEFAULT_PUBLISH_DEADLINE_MS = 15_000;

/** Called by the ingest pipeline after the alert was handed to delivery. Must never throw. */
export interface PostSavePublisher {
  publish(saved: Alert, options?: PublishOptions): Promise<void>;
}

/** One entry of an active index file. */
export interface ActiveAlertSummary {
  id: string;
  event: string;
  relaySeverity: Alert['relaySeverity'];
  state: Alert['state'];
  expires: string;
  headline: string | null;
  /** Every res-4 shard the alert touches, so the app can tell when it spans shards. */
  shards: string[];
}

export interface ActiveIndexFile {
  version: 1;
  shard: string;
  generatedAt: string;
  alerts: ActiveAlertSummary[];
}

export function shardsOf(cells: readonly string[]): string[] {
  return [...new Set(cells.map((c) => cellToParent(c, SHARD_RESOLUTION)))].sort();
}

function isLive(a: Alert, now: Date): boolean {
  if (a.state !== 'active' && a.state !== 'updated') return false;
  return alertEndsAtMs(a) > now.getTime();
}

export function summarize(a: Alert): ActiveAlertSummary {
  return {
    id: a.id,
    event: a.event,
    relaySeverity: a.relaySeverity,
    state: a.state,
    expires: a.expires,
    headline: a.official.headline,
    shards: shardsOf(a.area.h3Cells),
  };
}

export interface AlertPublisherOptions {
  writer: PublicObjectWriter;
  /** Source of truth for "what is active in this shard". */
  alerts: Pick<AlertStore, 'listActiveForCells'>;
  /** Resolution of the alert cells (H3_RESOLUTION, 7). */
  cellResolution: number;
  logger?: Logger;
  clock?: { now(): Date };
}

/**
 * Publishes public alert data so phones read it from the CDN instead of the API:
 *   alerts/v1/doc/{alertId}/{hash}.json   cell-free alert document (immutable, versioned key)
 *   alerts/v1/latest/{alertId}.json       short-TTL pointer to the current document
 *   alerts/v1/active/{shard}.json         summaries of the alerts active in one res-4 shard
 * The full alert with its h3Cells is NOT published: nothing reads it, and a statewide one is 1.5 MB.
 * Never throws: a failure is logged as `publish.failed` and the API stays the fallback.
 */
export class AlertPublisher implements PostSavePublisher {
  private readonly logger: Logger;
  private readonly clock: { now(): Date };

  constructor(private readonly opts: AlertPublisherOptions) {
    this.logger = opts.logger ?? noopLogger;
    this.clock = opts.clock ?? { now: () => new Date() };
  }

  async publish(saved: Alert, options: PublishOptions = {}): Promise<void> {
    const concurrency = Math.max(1, options.concurrency ?? DEFAULT_PUBLISH_CONCURRENCY);
    const deadlineMs = options.deadlineMs ?? DEFAULT_PUBLISH_DEADLINE_MS;
    try {
      // The document goes first (it is what a push points at), then the pointer that names it.
      await this.opts.writer.putJson(
        publicAlertDocKey(saved),
        JSON.stringify(buildPublicDocument(saved)),
        PUBLIC_DOC_CACHE_CONTROL,
      );
      await this.opts.writer.putJson(
        publicAlertPointerKey(saved.id),
        JSON.stringify(buildPointer(saved)),
        PUBLIC_CACHE_CONTROL,
      );
      const now = this.clock.now();
      const shards = shardsOf(saved.area.h3Cells);
      // One store read for every shard instead of one per shard (statewide: ~1,700 reads, each
      // re-reading the big alert's cells). Alerts are then sorted into shards in memory.
      const live = await this.readLiveAlerts(shards, saved, now);
      let next = 0;
      let done = 0;
      let failed = 0;
      let timedOut = false;
      const worker = async (): Promise<void> => {
        while (!timedOut && next < shards.length) {
          const shard = shards[next++];
          if (shard === undefined) return;
          try {
            await this.publishShard(shard, live, now);
            done++;
          } catch (err) {
            failed++;
            this.logger.error('publish.shard_failed', {
              alertId: saved.id,
              shard,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        }
      };
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cap = new Promise<'timeout'>((resolve) => {
        timer = setTimeout(() => resolve('timeout'), deadlineMs);
      });
      const pool = Promise.all(
        Array.from({ length: Math.min(concurrency, shards.length) }, worker),
      );
      const outcome = await Promise.race([pool.then(() => 'done' as const), cap]);
      clearTimeout(timer);
      if (outcome === 'timeout') {
        timedOut = true; // no new shard starts; writes already in flight are abandoned
        this.logger.warn('publish.deadline_exceeded', {
          alertId: saved.id,
          shards: shards.length,
          published: done,
          failed,
          deadlineMs,
        });
      }
    } catch (err) {
      this.logger.error('publish.failed', {
        alertId: saved.id,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Every live alert touching any of `shards`, each with the res-4 shards it spans. The saved
   * alert is authoritative for its own id (the store read may be stale, and a cancelled or
   * expired alert must drop out of the index).
   */
  private async readLiveAlerts(
    shards: string[],
    saved: Alert,
    now: Date,
  ): Promise<Array<{ summary: ActiveAlertSummary; shardSet: Set<string> }>> {
    const cells = shards.flatMap((s) => cellToChildren(s, this.opts.cellResolution));
    const found = await this.opts.alerts.listActiveForCells(cells, now);
    const byId = new Map(found.filter((a) => isLive(a, now)).map((a) => [a.id, a]));
    if (isLive(saved, now)) byId.set(saved.id, saved);
    else byId.delete(saved.id);
    return [...byId.values()]
      .map((a) => {
        const summary = summarize(a);
        return { summary, shardSet: new Set(summary.shards) };
      })
      .sort((a, b) => a.summary.id.localeCompare(b.summary.id));
  }

  private async publishShard(
    shard: string,
    live: Array<{ summary: ActiveAlertSummary; shardSet: Set<string> }>,
    now: Date,
  ): Promise<void> {
    const alerts = live.filter((l) => l.shardSet.has(shard)).map((l) => l.summary);
    const index: ActiveIndexFile = { version: 1, shard, generatedAt: now.toISOString(), alerts };
    await this.opts.writer.putJson(
      activeShardKey(shard),
      JSON.stringify(index),
      PUBLIC_CACHE_CONTROL,
    );
  }
}
