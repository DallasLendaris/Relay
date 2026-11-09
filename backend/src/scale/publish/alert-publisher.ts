import type { Alert } from '@relay/shared';
import { cellToChildren, cellToParent } from 'h3-js';
import { noopLogger, type Logger } from '../../delivery/types.js';
import type { AlertStore } from '../../ports.js';
import { SHARD_RESOLUTION } from '../contracts.js';
import { activeShardKey, publicAlertKey, PUBLIC_CACHE_CONTROL } from './paths.js';

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
  const endsAt = Math.max(Date.parse(a.expires), a.ends ? Date.parse(a.ends) : 0);
  return endsAt > now.getTime();
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
 *   alerts/v1/{alertId}.json          the full signed Alert (public data)
 *   alerts/v1/active/{shard}.json     summaries of the alerts active in one res-4 shard
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
      // The alert JSON goes first: it is the one object every phone needs.
      await this.opts.writer.putJson(
        publicAlertKey(saved.id),
        JSON.stringify(saved),
        PUBLIC_CACHE_CONTROL,
      );
      const now = this.clock.now();
      const shards = shardsOf(saved.area.h3Cells);
      let next = 0;
      let done = 0;
      let failed = 0;
      let timedOut = false;
      const worker = async (): Promise<void> => {
        while (!timedOut && next < shards.length) {
          const shard = shards[next++];
          if (shard === undefined) return;
          try {
            await this.publishShard(shard, saved, now);
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

  private async publishShard(shard: string, saved: Alert, now: Date): Promise<void> {
    // A res-4 shard has 343 res-7 children: ask the store about all of them.
    const found = await this.opts.alerts.listActiveForCells(
      cellToChildren(shard, this.opts.cellResolution),
      now,
    );
    // The saved alert is authoritative for its own id (the store read may be stale, and a
    // cancelled or expired alert must drop out of the index).
    const byId = new Map(found.filter((a) => isLive(a, now)).map((a) => [a.id, a]));
    if (isLive(saved, now)) byId.set(saved.id, saved);
    else byId.delete(saved.id);
    const alerts = [...byId.values()].map(summarize).sort((a, b) => a.id.localeCompare(b.id));
    const index: ActiveIndexFile = { version: 1, shard, generatedAt: now.toISOString(), alerts };
    await this.opts.writer.putJson(
      activeShardKey(shard),
      JSON.stringify(index),
      PUBLIC_CACHE_CONTROL,
    );
  }
}
