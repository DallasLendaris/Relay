import type { Alert } from '@relay/shared';
import type { AlertStore } from '../ports.js';
import { signAlert, type AlertSigner } from '../signing/alertSignature.js';
import type { ActiveAlertLister } from '../ingest/poll.js';
import { summariesFor } from '../summaries/index.js';

/**
 * Decorator that fills in summaries and signs every alert on save. Signing has to be
 * the last step, after h3Cells and summaries are final, or the signature would not
 * match what is stored. Reads pass straight through.
 */
export class SigningAlertStore implements AlertStore {
  constructor(
    private readonly inner: AlertStore,
    private readonly signer: AlertSigner,
  ) {}

  get(id: string) {
    return this.inner.get(id);
  }
  findBySourceId(sourceId: string) {
    return this.inner.findBySourceId(sourceId);
  }
  listActiveForCells(cells: string[], now: Date) {
    return this.inner.listActiveForCells(cells, now);
  }
  /** Feed reconciliation: every active alert of one source. Needs an inner store that has it. */
  async listActiveBySource(source: Alert['source'], now: Date): Promise<Alert[]> {
    const inner = this.inner as Partial<ActiveAlertLister>;
    if (!inner.listActiveBySource) throw new Error('alert store cannot list active alerts');
    return inner.listActiveBySource(source, now);
  }

  /** Feed restoration: cancelled alerts of one source that have not ended yet. */
  async listCancelledBySource(source: Alert['source'], now: Date): Promise<Alert[]> {
    const inner = this.inner as Partial<ActiveAlertLister>;
    if (!inner.listCancelledBySource) throw new Error('alert store cannot list cancelled alerts');
    return inner.listCancelledBySource(source, now);
  }

  async save(alert: Alert): Promise<void> {
    const withSummaries: Alert = {
      ...alert,
      // Canonical cell order (sorted, unique) so every store returns exactly what was signed.
      area: { ...alert.area, h3Cells: [...new Set(alert.area.h3Cells)].sort() },
      // A cancelled alert never carries summaries: regenerating them would say "in effect", and
      // earlier ones are dropped. summariesFor returns [] for a cancelled alert.
      summaries: summariesFor(alert, { timeZone: 'America/Los_Angeles' }),
    };
    const signed = await signAlert(withSummaries, this.signer);
    // Public CDN publishing is NOT done here: it is slow (a Data API query and a cross-region PUT
    // per shard), so the ingest pipeline runs it after AlertReady has been published.
    await this.inner.save(signed);
  }
}
