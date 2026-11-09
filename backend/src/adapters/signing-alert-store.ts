import type { Alert } from '@relay/shared';
import type { AlertStore } from '../ports.js';
import { signAlert, type SigningKey } from '../signing/index.js';
import { summariesFor } from '../summaries/index.js';

/**
 * Decorator that fills in summaries and signs every alert on save. Signing has to be
 * the last step, after h3Cells and summaries are final, or the signature would not
 * match what is stored. Reads pass straight through.
 */
export class SigningAlertStore implements AlertStore {
  constructor(
    private readonly inner: AlertStore,
    private readonly currentKey: SigningKey,
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

  async save(alert: Alert): Promise<void> {
    const withSummaries: Alert = {
      ...alert,
      // Canonical cell order (sorted, unique) so every store returns exactly what was signed.
      area: { ...alert.area, h3Cells: [...new Set(alert.area.h3Cells)].sort() },
      summaries: summariesFor(alert, { timeZone: 'America/Los_Angeles' }),
    };
    await this.inner.save(signAlert(withSummaries, this.currentKey));
  }
}
