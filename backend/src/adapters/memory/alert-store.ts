import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';

/** In-memory AlertStore for tests and local runs. Stores copies so callers can't mutate it. */
export class InMemoryAlertStore implements AlertStore {
  private readonly alerts = new Map<string, Alert>();

  async get(id: string): Promise<Alert | null> {
    const alert = this.alerts.get(id);
    return alert ? structuredClone(alert) : null;
  }

  /** Matches the current sourceId or any sourceId earlier in the chain. */
  async findBySourceId(sourceId: string): Promise<Alert | null> {
    for (const alert of this.alerts.values()) {
      if (alert.sourceId === sourceId || alert.references.some((r) => r.sourceId === sourceId)) {
        return structuredClone(alert);
      }
    }
    return null;
  }

  async save(alert: Alert): Promise<void> {
    this.alerts.set(alert.id, structuredClone(alert));
  }

  async listActiveForCells(cells: string[], now: Date): Promise<Alert[]> {
    const wanted = new Set(cells);
    const endsAt = (a: Alert) => Math.max(Date.parse(a.expires), a.ends ? Date.parse(a.ends) : 0);
    return [...this.alerts.values()]
      .filter((a) => a.state === 'active' || a.state === 'updated')
      .filter((a) => endsAt(a) > now.getTime())
      .filter((a) => a.area.h3Cells.some((c) => wanted.has(c)))
      .map((a) => structuredClone(a));
  }

  /** Every active or updated alert of one source that has not ended yet (feed reconciliation). */
  async listActiveBySource(source: Alert['source'], now: Date): Promise<Alert[]> {
    const endsAt = (a: Alert) => Math.max(Date.parse(a.expires), a.ends ? Date.parse(a.ends) : 0);
    return [...this.alerts.values()]
      .filter((a) => a.source === source && (a.state === 'active' || a.state === 'updated'))
      .filter((a) => endsAt(a) > now.getTime())
      .map((a) => structuredClone(a));
  }
}
