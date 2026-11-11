import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';

/** In-memory AlertStore for tests and local runs. Stores copies so callers can't mutate it. */
export class InMemoryAlertStore implements AlertStore {
  private readonly alerts = new Map<string, Alert>();
  /** `${alertId}\n${sourceId}` -> cells that version dropped (Postgres: alert_dropped_cells). */
  private readonly dropped = new Map<string, Set<string>>();

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
    const before = this.alerts.get(alert.id);
    if (before && before.sourceId !== alert.sourceId) {
      // Same rule as the Postgres store: a new version records the cells it no longer covers.
      const now = new Set(alert.area.h3Cells);
      const gone = before.area.h3Cells.filter((c) => !now.has(c));
      if (gone.length > 0) {
        const key = `${alert.id}\n${alert.sourceId}`;
        const set = this.dropped.get(key) ?? new Set<string>();
        for (const c of gone) set.add(c);
        this.dropped.set(key, set);
      }
    }
    this.alerts.set(alert.id, structuredClone(alert));
  }

  async listDroppedCells(alertId: string, sourceId: string): Promise<string[]> {
    return [...(this.dropped.get(`${alertId}\n${sourceId}`) ?? [])].sort();
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

  /** Cancelled alerts of one source whose end is still in the future (feed restoration). */
  async listCancelledBySource(source: Alert['source'], now: Date): Promise<Alert[]> {
    const endsAt = (a: Alert) => Math.max(Date.parse(a.expires), a.ends ? Date.parse(a.ends) : 0);
    return [...this.alerts.values()]
      .filter((a) => a.source === source && a.state === 'cancelled')
      .filter((a) => endsAt(a) > now.getTime())
      .map((a) => structuredClone(a));
  }
}
