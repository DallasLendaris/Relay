import { noopLogger, type Logger } from '../delivery/types.js';
import type { ZoneGeometry } from './s3-zone-provider.js';
import type { ZoneGeometryProvider } from './zones.js';

/**
 * Asks each provider in turn and returns the first geometry found. providers[0] is
 * the pack, and the rest are fallbacks, normally NWS.
 *
 * - A null from a provider is a miss, so the next provider is asked.
 * - A throw from a provider that is not last is logged, and the next provider is asked.
 * - A throw from the last provider propagates, so a network error still rejects.
 * - A 404 from NWS is null, so an unknown zone still means "no geometry".
 */
export class LayeredZoneProvider implements ZoneGeometryProvider {
  private readonly missLogged = new Set<string>();
  private readonly errorLogged = new Set<string>();

  constructor(
    private readonly providers: readonly ZoneGeometryProvider[],
    private readonly logger: Logger = noopLogger,
  ) {
    if (providers.length === 0) throw new Error('LayeredZoneProvider needs at least one provider');
  }

  async getZoneGeometry(ugc: string): Promise<ZoneGeometry | null> {
    const code = ugc.toUpperCase();
    const last = this.providers.length - 1;
    for (const [index, provider] of this.providers.entries()) {
      let geometry: ZoneGeometry | null;
      try {
        geometry = await provider.getZoneGeometry(code);
      } catch (err) {
        if (index === last) throw err;
        this.logError(index, err);
        continue;
      }
      if (geometry) return geometry;
      if (index === 0) this.logMiss(code);
    }
    return null;
  }

  private logMiss(code: string): void {
    if (this.missLogged.has(code)) return;
    this.missLogged.add(code);
    this.logger.info('zones.pack_miss', { ugc: code });
  }

  private logError(index: number, err: unknown): void {
    const message = err instanceof Error ? err.message : String(err);
    // A dead pack throws on every lookup, so log each distinct message once per instance.
    const key = `${index}:${message}`;
    if (this.errorLogged.has(key)) return;
    this.errorLogged.add(key);
    this.logger.warn('zones.pack_error', { provider: index, error: message });
  }
}
