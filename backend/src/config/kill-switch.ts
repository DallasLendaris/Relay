import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { configMetric, errorName, logStaleServed } from './stale.js';

/**
 * EMF metrics (namespace Relay/Config, dimension Source=killswitch), emitted on every real SSM
 * read (not on cache hits), so alarms can tell a quiet pause from a broken switch:
 * - KillSwitchPaused: gauge, 1 when the read decided "paused" for any reason, else 0.
 * - KillSwitchInvalidValue: count; the parameter held something other than running/paused.
 * - KillSwitchFailedClosed: count; SSM was unreadable and no value younger than the stale limit
 *   existed (never loaded, or expired), so delivery was paused by default.
 * - KillSwitchParameterMissing: count; the parameter does not exist (treated as running).
 */
export const KILL_SWITCH_METRIC_SOURCE = 'killswitch';

/** How long a read is cached; a ShardJob's passed-in kill-switch value is trusted for the same time. */
export const KILL_SWITCH_TTL_MS = 30 * 1000;
const TTL_MS = KILL_SWITCH_TTL_MS;
/** How long the last good value is served when SSM cannot be read (throttling, an outage). */
export const KILL_SWITCH_STALE_MS = 10 * 60 * 1000;
/** After a failed read, wait this long before asking SSM again. */
const RETRY_MS = 5 * 1000;

export interface KillSwitchOptions {
  /**
   * Random delay (0 to this many ms) before the first SSM read of a container, so hundreds of
   * cold shard workers do not read the parameter in the same instant. Default 0.
   */
  coldJitterMs?: number;
  /** Staleness limit for the last good value. Default 10 minutes. */
  staleMs?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

/**
 * Kill switch backed by an SSM String parameter. Returns () => Promise<boolean> (true = paused),
 * cached for 30 seconds.
 *
 * Only exactly "running" or "paused" (after trim and lower-casing) are understood. Any other
 * value ("pause", "true", "") is treated as PAUSED, so a typo stops delivery instead of leaving
 * it running; it is logged as `killswitch.invalid_value` (error, with a metric). A missing
 * parameter means running.
 *
 * When SSM cannot be read, the last good value is served for up to 10 minutes
 * (`config.stale_value_served`). With no usable value (never loaded, or older than that) the
 * switch fails closed: paused. It never throws.
 */
export function createKillSwitch(
  paramName: string,
  client: Pick<SSMClient, 'send'> = new SSMClient({}),
  now: () => number = Date.now,
  opts: KillSwitchOptions = {},
): () => Promise<boolean> {
  const staleMs = opts.staleMs ?? KILL_SWITCH_STALE_MS;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const random = opts.random ?? Math.random;
  let cached: { at: number; paused: boolean; retryAt?: number } | undefined;
  let inflight: Promise<boolean> | undefined;

  async function read(): Promise<boolean> {
    if (!cached && opts.coldJitterMs) await sleep(Math.floor(random() * opts.coldJitterMs));
    const t = now();
    try {
      const out = await client.send(new GetParameterCommand({ Name: paramName }));
      const raw = out.Parameter?.Value;
      const value = raw?.trim().toLowerCase();
      let paused: boolean;
      if (value === 'running') paused = false;
      else if (value === 'paused') paused = true;
      else {
        paused = true;
        console.error(
          JSON.stringify({
            level: 'error',
            msg: 'killswitch.invalid_value',
            length: raw?.length ?? 0,
          }),
        );
        console.log(configMetric('KillSwitchInvalidValue', KILL_SWITCH_METRIC_SOURCE, t));
      }
      console.log(configMetric('KillSwitchPaused', KILL_SWITCH_METRIC_SOURCE, t, paused ? 1 : 0));
      cached = { at: now(), paused };
      return paused;
    } catch (err) {
      if ((err as { name?: string }).name === 'ParameterNotFound') {
        console.warn(JSON.stringify({ level: 'warn', msg: 'killswitch.parameter_missing' }));
        console.log(configMetric('KillSwitchParameterMissing', KILL_SWITCH_METRIC_SOURCE, t));
        console.log(configMetric('KillSwitchPaused', KILL_SWITCH_METRIC_SOURCE, t, 0));
        cached = { at: now(), paused: false };
        return false;
      }
      if (cached && t - cached.at < staleMs) {
        cached = { ...cached, retryAt: t + RETRY_MS };
        logStaleServed(KILL_SWITCH_METRIC_SOURCE, t - cached.at, err, t);
        return cached.paused;
      }
      console.error(
        JSON.stringify({
          level: 'error',
          msg: 'killswitch.read_failed_closed',
          error: errorName(err),
        }),
      );
      console.log(configMetric('KillSwitchFailedClosed', KILL_SWITCH_METRIC_SOURCE, t));
      console.log(configMetric('KillSwitchPaused', KILL_SWITCH_METRIC_SOURCE, t, 1));
      return true;
    }
  }

  return async () => {
    const t = now();
    if (cached && t - cached.at < TTL_MS) return cached.paused;
    if (cached && cached.retryAt !== undefined && t < cached.retryAt && t - cached.at < staleMs) {
      return cached.paused;
    }
    inflight ??= read().finally(() => (inflight = undefined));
    return inflight;
  };
}
