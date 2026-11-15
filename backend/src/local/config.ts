/** Reads local-mode settings from the environment. See .env.example at the repo root. */
import { existsSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { FeedMode, PushMode } from './relay.js';

const here = dirname(fileURLToPath(import.meta.url));
/** backend/dev-data/replay, where saved NWS responses live. */
export const REPLAY_DIR = resolve(here, '../../dev-data/replay');

export interface LocalConfig {
  feed: FeedMode;
  push: PushMode;
  replayFile: string;
  replayZonesFile?: string;
  expoAccessToken?: string;
  host: string;
  port: number;
  pollSeconds: number;
}

/**
 * The replay file: RELAY_REPLAY_FILE if set, else the newest recording made by
 * `npm run capture-nws` (recorded-*.json), else the constructed sample shipped in the repo.
 * Its zones file is the same name with `.zones.json`, when present.
 */
export function pickReplayFile(env: Record<string, string | undefined>): {
  replayFile: string;
  replayZonesFile?: string;
} {
  const chosen = env.RELAY_REPLAY_FILE
    ? resolve(env.RELAY_REPLAY_FILE)
    : (() => {
        const recorded = existsSync(REPLAY_DIR)
          ? readdirSync(REPLAY_DIR)
              .filter((f) => /^recorded-.*(?<!\.zones)\.json$/.test(f))
              .sort()
          : [];
        const newest = recorded.at(-1);
        return join(REPLAY_DIR, newest ?? 'constructed-sample.json');
      })();
  const zones = chosen.replace(/\.json$/, '.zones.json');
  return { replayFile: chosen, ...(existsSync(zones) ? { replayZonesFile: zones } : {}) };
}

function oneOf<T extends string>(
  name: string,
  value: string | undefined,
  allowed: T[],
  fallback: T,
): T {
  if (value === undefined || value === '') return fallback;
  if ((allowed as string[]).includes(value)) return value as T;
  throw new Error(`${name} must be one of ${allowed.join(', ')} (got "${value}")`);
}

export function loadLocalConfig(
  env: Record<string, string | undefined> = process.env,
): LocalConfig {
  const feed = oneOf('RELAY_FEED', env.RELAY_FEED, ['live', 'replay'], 'replay');
  const push = oneOf('RELAY_PUSH', env.RELAY_PUSH, ['expo', 'log'], 'log');
  const port = Number(env.PORT || 8787);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('PORT must be 1-65535');
  const pollSeconds = Number(env.RELAY_POLL_SECONDS || 30);
  // NWS asks clients not to poll more often than every 30 seconds.
  if (!(pollSeconds >= 30)) throw new Error('RELAY_POLL_SECONDS must be at least 30');
  return {
    feed,
    push,
    ...pickReplayFile(env),
    ...(env.EXPO_ACCESS_TOKEN ? { expoAccessToken: env.EXPO_ACCESS_TOKEN } : {}),
    host: env.HOST || '0.0.0.0',
    port,
    pollSeconds,
  };
}
