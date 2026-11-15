/**
 * `npm run simulate`: runs the PoC path once, offline, and exits.
 * A saved NWS response is replayed through the real engine and API; pushes are logged instead
 * of sent. Exits 0 when every step holds, 1 otherwise.
 *
 * With a real recording (backend/dev-data/replay/recorded-*.json), the phones are placed from the
 * alerts that recording actually holds. If it holds no alert that would be pushed (California
 * often has none), the run says so and uses the constructed sample instead.
 */
import { join, relative } from 'node:path';
import { loadLocalConfig, REPLAY_DIR } from '../src/local/config.js';
import { pickDemoPoints } from '../src/local/pick-points.js';
import { createLocalRelay } from '../src/local/relay.js';
import { runScenario, type ScenarioResult } from '../src/local/scenario.js';

const SAMPLE = join(REPLAY_DIR, 'constructed-sample.json');
const SAMPLE_ZONES = join(REPLAY_DIR, 'constructed-sample.zones.json');

function relayFor(replayFile: string, replayZonesFile?: string) {
  return () =>
    createLocalRelay({
      feed: 'replay',
      push: 'log',
      replayFile,
      ...(replayZonesFile ? { replayZonesFile } : {}),
    });
}

async function runOn(
  replayFile: string,
  replayZonesFile: string | undefined,
): Promise<ScenarioResult | null> {
  const make = relayFor(replayFile, replayZonesFile);
  console.log(`Feed file: ${relative(process.cwd(), replayFile)}`);
  console.log(`Clock set to the feed's time: ${make().clock.now().toISOString()}`);

  if (replayFile === SAMPLE) return runScenario(make(), (l) => console.log(l));

  const points = await pickDemoPoints(make);
  if (!points.inside.length || !points.outside) {
    console.log(
      points.inside.length
        ? 'Every control location is under an alert in this recording.'
        : 'This recording has no alert that would be pushed (only statements, or none at all).',
    );
    return null;
  }
  // Try a few points: a cell centre can land where /places refuses it (outside the state line).
  let lastError: unknown;
  for (const p of points.inside.slice(0, 8)) {
    console.log(
      `Placing the phone inside the ${p.event} (${p.latitude.toFixed(3)}, ${p.longitude.toFixed(3)})\n`,
    );
    try {
      return await runScenario(make(), (l) => console.log(l), p, points.outside);
    } catch (err) {
      lastError = err;
      console.log(
        `  that point did not work: ${err instanceof Error ? err.message : String(err)}\n`,
      );
    }
  }
  throw lastError;
}

const config = loadLocalConfig({ ...process.env, RELAY_FEED: 'replay', RELAY_PUSH: 'log' });
console.log('Relay simulate: replaying a saved NWS response through the real engine');

try {
  let result = await runOn(config.replayFile, config.replayZonesFile);
  if (!result) {
    console.log('Falling back to the constructed sample.\n');
    result = await runOn(SAMPLE, SAMPLE_ZONES);
  }
  if (!result) throw new Error('no run completed');
  console.log('\nPASS', JSON.stringify(result));
  process.exit(0);
} catch (err) {
  console.error('\nFAIL', err instanceof Error ? err.message : err);
  process.exit(1);
}
