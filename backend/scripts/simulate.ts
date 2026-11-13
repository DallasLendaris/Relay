/**
 * `npm run simulate`: runs the PoC path once, offline, and exits.
 * A saved NWS response is replayed through the real engine and API; pushes are logged instead
 * of sent. Exits 0 when every step holds, 1 otherwise.
 */
import { relative } from 'node:path';
import { loadLocalConfig } from '../src/local/config.js';
import { createLocalRelay } from '../src/local/relay.js';
import { runScenario } from '../src/local/scenario.js';

const config = loadLocalConfig({ ...process.env, RELAY_FEED: 'replay', RELAY_PUSH: 'log' });
console.log('Relay simulate: replaying a saved NWS response through the real engine');
console.log(`Feed file: ${relative(process.cwd(), config.replayFile)}`);

const relay = createLocalRelay({
  feed: 'replay',
  push: 'log',
  replayFile: config.replayFile,
  ...(config.replayZonesFile ? { replayZonesFile: config.replayZonesFile } : {}),
});
console.log(`Clock set to the feed's time: ${relay.clock.now().toISOString()}\n`);

try {
  const result = await runScenario(relay, (line) => console.log(line));
  console.log('\nPASS', JSON.stringify(result));
  process.exit(0);
} catch (err) {
  console.error('\nFAIL', err instanceof Error ? err.message : err);
  process.exit(1);
}
