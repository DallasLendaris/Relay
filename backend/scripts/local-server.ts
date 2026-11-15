/**
 * `npm run local`: the Relay API and engine on this machine, no AWS.
 * Settings come from the environment (see .env.example). Defaults: replay a saved NWS response,
 * log pushes instead of sending them, listen on 0.0.0.0:8787, poll every 30 seconds.
 *
 * For the phone demo: RELAY_FEED=live RELAY_PUSH=expo npm run local
 */
import { relative } from 'node:path';
import { loadLocalConfig } from '../src/local/config.js';
import { createLocalRelay } from '../src/local/relay.js';
import { createLocalServer } from '../src/local/server.js';

const config = loadLocalConfig();
const time = () => new Date().toISOString().slice(11, 19);

const relay = createLocalRelay({
  feed: config.feed,
  push: config.push,
  replayFile: config.replayFile,
  ...(config.replayZonesFile ? { replayZonesFile: config.replayZonesFile } : {}),
  ...(config.expoAccessToken ? { expoAccessToken: config.expoAccessToken } : {}),
  onStage: (stage, detail) => console.log(`${time()} ${stage} ${JSON.stringify(detail)}`),
  logger: {
    info: () => {},
    warn: (msg, f) => console.warn(`${time()} warn ${msg} ${JSON.stringify(f ?? {})}`),
    error: (msg, f) => console.error(`${time()} error ${msg} ${JSON.stringify(f ?? {})}`),
  },
});

const server = createLocalServer(relay, {
  feed: config.feed,
  push: config.push,
  ...(config.feed === 'replay' ? { replayFile: relative(process.cwd(), config.replayFile) } : {}),
});

let polling = false;
async function tick() {
  if (polling) return;
  polling = true;
  try {
    await relay.pollAndDeliver();
  } catch (err) {
    console.error(`${time()} poll failed: ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    polling = false;
  }
}

server.listen(config.port, config.host, () => {
  console.log(`Relay local mode on http://${config.host}:${config.port}`);
  console.log(
    `  feed: ${config.feed}${config.feed === 'replay' ? ` (${relative(process.cwd(), config.replayFile)})` : ' (api.weather.gov)'}`,
  );
  console.log(`  push: ${config.push}`);
  console.log(`  clock: ${relay.clock.now().toISOString()}`);
  console.log(`  status page: http://localhost:${config.port}/`);
  void tick();
  setInterval(() => void tick(), config.pollSeconds * 1000).unref();
});

const stop = () => server.close(() => process.exit(0));
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
