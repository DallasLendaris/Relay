/**
 * Makes docs/demo/status-page.png: runs the scripted PoC path in local mode (replay feed, logged
 * pushes), then photographs the status page. Needs Playwright with Chromium (not a dependency).
 * Usage: npx tsx scripts/demo-screenshot.ts <output.png>
 * Set PLAYWRIGHT_MODULE to Playwright's install path if it is installed globally.
 */
import { createRequire } from 'node:module';
import type { AddressInfo } from 'node:net';
import { relative } from 'node:path';
import { loadLocalConfig } from '../src/local/config.js';
import { pickDemoPoints } from '../src/local/pick-points.js';
import { createLocalRelay } from '../src/local/relay.js';
import { runScenario } from '../src/local/scenario.js';
import { createLocalServer } from '../src/local/server.js';

const out = process.argv[2] ?? 'status-page.png';
const config = loadLocalConfig({ RELAY_FEED: 'replay', RELAY_PUSH: 'log' });
const make = () =>
  createLocalRelay({
    feed: 'replay',
    push: 'log',
    replayFile: config.replayFile,
    ...(config.replayZonesFile ? { replayZonesFile: config.replayZonesFile } : {}),
  });
// Place the phones from the alerts this feed actually holds (as simulate does).
const points = await pickDemoPoints(make);
const relay = make();
const inside = points.inside[0];
if (inside && points.outside) await runScenario(relay, () => {}, inside, points.outside);
else await runScenario(relay);
const server = createLocalServer(relay, {
  feed: 'replay',
  push: 'log',
  replayFile: relative(process.cwd(), config.replayFile),
});
await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
const port = (server.address() as AddressInfo).port;

interface Page {
  goto(url: string): Promise<unknown>;
  waitForSelector(selector: string): Promise<unknown>;
  screenshot(opts: { path: string; fullPage: boolean }): Promise<unknown>;
}
interface Chromium {
  launch(): Promise<{
    newPage(opts: object): Promise<Page>;
    close(): Promise<void>;
  }>;
}
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE ?? 'playwright') as {
  chromium: Chromium;
};
const browser = await chromium.launch();
const page = await browser.newPage({
  viewport: { width: 1000, height: 800 },
  deviceScaleFactor: 2,
});
await page.goto(`http://127.0.0.1:${port}/`);
await page.waitForSelector('#cells td:not(.muted)');
await page.screenshot({ path: out, fullPage: true });
await browser.close();
server.close();
console.log(`Saved ${out}`);
