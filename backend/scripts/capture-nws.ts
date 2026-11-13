/**
 * `npm run capture-nws`: saves the live NWS California feed, and the zone shapes its alerts
 * need, into backend/dev-data/replay/ as recorded-<time>.json and recorded-<time>.zones.json.
 * Run it on a machine with internet. Replay mode and simulate pick the newest recording.
 *
 * NWS asks for a User-Agent with contact details; set NWS_CONTACT (an email or URL) to add yours.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { REPLAY_DIR } from '../src/local/config.js';

const contact = process.env.NWS_CONTACT ?? 'github.com/DallasLendaris/relay';
const headers = {
  'User-Agent': `(Relay SJSU capstone, ${contact})`,
  Accept: 'application/geo+json',
};

const res = await fetch('https://api.weather.gov/alerts/active?area=CA', { headers });
if (!res.ok) {
  console.error(`NWS returned HTTP ${res.status}`);
  process.exit(1);
}
const body = await res.text();
const feed = JSON.parse(body) as {
  features: Array<{
    geometry: unknown;
    properties: { event: string; status: string; geocode?: { UGC?: string[] } };
  }>;
};

// Alerts without a polygon are located by their UGC zones: save those zone shapes too.
const ugcs = new Set<string>();
for (const f of feed.features) {
  if (!f.geometry) for (const u of f.properties.geocode?.UGC ?? []) ugcs.add(u);
}
const zones: Record<string, unknown> = {};
for (const ugc of ugcs) {
  const types = ugc[2] === 'C' ? ['county'] : ['forecast', 'fire'];
  for (const type of types) {
    const z = await fetch(`https://api.weather.gov/zones/${type}/${ugc}`, { headers });
    if (z.ok) {
      const zone = (await z.json()) as { geometry?: unknown };
      zones[`${type}/${ugc}`] = { geometry: zone.geometry ?? null };
      break;
    }
  }
}

mkdirSync(REPLAY_DIR, { recursive: true });
const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const feedFile = join(REPLAY_DIR, `recorded-${stamp}.json`);
writeFileSync(feedFile, body);
writeFileSync(join(REPLAY_DIR, `recorded-${stamp}.zones.json`), JSON.stringify(zones));

console.log(`Saved ${feed.features.length} alerts to ${feedFile}`);
console.log(`Saved ${Object.keys(zones).length} of ${ugcs.size} zone shapes`);
for (const f of feed.features) {
  console.log(
    `  ${f.properties.status.padEnd(8)} ${f.properties.event}${f.geometry ? '' : ' (zones)'}`,
  );
}
