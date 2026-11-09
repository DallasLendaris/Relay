/**
 * Writes fake dev data to backend/dev-data/: alerts.json, checkins.json and places.json.
 *
 *   npx tsx backend/scripts/generate-fake-data.ts --seed 42 --alerts 6 --checkins 500 --places 1000
 *
 * Every option is optional and the defaults above are used. The same options always
 * write the same files. See backend/dev-data/README.md.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { generateDataset } from '../src/dev/fake-data.js';

const DEFAULTS = { seed: 42, alerts: 6, checkins: 500, places: 1000 };

/** Reads "--name value" pairs. Unknown names and non-whole-number values are errors. */
function parseArgs(argv: string[]): typeof DEFAULTS {
  const options = { ...DEFAULTS };
  const names = Object.keys(DEFAULTS);
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i] ?? '';
    const name = flag.replace(/^--/, '');
    if (!flag.startsWith('--') || !names.includes(name)) {
      throw new Error(
        `Unknown option "${flag}". Use one of: ${names.map((n) => `--${n}`).join(', ')}`,
      );
    }
    const raw = argv[i + 1] ?? '';
    const value = Number(raw);
    if (raw === '' || !Number.isInteger(value) || value < 0) {
      throw new Error(`${flag} needs a whole number, got "${raw}"`);
    }
    options[name as keyof typeof DEFAULTS] = value;
  }
  return options;
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const data = await generateDataset(options);

  // dev-data sits next to this script's folder, so the output does not depend on where you run it from.
  const outDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'dev-data');
  mkdirSync(outDir, { recursive: true });
  for (const [name, rows] of Object.entries(data)) {
    writeFileSync(join(outDir, `${name}.json`), JSON.stringify(rows, null, 2) + '\n');
  }
  console.log(
    `Wrote ${data.alerts.length} alerts, ${data.checkins.length} check-ins and ` +
      `${data.places.length} places to ${outDir} (seed ${options.seed}).`,
  );
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
