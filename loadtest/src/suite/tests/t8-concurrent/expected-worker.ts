/**
 * t8/t9 child: works out who every alert must reach, from the seed GENERATOR (not from the on-disk
 * index or the planner, so a bug in either shows up as a difference). For each alert that pushes
 * (not statements) it lists the token hash of every seed device with at least one place in the
 * alert's cells; each such device gets exactly one push per alert whatever its number of places.
 *
 * Writes per alert `exp-a<i>.bin` (BigUint64 token hashes, one per device) and `exp-all.bin` (one hash
 * per (alert, device), so a token's multiplicity is the number of alerts that cover its device).
 * Returns the counts and the histogram of how many alerts cover each device.
 *
 * Control: run, stop.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Alert } from '@relay/shared';
import { generateDevices } from '../../../seed.js';
import { HashList } from '../t2-fanout/verify.js';
import { runCtlWorker, sleep } from './ctl.js';

export interface ExpectedReply {
  /** Devices expected per alert (0 for statements). */
  perAlert: number[];
  /** overlap[k] = devices covered by exactly k alerts that push. */
  overlap: number[];
  devices: number;
  pairs: number;
  ms: number;
  files: { perAlert: string[]; all: string };
}

runCtlWorker(async () => ({
  handlers: {
    run: async (p: {
      alertsFile: string;
      devices: number;
      seed: number;
      outDir: string;
    }): Promise<ExpectedReply> => {
      const t0 = Date.now();
      const alerts = JSON.parse(readFileSync(p.alertsFile, 'utf8')) as Alert[];
      const sets = alerts.map((a) =>
        a.relaySeverity === 'statement' ? null : new Set(a.area.h3Cells),
      );
      const lists = alerts.map(() => new HashList());
      const all = new HashList();
      const overlap: number[] = [];
      let n = 0;
      for (const d of generateDevices({ devices: p.devices, seed: p.seed, deviceIds: 'uuid' })) {
        let k = 0;
        sets.forEach((set, i) => {
          if (!set) return;
          for (const pl of d.places) {
            if (set.has(pl.h3Cell)) {
              (lists[i] as HashList).add(d.target.token);
              all.add(d.target.token);
              k++;
              return;
            }
          }
        });
        overlap[k] = (overlap[k] ?? 0) + 1;
        if (++n % 50_000 === 0) await sleep(0);
      }
      const files = {
        perAlert: lists.map((l, i) => {
          const f = join(p.outDir, `exp-a${i}.bin`);
          const words = l.toArray();
          writeFileSync(f, Buffer.from(words.buffer, words.byteOffset, words.byteLength));
          return f;
        }),
        all: join(p.outDir, 'exp-all.bin'),
      };
      const allWords = all.toArray();
      writeFileSync(
        files.all,
        Buffer.from(allWords.buffer, allWords.byteOffset, allWords.byteLength),
      );
      return {
        perAlert: lists.map((l) => l.n),
        overlap: Array.from(overlap, (v) => v ?? 0),
        devices: n,
        pairs: all.n,
        ms: Date.now() - t0,
        files,
      };
    },
  },
}));
