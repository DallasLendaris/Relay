/**
 * Seed process. 'gen' streams one chunk of devices into per-shard temp files (never holding the
 * chunk in memory); 'sort' turns the temp files of some shards into the final sorted TSV and its
 * byte-range index.
 */
import {
  appendFileSync,
  closeSync,
  openSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
  writeSync,
} from 'node:fs';
import path from 'node:path';
import { generateDevices } from '../../../seed.js';
import { serve } from './proc.js';
import {
  deviceLines,
  ensureDir,
  generateStateChunk,
  type GenDevice,
  type Region,
} from './seed-data.js';

const FLUSH_CHARS = 256 * 1024;

function* countyDevices(devices: number, seed: number): Generator<GenDevice> {
  for (const d of generateDevices({ devices, seed, deviceIds: 'uuid' })) {
    yield {
      deviceId: d.deviceId,
      platform: d.target.platform === 'ios' ? 'ios' : 'android',
      token: d.target.token,
      language: d.language,
      cells: d.places.map((p) => p.h3Cell),
    };
  }
}

serve({
  gen: (p: { dir: string; region: Region; devices: number; seed: number; chunk: number }) => {
    const tmp = path.join(p.dir, 'tmp', `c${p.chunk}`);
    ensureDir(tmp);
    const buffers = new Map<string, { parts: string[]; size: number }>();
    const flush = (shard: string, b: { parts: string[]; size: number }) => {
      if (b.size === 0) return;
      appendFileSync(path.join(tmp, `${shard}.tsv`), b.parts.join(''));
      b.parts = [];
      b.size = 0;
    };
    let entries = 0;
    let ios = 0;
    let android = 0;
    let multiCell = 0;
    const source =
      p.region === 'county'
        ? countyDevices(p.devices, p.seed)
        : generateStateChunk(p.seed, p.chunk, p.devices);
    for (const d of source) {
      if (d.platform === 'ios') ios++;
      else android++;
      if (new Set(d.cells).size > 1) multiCell++;
      for (const { shard, line } of deviceLines(d)) {
        entries++;
        let b = buffers.get(shard);
        if (!b) buffers.set(shard, (b = { parts: [], size: 0 }));
        b.parts.push(line);
        b.size += line.length;
        if (b.size >= FLUSH_CHARS) flush(shard, b);
      }
    }
    for (const [shard, b] of buffers) flush(shard, b);
    return { entries, ios, android, multiCell, shards: [...buffers.keys()] };
  },

  sort: (p: { dir: string; shards: string[] }) => {
    const tmpRoot = path.join(p.dir, 'tmp');
    const chunks = readdirSync(tmpRoot);
    for (const shard of p.shards) {
      const lines: string[] = [];
      for (const c of chunks) {
        let text: string;
        try {
          text = readFileSync(path.join(tmpRoot, c, `${shard}.tsv`), 'utf8');
        } catch {
          continue;
        }
        for (const l of text.split('\n')) if (l !== '') lines.push(l);
      }
      // Lines start with cell, deviceId (fixed length) and placeId, so plain string order is
      // (cell, `${deviceId}#${placeId}`) order, the DynamoDB sort-key order.
      lines.sort();
      const index: Record<string, [number, number, number]> = {};
      let offset = 0;
      let cur = '';
      let start = 0;
      let count = 0;
      const close = () => {
        if (cur !== '') index[cur] = [start, offset - start, count];
      };
      for (const l of lines) {
        const cell = l.slice(0, l.indexOf('\t'));
        if (cell !== cur) {
          close();
          cur = cell;
          start = offset;
          count = 0;
        }
        count++;
        offset += l.length + 1; // ASCII only: characters are bytes
      }
      close();
      // Written in slices: a dense metro shard can be hundreds of MB, near V8's string limit.
      const fd = openSync(path.join(p.dir, `${shard}.tsv`), 'w');
      try {
        for (let i = 0; i < lines.length; i += 20_000) {
          writeSync(fd, lines.slice(i, i + 20_000).join('\n') + '\n');
        }
      } finally {
        closeSync(fd);
      }
      writeFileSync(path.join(p.dir, `${shard}.idx.json`), JSON.stringify(index));
    }
    return { shards: p.shards.length };
  },

  cleanup: (p: { dir: string }) => {
    rmSync(path.join(p.dir, 'tmp'), { recursive: true, force: true });
    return true;
  },
});
