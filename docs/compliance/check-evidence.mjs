#!/usr/bin/env node
// Checks the evidence cited in docs/compliance/control-map.md against the repository.
//
//   node docs/compliance/check-evidence.mjs
//
// For every table row it finds each code-span citation that looks like a repo path, optionally
// followed by `#name` (`infra/lib/data-stack.ts#storageEncrypted`), and fails when the file does
// not exist or does not contain the name. Relative Markdown links must point at existing files.
// Every row whose status is "Built" must cite at least one path with a name. No dependencies.

import { existsSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
const mapPath = join(here, 'control-map.md');

// A path: no spaces, no ':', no '<', ends in an extension (Dockerfile.sender counts).
const PATH_RE = /^\.?[\w.\-/]+\.[A-Za-z]+$/;

const errors = [];
let rows = 0;
let citations = 0;
const cache = new Map();
const read = (p) => {
  if (!cache.has(p)) cache.set(p, readFileSync(p, 'utf8'));
  return cache.get(p);
};

for (const [n, line] of readFileSync(mapPath, 'utf8').split('\n').entries()) {
  if (!line.startsWith('| ') || line.startsWith('| Control') || line.startsWith('| ---')) continue;
  const cells = line
    .split('|')
    .slice(1, -1)
    .map((c) => c.trim());
  if (cells.length !== 6) {
    errors.push(`line ${n + 1}: expected 6 cells, got ${cells.length}`);
    continue;
  }
  rows++;
  const [control, , , status, evidence] = cells;
  let named = 0;

  for (const [, span] of evidence.matchAll(/`([^`]+)`/g)) {
    const hash = span.indexOf('#');
    const file = hash === -1 ? span : span.slice(0, hash);
    const name = hash === -1 ? '' : span.slice(hash + 1);
    if (!PATH_RE.test(file)) continue; // an identifier or setting, not a citation
    citations++;
    const abs = join(root, file);
    if (!existsSync(abs) || !statSync(abs).isFile()) {
      errors.push(`${control} (line ${n + 1}): missing file ${file}`);
      continue;
    }
    if (name) {
      if (!read(abs).includes(name)) {
        errors.push(`${control} (line ${n + 1}): "${name}" not found in ${file}`);
        continue;
      }
      named++;
    }
  }

  for (const [, target] of evidence.matchAll(/\]\(([^)]+)\)/g)) {
    if (/^[a-z]+:/i.test(target)) continue; // external URL
    const file = target.split('#')[0];
    if (!file) continue;
    citations++;
    if (!existsSync(resolve(here, file))) {
      errors.push(`${control} (line ${n + 1}): broken link ${target}`);
    }
  }

  if (status === 'Built' && named === 0) {
    errors.push(`${control} (line ${n + 1}): Built but cites no path#name evidence`);
  }
}

if (rows === 0) errors.push('no table rows found in control-map.md');
if (errors.length > 0) {
  console.error(errors.join('\n'));
  console.error(`\n${errors.length} problem(s) in ${rows} rows`);
  process.exit(1);
}
console.log(`ok: ${rows} rows, ${citations} citations checked`);
