import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// RED-7 / DEP-2 / DEP-3 / DEP-4 / DEP-7 / SEC-C-10: the repository files that decide what runs in
// CI and what ships in the sender image.

const root = fileURLToPath(new URL('../../', import.meta.url));
const read = (p: string) => readFileSync(`${root}${p}`, 'utf8');
const workflows = readdirSync(`${root}.github/workflows`).filter((f) => /\.ya?ml$/.test(f));

describe('GitHub Actions workflows', () => {
  it('has at least one workflow', () => {
    expect(workflows.length).toBeGreaterThan(0);
  });

  it.each(workflows)(
    '%s: every action is pinned to a full commit SHA with a version comment',
    (f) => {
      const uses = read(`.github/workflows/${f}`)
        .split('\n')
        .filter((l) => /^\s*-?\s*uses:/.test(l));
      expect(uses.length).toBeGreaterThan(0);
      for (const line of uses) {
        expect(line).toMatch(/uses:\s+[\w.-]+\/[\w./-]+@[0-9a-f]{40}\s+#\s+v\d+(\.\d+){0,2}\s*$/);
      }
    },
  );

  it.each(workflows)('%s: top-level permissions are read-only', (f) => {
    const text = read(`.github/workflows/${f}`);
    expect(text).toMatch(/^permissions:\n\s+contents: read\s*$/m);
    // No job may ask for write access without this test being updated on purpose.
    // Allowed on purpose: CodeQL's analyze job uploads SARIF results (SA-11, RA-5), which needs
    // security-events: write. Nothing else may write, and that permission may not be top-level.
    const allowedWrites: Record<string, string[]> = { 'codeql.yml': ['security-events'] };
    const writes = [...text.matchAll(/^\s*([\w-]+):\s*write\b/gm)].map((m) => m[1]);
    expect(writes).toEqual(allowedWrites[f] ?? []);
    if (writes.length > 0) {
      const topLevel = text.split(/^jobs:/m)[0];
      expect(topLevel).not.toMatch(/:\s*write\b/);
    }
  });
});

describe('Dependabot', () => {
  it('updates github-actions, npm and docker weekly', () => {
    const text = read('.github/dependabot.yml');
    for (const eco of ['github-actions', 'npm', 'docker']) {
      expect(text).toMatch(new RegExp(`package-ecosystem: ${eco}\\b`));
    }
    expect(text.match(/interval: weekly/g)).toHaveLength(3);
  });
});

describe('sender image', () => {
  const docker = read('backend/Dockerfile.sender');

  it('DEP-3: every FROM is digest-pinned, or marked with the TODO that says how to pin it', () => {
    const froms = docker.split('\n').filter((l) => l.startsWith('FROM '));
    expect(froms.length).toBeGreaterThanOrEqual(2);
    const unpinned = froms.filter((l) => !/@sha256:[0-9a-f]{64}/.test(l));
    if (unpinned.length > 0) {
      expect(docker).toContain('TODO(DEP-3)');
      expect(docker).toContain('pin-node-digest.sh');
    } else {
      expect(docker).not.toContain('TODO(DEP-3)');
    }
  });

  it('DEP-3: the pin script exists and refuses to write anything but a real digest', () => {
    const script = read('backend/scripts/pin-node-digest.sh');
    expect(script).toContain('^sha256:[0-9a-f]{64}$');
  });

  it('DEP-2: source maps are built without the embedded source text', () => {
    expect(docker).toMatch(/--sourcemap\s+--sources-content=false/);
    // Stack traces stay readable: the runtime still enables source maps and ships the map.
    expect(docker).toContain('--enable-source-maps');
    expect(docker).toContain('sender.mjs.map');
  });

  it('runs as the non-root node user', () => {
    expect(docker).toMatch(/^USER node$/m);
  });
});

describe('.dockerignore (DEP-7)', () => {
  it('exists and excludes secrets, VCS, tooling, build output and tests', () => {
    expect(existsSync(`${root}.dockerignore`)).toBe(true);
    const lines = read('.dockerignore')
      .split('\n')
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith('#'));
    for (const must of [
      '.env',
      '.env.*',
      '.git',
      '.claude',
      '**/node_modules',
      '**/cdk.out',
      '**/coverage',
      '**/test',
      '*.md',
    ]) {
      expect(lines).toContain(must);
    }
  });

  it('does not exclude anything the image build copies', () => {
    const lines = read('.dockerignore')
      .split('\n')
      .map((l) => l.trim());
    for (const needed of ['package.json', 'package-lock.json', 'packages/shared', 'backend/src']) {
      expect(lines).not.toContain(needed);
      expect(lines).not.toContain(`${needed}/`);
    }
    // The Dockerfile copies infra/package.json, so infra itself must not be excluded wholesale.
    expect(lines).not.toContain('infra');
  });
});
