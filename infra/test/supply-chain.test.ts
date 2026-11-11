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
    // No job may ask for write access without this test being updated on purpose, and no write
    // permission may be top-level. Allowed on purpose, per job:
    // - codeql.yml analyze: uploads SARIF results (SA-11, RA-5), needs security-events: write.
    // - deploy.yml diff and deploy: GitHub OIDC token for the AWS role (S-06, CM-5), needs
    //   id-token: write. Only those two jobs, never build.
    const allowedWrites: Record<string, Record<string, string[]>> = {
      'codeql.yml': { analyze: ['security-events'] },
      'deploy.yml': { diff: ['id-token'], deploy: ['id-token'] },
    };
    const topLevel = text.split(/^jobs:/m)[0] ?? '';
    expect(topLevel).not.toMatch(/:\s*write\b/);
    const writesByJob = Object.fromEntries(
      Object.entries(jobBlocks(text))
        .map(([job, body]) => [
          job,
          [...body.matchAll(/^\s*([\w-]+):\s*write\b/gm)].map((m) => m[1]),
        ])
        .filter(([, writes]) => writes!.length > 0),
    );
    expect(writesByJob).toEqual(allowedWrites[f] ?? {});
  });
});

/** Splits a workflow's `jobs:` section into each job's text, keyed by job id. */
function jobBlocks(text: string): Record<string, string> {
  const jobs = text.split(/^jobs:\n/m)[1] ?? '';
  const out: Record<string, string> = {};
  let current: string | undefined;
  for (const line of jobs.split('\n')) {
    const m = /^ {2}([\w-]+):\s*$/.exec(line);
    if (m) current = m[1]!;
    else if (current) out[current] = `${out[current] ?? ''}${line}\n`;
  }
  return out;
}

describe('deploy workflow (S-06, CM-5, SEC-C-10)', () => {
  const text = read('.github/workflows/deploy.yml');
  const jobs = jobBlocks(text);
  // The `on:` section, without comments (the header comment mentions pull_request on purpose).
  const triggers = (text.split(/^permissions:/m)[0] ?? '').replace(/^\s*#.*$/gm, '');

  it('has the build -> diff -> deploy jobs, in that order', () => {
    expect(Object.keys(jobs)).toEqual(['build', 'diff', 'deploy']);
    expect(jobs.diff).toMatch(/^ {4}needs: build$/m);
    expect(jobs.deploy).toMatch(/^ {4}needs: diff$/m);
  });

  it('runs only on manual dispatch (dev|prod) and on push to main, never on pull requests', () => {
    expect(triggers).toMatch(/^ {2}workflow_dispatch:$/m);
    expect(triggers).toMatch(/options: \[dev, prod\]/);
    expect(triggers).toMatch(/^ {2}push:\n {4}branches: \[main\]$/m);
    expect(triggers).not.toMatch(/pull_request|workflow_run|issue_comment|repository_dispatch/);
  });

  it.each(['build', 'diff', 'deploy'])('%s is inert until AWS_DEPLOY_ENABLED is true', (job) => {
    expect(jobs[job]).toMatch(/^ {4}if: vars\.AWS_DEPLOY_ENABLED == 'true' && /m);
  });

  it('push deploys dev only: the stage falls back to dev when there is no dispatch input', () => {
    expect(text).toContain("STAGE: ${{ inputs.stage || 'dev' }}");
    expect(text).toContain("group: deploy-${{ inputs.stage || 'dev' }}");
    expect(text).toMatch(/cancel-in-progress: false/);
    expect(jobs.build).toMatch(/if: env\.STAGE == 'prod' && github\.ref != 'refs\/heads\/main'/);
  });

  it('diff runs in <stage>-diff and deploy in <stage>, the environments the roles trust', () => {
    expect(jobs.build).not.toMatch(/environment:/);
    expect(jobs.diff).toContain("environment: ${{ inputs.stage || 'dev' }}-diff");
    expect(jobs.deploy).toContain("environment: ${{ inputs.stage || 'dev' }}\n");
    expect(jobs.diff).toContain('role-to-assume: ${{ vars.AWS_DIFF_ROLE_ARN }}');
    expect(jobs.deploy).toContain('role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}');
    expect(jobs.build).not.toMatch(/configure-aws-credentials/);
  });

  it('uses OIDC only: no access keys, no secrets', () => {
    expect(text).not.toMatch(/aws-access-key-id|aws-secret-access-key|aws-session-token/);
    expect(text).not.toMatch(/secrets\./);
    for (const job of ['diff', 'deploy']) {
      expect(jobs[job]).toContain('allowed-account-ids: ${{ vars.AWS_ACCOUNT_ID }}');
    }
  });

  it('never persists the checkout token', () => {
    const checkouts = text.match(/uses: actions\/checkout@/g) ?? [];
    expect(checkouts).toHaveLength(3);
    expect(text.match(/persist-credentials: false/g)).toHaveLength(checkouts.length);
  });

  it('synthesizes with cdk-nag on and deploys exactly the diffed cloud assembly', () => {
    expect(text).not.toMatch(/nag=off/);
    expect(jobs.diff).toMatch(/npx cdk synth .*--output cdk\.out/);
    expect(jobs.diff).toMatch(/npx cdk diff --app cdk\.out --all --method=template/);
    expect(jobs.diff).toContain('GITHUB_STEP_SUMMARY');
    expect(jobs.deploy).toMatch(/npx cdk deploy --app cdk\.out --all/);
    expect(jobs.deploy).not.toMatch(/cdk synth/);
  });

  it('takes prod context from environment variables, never hard-coded values', () => {
    for (const [ctx, v] of [
      ['publicAlertBaseUrl', 'PUBLIC_ALERT_BASE_URL'],
      ['alarmEmail', 'ALARM_EMAIL'],
      ['edgeDomainName', 'EDGE_DOMAIN_NAME'],
      ['edgeCertificateArn', 'EDGE_CERTIFICATE_ARN'],
    ]) {
      expect(jobs.diff).toContain(`${v}: \${{ vars.${v} }}`);
      expect(jobs.diff).toContain(`-c "${ctx}=$${v}"`);
    }
    expect(text).not.toMatch(/https:\/\/alerts\.|@example\.|arn:aws:acm:|\b\d{12}\b/);
    // No ${{ }} expression is interpolated into a shell script (script injection).
    const scripts = text.split(/^\s+run: \|?/m).slice(1);
    for (const s of scripts) expect(s.split(/^\s+- (uses|name|run):/m)[0]).not.toContain('${{');
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
