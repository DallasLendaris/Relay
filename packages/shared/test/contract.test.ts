import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import SwaggerParser from '@apidevtools/swagger-parser';
import { describe, expect, it } from 'vitest';
import type { Alert, CheckIn, components } from '../src/index.js';

const root = fileURLToPath(new URL('..', import.meta.url));
const specPath = join(root, 'openapi/relay.yaml');

// --- Type-level drift check -------------------------------------------------
// Each type must be assignable to the other. If the zod schema and the OpenAPI
// component diverge in either direction, `npm run typecheck` fails here.
type Mutual<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Schemas = components['schemas'];

const alertMatches: Mutual<Alert, Schemas['Alert']> = true;
const checkInMatches: Mutual<CheckIn, Schemas['CheckIn']> = true;

describe('zod and OpenAPI types match', () => {
  it('Alert and CheckIn (enforced at compile time by typecheck)', () => {
    expect([alertMatches, checkInMatches]).toEqual([true, true]);
  });
});

describe('OpenAPI spec', () => {
  it('validates as OpenAPI 3.1', async () => {
    const api = await SwaggerParser.validate(specPath);
    expect((api as { openapi: string }).openapi).toBe('3.1.0');
  });

  it('has every demo path and both security schemes', async () => {
    const api = (await SwaggerParser.dereference(specPath)) as {
      paths: Record<string, unknown>;
      components: { securitySchemes: Record<string, unknown> };
    };
    expect(Object.keys(api.paths).sort()).toEqual(
      [
        '/agency/alerts',
        '/agency/alerts/{alertId}/response',
        '/alerts',
        '/alerts/{alertId}',
        '/alerts/{alertId}/checkins',
        '/devices',
        '/devices/me/push-token',
        '/places',
        '/places/{placeId}',
      ].sort(),
    );
    expect(Object.keys(api.components.securitySchemes).sort()).toEqual([
      'agencyAuth',
      'deviceAuth',
    ]);
  });

  it('generated types are up to date with the spec', () => {
    const dir = mkdtempSync(join(tmpdir(), 'relay-gen-'));
    const out = join(dir, 'api.ts');
    // npx finds the CLI whether npm hoisted it to the repo root or not.
    execFileSync('npx', ['--no-install', 'openapi-typescript', specPath, '-o', out], {
      cwd: root,
      stdio: 'pipe',
    });
    const committed = readFileSync(join(root, 'src/generated/api.ts'), 'utf8');
    expect(readFileSync(out, 'utf8')).toBe(committed);
  });
});
