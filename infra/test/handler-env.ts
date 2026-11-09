import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export function backendSource(path: string): string {
  return readFileSync(fileURLToPath(new URL(`../../backend/src/${path}`, import.meta.url)), 'utf8');
}

export interface EnvReadOptions {
  /**
   * Only read these named list constants (for a file that serves several handlers, such as
   * PLANNER_ENV / SHARD_ENV in fanout/handlers.ts). Property accesses are then ignored.
   */
  lists?: string[];
}

/**
 * Reads the environment variable names a Lambda handler or container entry point needs straight
 * from its source, so the infra tests cannot drift from the code. It recognizes the ways our
 * code reads env vars:
 *   1. process.env.NAME, env.NAME, process.env['NAME'], env['NAME']   (property access)
 *   2. const { A, B } = env;                                          (destructuring)
 *   3. const REQUIRED_ENV / PLANNER_ENV / names = ['A', 'B'] (a list of names that is looped over)
 *   4. requireEnv(env, ['A'])                                         (inline list)
 * If you read env vars in a new way, extend this file; the "source yields env names" check in
 * wiring.test.ts fails loudly when a handler's names cannot be found.
 */
export function envNamesRead(handlerPath: string, options: EnvReadOptions = {}): string[] {
  const src = backendSource(handlerPath);
  const name = /^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$/;
  const found = new Set<string>();
  const addList = (body: string) => {
    for (const s of body.matchAll(/'([A-Z0-9_]+)'/g)) found.add(s[1] as string);
  };

  if (options.lists) {
    for (const list of options.lists) {
      const m = new RegExp(`\\b${list}\\s*=\\s*\\[([^\\]]*)\\]`).exec(src);
      if (!m) throw new Error(`${list} not found in ${handlerPath}`);
      addList(m[1] as string);
    }
    return [...found].sort();
  }

  for (const m of src.matchAll(/\benv\.([A-Z][A-Z0-9_]*)/g)) found.add(m[1] as string);
  for (const m of src.matchAll(/\benv\['([A-Z][A-Z0-9_]*)'\]/g)) found.add(m[1] as string);

  for (const m of src.matchAll(/\{([^{}]*)\}\s*=\s*(?:process\.)?env\b/g)) {
    for (const part of (m[1] as string).split(',')) {
      const id = part.split(':')[0]?.trim() ?? '';
      if (name.test(id)) found.add(id);
    }
  }

  for (const m of src.matchAll(/\b(?:REQUIRED(?:_ENV)?|[A-Z]+_ENV|names)\s*=\s*\[([^\]]*)\]/g)) {
    addList(m[1] as string);
  }
  for (const m of src.matchAll(/requireEnv\(\s*\w+\s*,\s*\[([^\]]*)\]/g)) addList(m[1] as string);
  return [...found].sort();
}
