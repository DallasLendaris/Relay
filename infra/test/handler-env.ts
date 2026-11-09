import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/**
 * Reads the environment variable names a Lambda handler needs straight from its source,
 * so the infra tests cannot drift from the code. It recognizes the ways our handlers
 * read env vars:
 *   1. process.env.NAME and env.NAME            (property access)
 *   2. const { A, B } = env;                    (destructuring)
 *   3. const REQUIRED_ENV / names = ['A', 'B']  (a list of names that is looped over)
 * If you read env vars in a new way, extend this file; the "handler source yields env
 * names" check in wiring.test.ts fails loudly when a handler's names cannot be found.
 */
export function envNamesRead(handlerPath: string): string[] {
  const src = readFileSync(
    fileURLToPath(new URL(`../../backend/src/${handlerPath}`, import.meta.url)),
    'utf8',
  );
  const name = /^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$/;
  const found = new Set<string>();

  for (const m of src.matchAll(/\benv\.([A-Z][A-Z0-9_]*)/g)) found.add(m[1] as string);

  for (const m of src.matchAll(/\{([^{}]*)\}\s*=\s*(?:process\.)?env\b/g)) {
    for (const part of (m[1] as string).split(',')) {
      const id = part.split(':')[0]?.trim() ?? '';
      if (name.test(id)) found.add(id);
    }
  }

  for (const m of src.matchAll(/\b(?:REQUIRED_ENV|names)\s*=\s*\[([^\]]*)\]/g)) {
    for (const s of (m[1] as string).matchAll(/'([A-Z0-9_]+)'/g)) found.add(s[1] as string);
  }
  return [...found].sort();
}
