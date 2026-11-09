import { configDefaults, defineConfig } from 'vitest/config';

// CDK synth in a test can take several seconds on a busy machine; the 5 s default is too tight.
// cdk.out holds bundled asset copies (including copies of these tests), so it is excluded.
export default defineConfig({
  test: {
    testTimeout: 60_000,
    hookTimeout: 60_000,
    exclude: [...configDefaults.exclude, 'cdk.out/**'],
  },
});
