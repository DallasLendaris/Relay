import { defineConfig } from 'vitest/config';

// CDK synth in a test can take several seconds on a busy machine; the 5 s default is too tight.
export default defineConfig({
  test: { testTimeout: 60_000, hookTimeout: 60_000 },
});
