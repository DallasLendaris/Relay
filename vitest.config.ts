import { defineConfig } from 'vitest/config';

// Shared defaults. Each workspace runs `vitest run` from its own folder.
export default defineConfig({
  test: { include: ['src/**/*.test.ts', 'test/**/*.test.ts'] },
});
