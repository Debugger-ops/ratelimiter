import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // These tests run against a real Redis on purpose. A mock cannot tell you
    // whether the Lua is atomic under concurrency, which is the property the
    // whole design rests on.
    environment: 'node',
    testTimeout: 20_000,
    hookTimeout: 20_000,
    pool: 'forks',
    poolOptions: { forks: { singleFork: true } },
  },
});
