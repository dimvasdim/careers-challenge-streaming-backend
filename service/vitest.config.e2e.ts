import { defineConfig } from 'vitest/config';

// Integration tests: need the compose Postgres on localhost:5433
// (`docker compose up -d postgres`). They share one test database, so files run serially.
export default defineConfig({
  resolve: { tsconfigPaths: true },
  test: {
    globals: true,
    root: './',
    include: ['**/*.e2e-spec.ts'],
    globalSetup: ['./test/global-setup.ts'],
    fileParallelism: false,
    env: {
      DATABASE_URL: 'postgres://teton:teton@localhost:5433/teton_test',
    },
  },
});
