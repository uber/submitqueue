import { defineConfig } from "vitest/config";

export default defineConfig({
  esbuild: {
    jsx: "automatic",
  },
  test: {
    maxWorkers: 2,
    environment: "jsdom",
    globals: true,
    setupFiles: ["./test-setup.ts"],
    server: {
      deps: {
        inline: ["@submitqueue/web-platform"],
      },
    },
  },
});
