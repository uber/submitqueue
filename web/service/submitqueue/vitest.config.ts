import path from "node:path";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "server-only": path.resolve(import.meta.dirname, "src/test/server-only.ts"),
    },
  },
  test: {
    environment: "node",
    server: {
      deps: {
        inline: ["@submitqueue/web-submitqueue"],
      },
    },
  },
});
