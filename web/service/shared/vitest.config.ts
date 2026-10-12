import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: {
      "server-only": path.resolve(import.meta.dirname, "test/server-only.ts"),
    },
  },
  test: { environment: "node" },
});
