import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["*-runtime.test.mjs"],
    server: {
      deps: {
        inline: [/@submitqueue\/web-/, /(?:^|\/)baseui(?:\/|$)/],
      },
    },
  },
});
