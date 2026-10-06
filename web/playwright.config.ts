import { defineConfig } from "@playwright/test";

const baseURL = process.env.SUBMITQUEUE_E2E_WEB_URL;
const gatewayURL = process.env.SUBMITQUEUE_E2E_GATEWAY_URL;
const token = process.env.SUBMITQUEUE_WEB_TOKEN;
const chromiumExecutablePath =
  process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;

if (!baseURL || !gatewayURL || !token || !chromiumExecutablePath) {
  throw new Error(
    "SUBMITQUEUE_E2E_WEB_URL, SUBMITQUEUE_E2E_GATEWAY_URL, SUBMITQUEUE_WEB_TOKEN, and PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH are required",
  );
}

export default defineConfig({
  outputDir: process.env.PLAYWRIGHT_OUTPUT_DIR ?? "test-results",
  testDir: "./test/e2e",
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  use: {
    baseURL,
    httpCredentials: {
      username: "test",
      password: token,
    },
    launchOptions: {
      executablePath: chromiumExecutablePath,
    },
    trace: "retain-on-failure",
  },
});
