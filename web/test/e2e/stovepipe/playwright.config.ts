import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: ".", testMatch: "*.spec.ts", timeout: 0, workers: 1, reporter: "line",
  outputDir: process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? "test-results",
  use: { baseURL: process.env.STOVEPIPE_E2E_WEB_URL, timezoneId: "America/Los_Angeles", httpCredentials: { username: "test", password: "test" },
    launchOptions: { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH }, trace: "retain-on-failure" },
});
