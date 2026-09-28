import { defineConfig } from "@playwright/test";

const baseURL = process.env.SUBMITQUEUE_E2E_WEB_URL;
const gatewayURL = process.env.SUBMITQUEUE_E2E_GATEWAY_URL;
const token = process.env.SUBMITQUEUE_WEB_TOKEN;

if (!baseURL || !gatewayURL || !token) {
  throw new Error(
    "SUBMITQUEUE_E2E_WEB_URL, SUBMITQUEUE_E2E_GATEWAY_URL, and SUBMITQUEUE_WEB_TOKEN are required",
  );
}

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  use: {
    baseURL,
    httpCredentials: {
      username: "submitqueue",
      password: token,
    },
    trace: "retain-on-failure",
  },
});
