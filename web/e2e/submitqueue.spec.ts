import AxeBuilder from "@axe-core/playwright";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { expect, request as playwrightRequest, test } from "@playwright/test";

import { Strategy } from "../api/src/gen/api/base/mergestrategy/proto/mergestrategy_pb.js";
import { SubmitQueueGateway } from "../api/src/gen/api/submitqueue/gateway/proto/gateway_pb.js";

const queue = "demo-queue";
const gatewayURL = process.env.SUBMITQUEUE_E2E_GATEWAY_URL as string;
const webURL = process.env.SUBMITQUEUE_E2E_WEB_URL as string;

const gateway = createClient(
  SubmitQueueGateway,
  createGrpcTransport({ baseUrl: gatewayURL }),
);

async function submitRequest(uri: string): Promise<string> {
  const response = await gateway.land({
    queue,
    change: { uris: [uri] },
    strategy: Strategy.SQUASH_REBASE,
  });
  expect(response.sqid).not.toBe("");
  await expect
    .poll(
      async () => {
        const summary = await gateway.getRequestSummaryByID({
          queue,
          sqid: response.sqid,
        });
        return summary.request?.status;
      },
      { timeout: 0 },
    )
    .toBe("landed");
  return response.sqid;
}

function expectOrderedHistory(values: readonly string[], expected: readonly string[]): void {
  let position = 0;
  for (const value of values) {
    if (value.includes(expected[position] ?? "\u0000")) {
      position += 1;
    }
  }
  expect(position).toBe(expected.length);
}

test("challenges anonymous requests", async () => {
  const anonymous = await playwrightRequest.newContext({ baseURL: webURL });
  try {
    const response = await anonymous.get("/requests");
    expect(response.status()).toBe(401);
    expect(response.headers()["www-authenticate"]).toContain("Basic");
  } finally {
    await anonymous.dispose();
  }
});

test("shows newest requests and an accessible encoded detail history", async ({ page }) => {
  const firstSqid = await submitRequest(
    "git://git.example.com/demo/refs%2Fheads%2Fweb-first/1111111111111111111111111111111111111111?sq-files=web/first.txt",
  );
  const secondSqid = await submitRequest(
    "git://git.example.com/demo/refs%2Fheads%2Fweb-second/2222222222222222222222222222222222222222?sq-files=web/second.txt",
  );

  await page.goto("/requests");
  const requestLinks = page.locator(".sq-request-list__item a");
  await expect(requestLinks).toHaveCount(2, { timeout: 0 });
  await expect(requestLinks.nth(0)).toHaveText(secondSqid);
  await expect(requestLinks.nth(1)).toHaveText(firstSqid);

  const listAccessibility = await new AxeBuilder({ page }).analyze();
  expect(listAccessibility.violations).toEqual([]);

  await requestLinks.nth(0).click();
  await expect(page.getByRole("heading", { name: "Request status" })).toBeVisible();
  await expect(page.getByText(secondSqid, { exact: true })).toBeVisible();
  await expect(page.locator(".sq-request-detail .sq-status").first()).toHaveText("Landed");

  const encodedSegment = new URL(page.url()).pathname.split("/").at(-1);
  expect(encodedSegment).toMatch(/^[A-Za-z0-9_-]+$/u);
  expect(encodedSegment).not.toContain("/");

  const history = await page.locator(".sq-history__item").allTextContents();
  expectOrderedHistory(history, [
    "Accepted",
    "Started",
    "Validating",
    "Batched",
    "Speculating",
    "Building",
    "Built",
    "Speculated",
    "Landing",
    "Landed",
  ]);

  const detailAccessibility = await new AxeBuilder({ page }).analyze();
  expect(detailAccessibility.violations).toEqual([]);
});
