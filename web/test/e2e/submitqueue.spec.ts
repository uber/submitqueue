import AxeBuilder from "@axe-core/playwright";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { expect, test } from "@playwright/test";

import { Strategy } from "@submitqueue/api/base/mergestrategy";
import { SubmitQueueGateway } from "@submitqueue/api/submitqueue/gateway";

const queue = "demo-queue";
const gatewayURL = process.env.SUBMITQUEUE_E2E_GATEWAY_URL as string;
const webURL = process.env.SUBMITQUEUE_E2E_WEB_URL as string;

const browserErrors = new Map<object, string[]>();
test.beforeEach(async ({ page }) => {
  const errors: string[] = [];
  browserErrors.set(page, errors);
  page.on("pageerror", error => errors.push(error.message));
  page.on("console", message => {
    if (message.type() === "error" && /hydration|hydrated|server rendered/i.test(message.text())) errors.push(message.text());
  });
});
test.afterEach(async ({ page }) => {
  expect(browserErrors.get(page)).toEqual([]);
  browserErrors.delete(page);
});

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
  const response = await fetch(new URL(`/${queue}`, webURL), {
    redirect: "manual",
  });
  expect(response.status).toBe(401);
  expect(response.headers.get("www-authenticate")).toContain("Basic");
});

test("renders Base Web styles before JavaScript starts", async ({ browser }) => {
  const context = await browser.newContext({
    javaScriptEnabled: false,
    httpCredentials: { username: "test", password: process.env.SUBMITQUEUE_WEB_TOKEN! },
  });
  try {
    const page = await context.newPage();
    await page.goto(`/${queue}`);
    await expect(page.getByRole("heading", { name: queue, exact: true })).toBeVisible();
    expect(await page.locator("style[data-submitqueue-styletron]").count()).toBeGreaterThan(0);
    const background = await page.getByRole("button", { name: "Refresh", exact: true }).evaluate(button => getComputedStyle(button).backgroundColor);
    expect(background).not.toBe("rgba(0, 0, 0, 0)");
  } finally {
    await context.close();
  }
});

test("discovers gateway queues including empty queues and rejects unknown queues", async ({ page }) => {
  const expected = (await gateway.listQueues({})).queues.map(value => value.name);
  expect(expected).toContain(queue);
  const emptyQueue = expected.find(name => name !== queue)!;
  expect(emptyQueue).toBeDefined();
  await page.goto("/");
  await expect(page.locator(".sq-directory h2 a")).toHaveText(expected);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.getByRole("link", { name: emptyQueue, exact: true }).click();
  await expect(page.getByRole("heading", { name: emptyQueue, exact: true })).toBeVisible();
  await expect(page.getByText("No requests were received in this window.")).toBeVisible();
  await page.goto(`/${emptyQueue}/request/999999`);
  await expect(page.getByRole("heading", { name: "Request not found", exact: true })).toBeVisible();
  await page.goto("/not-a-configured-queue");
  await expect(page.getByRole("heading", { name: "Request not found", exact: true })).toBeVisible();
});

test("shows newest requests and an accessible readable detail history", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Queues", exact: true })).toBeVisible();
  await page.getByRole("link", { name: queue, exact: true }).click();
  await expect(page.getByText("No requests were received in this window.")).toBeVisible();
  expect(new URL(page.url()).search).toBe("");

  const firstSqid = await submitRequest(
    "github://github.com/uber/submitqueue/pull/123/1111111111111111111111111111111111111111",
  );
  const secondSqid = await submitRequest(
    "github://github.com/uber/submitqueue/pull/123/2222222222222222222222222222222222222222",
  );
  const repeatedSqid = await submitRequest(
    "github://github.com/uber/submitqueue/pull/123/2222222222222222222222222222222222222222",
  );

  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  const requestLinks = page.locator(".sq-request-list__item td:first-child a");
  await expect(requestLinks).toHaveCount(3, { timeout: 0 });
  await expect(requestLinks.nth(0)).toHaveText(repeatedSqid);
  await expect(requestLinks.nth(1)).toHaveText(secondSqid);
  await expect(requestLinks.nth(2)).toHaveText(firstSqid);
  expect(new URL(page.url()).search).toBe("");

  const listAccessibility = await new AxeBuilder({ page }).analyze();
  expect(listAccessibility.violations).toEqual([]);
  await page.screenshot({ path: test.info().outputPath("queue.png"), fullPage: true });

  await page.getByRole("searchbox").fill("1111111111111111111111111111111111111111");
  await expect(requestLinks).toHaveCount(1);
  await page.getByRole("searchbox").fill("");

  await requestLinks.nth(0).click();
  await expect(page.getByRole("heading", { name: repeatedSqid, exact: true })).toBeVisible();
  await expect(page.locator(".sq-request-detail .sq-status").first()).toHaveText("Landed");

  expect(new URL(page.url()).pathname).toBe(
    `/${queue}/request/${repeatedSqid}`,
  );

  await expect(page.getByRole("button", { name: "Copy ID" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Summary", exact: true })).toHaveAttribute("aria-current", "page");
  const actionBoxes = await Promise.all(["Copy ID", "Copy link", "Refresh"].map(name =>
    page.getByRole("button", { name, exact: true }).boundingBox()));
  expect(actionBoxes.every(box => box !== null)).toBe(true);
  expect(actionBoxes.map(box => box!.y)).toEqual([actionBoxes[0]!.y, actionBoxes[0]!.y, actionBoxes[0]!.y]);
  expect(actionBoxes.map(box => box!.height)).toEqual([actionBoxes[0]!.height, actionBoxes[0]!.height, actionBoxes[0]!.height]);
  await page.screenshot({ path: test.info().outputPath("request-summary.png"), fullPage: true });
  await page.getByRole("link", { name: /^History/ }).click();
  await expect(page).toHaveURL(url => url.searchParams.get("view") === "history");
  await expect(page.getByRole("list", { name: "Request history" })).toBeVisible();

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
  await page.screenshot({ path: test.info().outputPath("request-history.png"), fullPage: true });

  await page.getByRole("combobox").click();
  await page.getByRole("option", { name: "Occurrence events" }).click();
  await expect(page.getByRole("list", { name: "Request history" }).getByText("Accepted")).toHaveCount(0);
  await page.getByRole("link", { name: "Summary", exact: true }).click();
  await page.getByRole("link", { name: "github://github.com/uber/submitqueue/pull/123/2222222222222222222222222222222222222222", exact: true }).click();
  await expect(page.getByRole("heading", { name: "PR #123" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(3);
  expect(new URL(page.url()).search).toBe("");
  await page.screenshot({ path: test.info().outputPath("change-submissions.png"), fullPage: true });
  await page.getByRole("combobox", { name: "Filter submissions by version" }).click();
  await page.getByRole("option", { name: "2222222222222222222222222222222222222222" }).click();
  await expect(page.getByText("Retained submissions for this exact version")).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(2);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);

  await page.goto(`/${queue}?from=1&to=2`);
  await expect(page.getByRole("heading", { name: queue, exact: true })).toBeVisible();
  expect(new URL(page.url()).search).toBe("");
  const gitUri = "git://git.example.com/demo/refs%2Fheads%2Fweb-demo/3333333333333333333333333333333333333333";
  const gitRequest = await submitRequest(`${gitUri}?sq-files=demo%2F00%2Ffile.txt`);
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(page.getByRole("link", { name: gitRequest, exact: true })).toBeVisible();
  const changeLink = page.getByRole("link", { name: gitUri, exact: true });
  await expect(changeLink).toBeVisible();
  await expect(changeLink).toHaveAttribute("href", `/${queue}/change/git/git.example.com/demo/refs%2Fheads%2Fweb-demo`);
  await expect(page.getByText(/sq-files=/)).toHaveCount(0);
  await page.screenshot({ path: test.info().outputPath("queue-live.png"), fullPage: true });
  await changeLink.click();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo", exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(1);
  expect(new URL(page.url()).search).toBe("");
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.screenshot({ path: test.info().outputPath("git-change.png"), fullPage: true });
  const logicalChangeUrl = page.url();
  await page.reload();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo", exact: true })).toBeVisible();
  await page.getByRole("combobox", { name: "Filter submissions by version" }).click();
  await page.getByRole("option", { name: "3333333333333333333333333333333333333333" }).click();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo", exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(1);
  await page.goto(logicalChangeUrl);
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo", exact: true })).toBeVisible();
});

test("preserves literal percent escapes in Git refs when opening change history", async ({ page }) => {
  const uri = "git://git.example.com/demo/refs%2Fheads%2Fweb-demo%252Fpercent/4444444444444444444444444444444444444444";
  const sqid = await submitRequest(`${uri}?sq-files=demo%2F00%2Fpercent.txt`);
  await page.goto(`/${queue}`);
  await page.getByRole("link", { name: uri, exact: true }).click();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo%2Fpercent", exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).getByRole("link", { name: sqid, exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo%2Fpercent", exact: true })).toBeVisible();
});

test("paginates change history through more than 500 queue requests", async ({ page }) => {
  const changePath = `/${queue}/change/git/git.example.com/demo/refs%2Fheads%2Fpagination`;
  const oldest = await submitRequest("git://git.example.com/demo/refs%2Fheads%2Fpagination/5555555555555555555555555555555555555555");
  const expectedIds = [oldest];
  for (let offset = 0; offset < 501; offset += 25) {
    const replies = await Promise.all(Array.from({ length: Math.min(25, 501 - offset) }, (_, index) => gateway.land({
      queue,
      change: { uris: [`github://github.com/uber/submitqueue/pull/${1000 + offset + index}/6666666666666666666666666666666666666666`] },
      strategy: Strategy.SQUASH_REBASE,
    })));
    expectedIds.push(...replies.map(reply => reply.sqid));
  }
  const newest = (await gateway.land({
    queue,
    change: { uris: ["git://git.example.com/demo/refs%2Fheads%2Fpagination/7777777777777777777777777777777777777777"] },
    strategy: Strategy.SQUASH_REBASE,
  })).sqid;
  expectedIds.push(newest);
  await expect.poll(async () => {
    const ids = new Set<string>();
    const toMs = BigInt(Date.now());
    let pageToken = "";
    do {
      const response = await gateway.list({
        queue, receivedAtOrAfterMs: toMs - 86_400_000n, receivedBeforeMs: toMs,
        pageSize: 50, pageToken,
      });
      response.requests.forEach(request => ids.add(request.sqid));
      pageToken = response.nextPageToken;
    } while (pageToken);
    return expectedIds.every(id => ids.has(id));
  }, { timeout: 0 }).toBe(true);

  await page.goto(changePath);
  await expect(page.getByRole("heading", { name: "refs/heads/pagination", exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).getByRole("link", { name: newest, exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: oldest, exact: true })).toHaveCount(0);
  await expect(page.getByText("More queue requests remain to be searched.", { exact: false })).toBeVisible();
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.getByRole("link", { name: "Continue history scan", exact: true }).click();
  await expect(page.getByRole("table", { name: "Change submissions" }).getByRole("link", { name: oldest, exact: true })).toBeVisible();
  await expect(page.getByRole("link", { name: "Continue history scan", exact: true })).toHaveCount(0);
  expect(new URL(page.url()).searchParams.has("page")).toBe(true);
  await page.reload();
  await expect(page.getByRole("link", { name: oldest, exact: true })).toBeVisible();
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(page.getByRole("link", { name: newest, exact: true })).toBeVisible();
  expect(new URL(page.url()).search).toBe("");
});
