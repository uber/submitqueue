import AxeBuilder from "@axe-core/playwright";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { expect, test } from "@playwright/test";

import { Strategy } from "@submitqueue/api/base/mergestrategy";
import { SubmitQueueGateway } from "@submitqueue/api/submitqueue/gateway";

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
  const response = await fetch(new URL(`/queue/${queue}`, webURL), {
    redirect: "manual",
  });
  expect(response.status).toBe(401);
  expect(response.headers.get("www-authenticate")).toContain("Basic");
});

test("serves a queue's filtered requests as JSON behind the same authentication", async ({ request }) => {
  expect((await fetch(new URL(`/api/queue/${queue}/requests`, webURL))).status).toBe(401);
  const response = await request.get(`/api/queue/${queue}/requests?status=landed`);
  expect(response.status()).toBe(200);
  expect(response.headers()["cache-control"]).toBe("no-store");
  const page = await response.json() as { requests: { status: string }[]; nextHref: string | null };
  expect(page.requests.every(request => request.status === "landed")).toBe(true);
  expect((await request.get("/api/queue/not-a-configured-queue/requests")).status()).toBe(404);
});

test("discovers gateway queues including empty queues and rejects unknown queues", async ({ page }) => {
  const expected = (await gateway.listQueues({})).queues.map(value => value.name);
  expect(expected).toContain(queue);
  const emptyQueue = expected.find(name => name !== queue)!;
  expect(emptyQueue).toBeDefined();
  await page.goto("/");
  await expect(page.locator(".sq-directory h2 a")).toHaveText(expected);
  await page.screenshot({ path: test.info().outputPath("directory.png"), fullPage: true });
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.getByRole("link", { name: emptyQueue, exact: true }).click();
  await expect(page.getByRole("heading", { name: emptyQueue, exact: true })).toBeVisible();
  await expect(page.getByText("No requests have been received on this queue.")).toBeVisible();
  await page.goto(`/queue/${emptyQueue}/request/999999`);
  await expect(page.getByRole("heading", { name: "Request not found", exact: true })).toBeVisible();
  await page.goto("/queue/not-a-configured-queue");
  await expect(page.getByRole("heading", { name: "Request not found", exact: true })).toBeVisible();
});

test("shows newest requests and an accessible readable detail history", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Queues", exact: true })).toBeVisible();
  await page.getByRole("link", { name: queue, exact: true }).click();
  await expect(page.getByText("No requests have been received on this queue.")).toBeVisible();
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

  await page.reload();
  const requestLinks = page.locator(".sq-request-list__item td:first-child a");
  await expect(requestLinks).toHaveCount(3, { timeout: 0 });
  await expect(requestLinks.nth(0)).toHaveText(repeatedSqid);
  await expect(requestLinks.nth(1)).toHaveText(secondSqid);
  await expect(requestLinks.nth(2)).toHaveText(firstSqid);
  expect(new URL(page.url()).search).toBe("");

  const listAccessibility = await new AxeBuilder({ page }).analyze();
  expect(listAccessibility.violations).toEqual([]);
  await page.screenshot({ path: test.info().outputPath("queue.png"), fullPage: true });
  await page.setViewportSize({ width: 1920, height: 1080 });
  const table = await page.getByRole("table", { name: `Requests in ${queue}` }).boundingBox();
  const column = await page.getByRole("main").boundingBox();
  expect(column?.width).toBe(1200);
  expect(table?.width).toBeGreaterThan(1200 * 0.95);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.setViewportSize({ width: 1280, height: 720 });

  await page.getByRole("combobox", { name: "Filter requests by status" }).click();
  await page.getByRole("option", { name: "Landed", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`/queue/${queue}\\?status=landed$`));
  await page.goto(`/queue/${queue}`);

  await requestLinks.nth(0).click();
  await expect(page.getByRole("heading", { name: repeatedSqid, exact: true })).toBeVisible();
  await expect(page.locator(".sq-request-detail .sq-status").first()).toHaveText("Landed");

  expect(new URL(page.url()).pathname).toBe(
    `/queue/${queue}/request/${repeatedSqid}`,
  );

  await expect(page.getByRole("button", { name: "Copy ID" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Summary", exact: true })).toHaveAttribute("aria-current", "page");
  const actionBoxes = await Promise.all(["Copy ID", "Copy link"].map(name =>
    page.getByRole("button", { name, exact: true }).boundingBox()));
  expect(actionBoxes.every(box => box !== null)).toBe(true);
  expect(actionBoxes.map(box => box!.y)).toEqual([actionBoxes[0]!.y, actionBoxes[0]!.y]);
  expect(actionBoxes.map(box => box!.height)).toEqual([actionBoxes[0]!.height, actionBoxes[0]!.height]);
  const facts = page.locator(".sq-request-detail dl").first().locator(":scope > div");
  await expect(facts).toHaveCount(6);
  const factLayout = await facts.evaluateAll(rows => rows.map(row => {
    const label = row.querySelector("dt")!.getBoundingClientRect();
    const value = row.querySelector("dd")!.getBoundingClientRect();
    return {
      height: row.getBoundingClientRect().height,
      labelGap: value.top - label.bottom,
      separator: parseFloat(getComputedStyle(row).borderBottomWidth),
    };
  }));
  for (const row of factLayout) {
    expect(row.height).toBeGreaterThanOrEqual(60);
    expect(row.labelGap).toBeGreaterThanOrEqual(6);
    expect(row.separator).toBeGreaterThan(0);
  }
  const lifecycle = page.getByRole("list", { name: "Observed lifecycle transitions" });
  const firstSteps = await lifecycle.locator("li").evaluateAll(items => items.slice(0, 2).map(item => {
    const { x, y } = item.getBoundingClientRect();
    return { x, y };
  }));
  expect(firstSteps).toHaveLength(2);
  expect(firstSteps[1]!.y).toBe(firstSteps[0]!.y);
  expect(firstSteps[1]!.x).toBeGreaterThan(firstSteps[0]!.x);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.screenshot({ path: test.info().outputPath("request-summary.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole("link", { name: /^History/ }).click();
  await expect(page).toHaveURL(url => url.searchParams.get("view") === "history");
  await expect(page.getByRole("list", { name: "Request history" })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole("list", { name: "Request history" }).locator("summary").first().click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.setViewportSize({ width: 1280, height: 720 });

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
  await page.getByRole("option", { name: "Occurrence events", exact: true }).click();
  await expect(page.getByRole("list", { name: "Request history" }).getByText("Accepted")).toHaveCount(0);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  const historyFilter = page.getByRole("combobox", { name: /Show events/ });
  await historyFilter.focus();
  await historyFilter.press("ArrowDown");
  await expect(historyFilter).toHaveAttribute("aria-expanded", "true");
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await historyFilter.press("ArrowUp");
  await expect.poll(() => historyFilter.evaluate(input => {
    const activeId = input.getAttribute("aria-activedescendant");
    return activeId ? document.getElementById(activeId)?.getAttribute("role") : null;
  })).toBe("option");
  await historyFilter.press("Enter");
  await expect(historyFilter).toHaveAttribute("aria-expanded", "false");
  await expect(page.getByRole("list", { name: "Request history" }).getByText("Accepted")).toBeVisible();
  await historyFilter.press("ArrowDown");
  await expect(historyFilter).toHaveAttribute("aria-expanded", "true");
  await historyFilter.press("Escape");
  await expect(historyFilter).toHaveAttribute("aria-expanded", "false");
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.getByRole("link", { name: "Summary", exact: true }).click();
  await page.getByRole("link", { name: "github://github.com/uber/submitqueue/pull/123/2222222222222222222222222222222222222222", exact: true }).click();
  await expect(page.getByRole("heading", { name: "PR #123" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(3);
  expect(new URL(page.url()).search).toBe("");
  await page.screenshot({ path: test.info().outputPath("change-submissions.png"), fullPage: true });
  await page.getByRole("combobox", { name: /Filter submissions by version/ }).click();
  await page.getByRole("option", { name: "2222222222222222222222222222222222222222", exact: true }).click();
  await expect(page.getByText("Retained submissions for this exact version")).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(2);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);

  await page.goto(`/queue/${queue}?from=1&to=2`);
  await expect(page.getByRole("heading", { name: queue, exact: true })).toBeVisible();
  expect(new URL(page.url()).search).toBe("");
  const gitUri = "git://git.example.com/demo/refs%2Fheads%2Fweb-demo/3333333333333333333333333333333333333333";
  const gitRequest = await submitRequest(`${gitUri}?sq-files=demo%2F00%2Ffile.txt`);
  await page.reload();
  await expect(page.getByRole("link", { name: gitRequest, exact: true })).toBeVisible();
  const changeLink = page.getByRole("link", { name: gitUri, exact: true });
  await expect(changeLink).toBeVisible();
  await expect(changeLink).toHaveAttribute("href", `/queue/${queue}/change/git/git.example.com/demo/refs%2Fheads%2Fweb-demo`);
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
  await page.getByRole("combobox", { name: /Filter submissions by version/ }).click();
  await page.getByRole("option", { name: "3333333333333333333333333333333333333333", exact: true }).click();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo", exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).locator("tbody tr")).toHaveCount(1);
  await page.goto(logicalChangeUrl);
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo", exact: true })).toBeVisible();
});

test("preserves literal percent escapes in Git refs when opening change history", async ({ page }) => {
  const uri = "git://git.example.com/demo/refs%2Fheads%2Fweb-demo%252Fpercent/4444444444444444444444444444444444444444";
  const sqid = await submitRequest(`${uri}?sq-files=demo%2F00%2Fpercent.txt`);
  await page.goto(`/queue/${queue}`);
  await page.getByRole("link", { name: uri, exact: true }).click();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo%2Fpercent", exact: true })).toBeVisible();
  await expect(page.getByRole("table", { name: "Change submissions" }).getByRole("link", { name: sqid, exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("heading", { name: "refs/heads/web-demo%2Fpercent", exact: true })).toBeVisible();
});

test("paginates change history through more than 500 queue requests", async ({ page }) => {
  const changePath = `/queue/${queue}/change/git/git.example.com/demo/refs%2Fheads%2Fpagination`;
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
      const response = await gateway.list(pageToken ? { queue, pageSize: 50, pageToken } : {
        queue, pageSize: 50,
        receivedLowerBound: { case: "receivedAtOrAfterMs", value: toMs - 86_400_000n },
        receivedUpperBound: { case: "receivedBeforeMs", value: toMs },
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
  await page.getByRole("link", { name: "Latest history", exact: true }).click();
  await expect(page.getByRole("link", { name: newest, exact: true })).toBeVisible();
  expect(new URL(page.url()).search).toBe("");
});

test("refreshes from the header only while auto-refresh is enabled", async ({ page }) => {
  await page.goto(`/queue/${queue}`);
  const automatic = page.getByRole("banner").getByRole("checkbox", { name: "Auto refresh every 10s" });
  await expect(automatic).not.toBeChecked();
  const now = new Date();
  await page.clock.install({ time: now });
  // The installed clock keeps running, so pausing at the install time races it.
  await page.clock.pauseAt(new Date(now.getTime() + 60_000));
  let refreshRequests = 0;
  const isRefresh = (request: import("@playwright/test").Request) =>
    request.headers()["rsc"] === "1" && new URL(request.url()).pathname === new URL(page.url()).pathname;
  page.on("request", request => { if (isRefresh(request)) refreshRequests++; });
  await page.clock.fastForward(30_000);
  expect(refreshRequests).toBe(0);
  await page.getByRole("banner").getByText("Auto refresh every 10s", { exact: true }).click();
  await page.clock.runFor(9_999);
  expect(refreshRequests).toBe(0);
  for (let cycle = 0; cycle < 2; cycle++) {
    const response = page.waitForResponse(response => isRefresh(response.request()));
    await page.clock.runFor(cycle === 0 ? 1 : 10_000);
    expect((await response).ok()).toBe(true);
    await expect(page.getByRole("banner").getByText("Live", { exact: true })).toBeVisible();
    await expect(automatic).toBeChecked();
  }
  await page.getByRole("banner").getByText("Auto refresh every 10s", { exact: true }).click();
  await expect(automatic).not.toBeChecked();
  await page.clock.fastForward(30_000);
  expect(refreshRequests).toBe(2);
});

test.describe("time zone selection", () => {
  test.use({ timezoneId: "America/New_York" });
  test("switches queue, request history and change timestamps from the shared header", async ({ page }) => {
    const expectTimeZone = async (zone: string) => {
      await expect.poll(() => page.locator("time[datetime]").evaluateAll((times, timeZone) => {
        const formatter = new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeStyle: "long", timeZone });
        return times.length > 0 && times.every(time => time.textContent === formatter.format(new Date(time.getAttribute("datetime")!)));
      }, zone)).toBe(true);
    };
    const timeZones = page.getByRole("banner").getByRole("listbox", { name: "Time zone", exact: true });
    await page.goto(`/queue/${queue}`);
    await expect(timeZones.getByRole("option", { name: "Browser", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("America/New_York");
    await timeZones.getByRole("option", { name: "UTC", exact: true }).click();
    await expectTimeZone("UTC");
    await page.locator(".sq-request-list__item td:first-child a").first().click();
    await expect(timeZones.getByRole("option", { name: "UTC", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("UTC");
    await expect(page.getByText("Received", { exact: true })).toBeVisible();
    await page.getByRole("link", { name: /^History/ }).click();
    await expect(page.getByRole("list", { name: "Request history", exact: true })).toBeVisible();
    await expect(timeZones.getByRole("option", { name: "UTC", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("UTC");
    const changePath = `/queue/${queue}/change/git/git.example.com/demo/refs%2Fheads%2Fpagination`;
    for (const path of [changePath, `${changePath}/7777777777777777777777777777777777777777`]) {
      await page.goto(path);
      await expect(page.getByRole("table", { name: "Change submissions" })).toBeVisible();
      await expect(timeZones.getByRole("option", { name: "UTC", exact: true })).toHaveAttribute("aria-selected", "true");
      await expectTimeZone("UTC");
    }
    await timeZones.getByRole("option", { name: "Browser", exact: true }).click();
    await expectTimeZone("America/New_York");
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });
});
