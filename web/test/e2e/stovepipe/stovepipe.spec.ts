import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { Stovepipe } from "@submitqueue/api/stovepipe";
const client = createClient(
  Stovepipe,
  createGrpcTransport({
    baseUrl: process.env.STOVEPIPE_E2E_URL!,
    defaultTimeoutMs: 5_000,
  }),
);
test.describe("server-rendered styling", () => {
  test.use({ javaScriptEnabled: false });
  test("styles the queue before hydration", async ({ page }) => {
    await page.goto("/web%2Fpagination");
    await expect(page.getByRole("heading", { level: 1 })).toHaveCSS("font-size", "32px");
    await expect(page.getByRole("banner").getByRole("checkbox", { name: "Auto refresh every 10s" })).not.toBeChecked();
    await expect(page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr")).toHaveCount(50);
    await expect(page.getByRole("status")).toContainText("More requests available");
  });
  test("renders request status and views without JavaScript", async ({ page }) => {
    await page.goto("/web%2Fpagination/request/51");
    const status = page.getByRole("region", { name: "Request status" });
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("Partial Breakage", { exact: true })).toBeVisible();
    await expect(status.getByText("git://web/pagination/51?file=src%2Fmain#fragment", { exact: true })).toBeVisible();
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "History", exact: true }).click();
    await expect(page.getByRole("table", { name: "Request history" })).toBeVisible();
    for (const label of ["Build and Test Issues", "Fix Attempts"]) {
      await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: label, exact: true }).click();
      await expect(page.getByRole("region", { name: label, exact: true }).getByText("Coming soon", { exact: true })).toBeVisible();
    }
  });
  test("keeps continuation links usable without JavaScript", async ({ page }) => {
    await page.goto("/web%2Fpagination");
    await page.getByRole("link", { name: "Older requests →" }).click();
    await expect(page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr")).toHaveCount(1);
    expect(new URL(page.url()).searchParams.get("page")).toBeTruthy();
    await page.reload();
    await expect(page.getByRole("link", { name: "1", exact: true })).toBeVisible();
  });
  test("uses the available width on a wide screen", async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto("/web%2Fpagination");
    const table = await page.getByRole("table", { name: "Requests", exact: true }).boundingBox();
    expect(table?.width).toBeGreaterThan(1920 * 0.95);
  });
});
test("keeps queue tables inside a narrow viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/web%2Fpagination");
  await expect(page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr")).toHaveCount(50);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole("link", { name: "51", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Request 51", exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "History", exact: true }).click();
  const history = page.getByRole("region", { name: "Request history", exact: true });
  await history.focus();
  await expect(history).toBeFocused();
  await page.keyboard.press("ArrowRight");
  await expect.poll(() => history.evaluate(element => element.scrollLeft)).toBeGreaterThan(0);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
});
test("shows loading progress without shifting the controls and respects reduced motion", async ({ page }) => {
  let releaseResponse!: () => void;
  const responseGate = new Promise<void>(resolve => { releaseResponse = resolve; });
  let acknowledgeRequest!: () => void;
  const requested = new Promise<void>(resolve => { acknowledgeRequest = resolve; });
  await page.route("**/api/queue-requests?*", async route => {
    acknowledgeRequest();
    await responseGate;
    await route.continue();
  });
  try {
    await page.goto("/web%2Fpagination");
    const rows = page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr");
    const pagination = page.getByRole("region", { name: "Queue pagination", exact: true });
    const control = page.getByRole("button", { name: "Load more requests", exact: true });
    const progress = page.getByRole("progressbar", { name: "Loading older requests", exact: true });
    const appearance = (element: HTMLElement) => {
      const rect = element.getBoundingClientRect();
      const style = getComputedStyle(element);
      return { width: rect.width, height: rect.height, color: style.color, background: style.backgroundColor };
    };
    const before = await control.evaluate(appearance);
    const footerHeight = await pagination.evaluate(element => element.getBoundingClientRect().height);
    await expect(progress).toHaveCount(0);
    await control.scrollIntoViewIfNeeded();
    await requested;
    await expect(control).toHaveAttribute("aria-disabled", "true");
    await expect(page.getByRole("status")).toContainText("Loading older requests…");
    await expect(progress).toBeVisible();
    await expect(progress).toHaveCSS("opacity", "1");
    await expect(rows).toHaveCount(50);
    expect(await control.evaluate(appearance)).toEqual(before);
    expect(await pagination.evaluate(element => element.getBoundingClientRect().height)).toBe(footerHeight);
    expect(await progress.evaluate(element => element.getAnimations({ subtree: true }).some(animation => animation.effect?.getTiming().iterations === Infinity))).toBe(true);
    await page.screenshot({ path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-loading.png` });
    await page.emulateMedia({ reducedMotion: "reduce" });
    await expect.poll(() => progress.evaluate(element => element.getAnimations({ subtree: true }).length)).toBe(0);
    await page.setViewportSize({ width: 390, height: 844 });
    await progress.scrollIntoViewIfNeeded();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.screenshot({ path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-loading-narrow.png` });
    const narrowFooterHeight = await pagination.evaluate(element => element.getBoundingClientRect().height);
    releaseResponse();
    await expect(rows).toHaveCount(51);
    await expect(progress).toHaveCount(0);
    expect(await pagination.evaluate(element => element.getBoundingClientRect().height)).toBe(narrowFooterHeight);
  } finally {
    releaseResponse();
  }
});
test("loads older requests on scroll, retries failures, and restores the list after a detail visit", async ({ page }) => {
  await page.goto("/web%2Fpagination");
  const rows = page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr");
  await expect(rows).toHaveCount(50);
  const continuation = "**/api/queue-requests?*";
  let calls = 0;
  await page.route(continuation, async route => {
    calls++;
    await route.fulfill({ status: 503 });
  });
  await page.getByRole("button", { name: "Load more requests", exact: true }).scrollIntoViewIfNeeded();
  await expect(page.getByRole("alert").filter({ hasText: "Could not load older requests" })).toBeVisible();
  await expect(rows).toHaveCount(50);
  expect(calls).toBe(1);
  await page.unroute(continuation);
  await page.getByRole("button", { name: "Retry loading older requests" }).click();
  await expect(rows).toHaveCount(51);
  await expect(page.getByRole("status")).toContainText("All available requests loaded");
  expect(new URL(page.url()).search).toBe("");
  const scrollY = await page.evaluate(() => window.scrollY);
  await page.getByRole("link", { name: "1", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Request 1", exact: true })).toBeVisible();
  await page.goBack();
  await expect(rows).toHaveCount(51);
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBeGreaterThan(scrollY - 200);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);

  await page.evaluate(() => window.scrollTo(0, 0));
  await page.reload();
  await expect(rows).toHaveCount(50);
  await page.getByRole("button", { name: "Load more requests", exact: true }).scrollIntoViewIfNeeded();
  await expect(rows).toHaveCount(51);
  expect(new URL(page.url()).search).toBe("");
});
test("challenges anonymous continuation requests", async () => {
  const url = new URL("/api/queue-requests?path=%2Fweb%252Fpagination&page=opaque", process.env.STOVEPIPE_E2E_WEB_URL!);
  const response = await fetch(url);
  expect(response.status).toBe(401);
  expect(response.headers.get("www-authenticate")).toContain("Basic");
});
test("refreshes from the header only while auto-refresh is enabled", async ({ page }) => {
  await page.goto("/web%2Fpagination");
  const automatic = page.getByRole("banner").getByRole("checkbox", { name: "Auto refresh every 10s" });
  await expect(automatic).not.toBeChecked();
  const now = new Date();
  await page.clock.install({ time: now });
  await page.clock.pauseAt(now);
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
  test.use({ timezoneId: "America/Los_Angeles" });
  test("defaults to browser time, switches all timestamps, and retains UTC across navigation", async ({ page }) => {
    const errors: string[] = [];
    page.on("pageerror", error => errors.push(error.message));
    page.on("console", message => {
      if (message.type() === "error" && /hydrat|did not match|Minified React error #(?:418|423|425)/iu.test(message.text())) errors.push(message.text());
    });
    const expectTimeZone = async (zone: string) => {
      await expect.poll(() => page.locator("time[datetime]").evaluateAll((times, timeZone) => {
        const formatter = new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeStyle: "long", timeZone });
        return times.length > 0 && times.every(time => time.textContent === formatter.format(new Date(time.getAttribute("datetime")!)));
      }, zone)).toBe(true);
    };
    const timeZones = page.getByRole("banner").getByRole("listbox", { name: "Time zone", exact: true });
    await page.goto("/web%2Fpagination");
    await expect(timeZones.getByRole("option", { name: "Browser", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("America/Los_Angeles");
    await timeZones.getByRole("option", { name: "UTC", exact: true }).click();
    await expectTimeZone("UTC");
    await page.getByRole("link", { name: "51", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Request 51", exact: true })).toBeVisible();
    await expect(timeZones.getByRole("option", { name: "UTC", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("UTC");
    await timeZones.getByRole("option", { name: "Browser", exact: true }).click();
    await expectTimeZone("America/Los_Angeles");
    const browser = timeZones.getByRole("option", { name: "Browser", exact: true });
    await browser.focus();
    await page.keyboard.press("ArrowRight");
    await expect(timeZones.getByRole("option", { name: "UTC", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("UTC");
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.reload();
    await expect(timeZones.getByRole("option", { name: "UTC", exact: true })).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("UTC");
    await browser.click();
    await page.reload();
    await expect(browser).toHaveAttribute("aria-selected", "true");
    await expectTimeZone("America/Los_Angeles");
    expect(errors).toEqual([]);
  });
});
test("real ingestion, queue selection, cursors, request summaries, history, project results and refresh", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error" && /hydrat|did not match|Minified React error #(?:418|423|425)/iu.test(message.text())) errors.push(message.text());
  });
  const id =
    await test.step("Ingest a real request and wait for its projection", async () => {
      const { id } = await client.ingest({ queue: "monorepo/main" });
      await expect
        .poll(
          async () =>
            (await client.list({ queue: "monorepo/main" })).requests.some(
              (request) => request.requestId === id,
            ),
          { timeout: 0 },
        )
        .toBe(true);
      return id;
    });
  const requests = page.getByRole("table", { name: "Requests", exact: true });
  const queues = page.getByRole("navigation", { name: "Queues" });

  await test.step("Open the ingested request, read history and refresh", async () => {
    await page.goto("/monorepo%2Fmain");
    await expect(
      page.getByRole("heading", { name: "monorepo/main", exact: true }),
    ).toBeVisible();
    await page
      .getByRole("table", { name: "Requests", exact: true })
      .getByRole("row")
      .filter({ has: page.getByRole("cell", { name: id, exact: true }) })
      .getByRole("link", { name: id, exact: true })
      .click();
    await expect(
      page.getByRole("heading", { name: `Request ${id}`, exact: true }),
    ).toBeVisible();
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "History", exact: true }).click();
    await expect(
      page.getByRole("table", { name: "Request history" }).locator("tbody tr"),
    ).not.toHaveCount(0);
    await expect(page).toHaveURL(new RegExp(`/monorepo%2Fmain/request/${id}\\?view=history$`));
    await expect(
      page.getByRole("region", { name: "Request status" }),
    ).toBeVisible();
    await page.reload();
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });
  await test.step("Select the populated queue", async () => {
    await queues
      .getByRole("link", { name: "web/pagination", exact: true })
      .click();
    await expect(requests.locator("tbody tr")).toHaveCount(50);
    await page.screenshot({
      path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-queue.png`,
      fullPage: true,
    });
  });
  await test.step("Reload a request ID and display its summary above history", async () => {
    await requests
      .getByRole("link", {
        name: "51",
        exact: true,
      })
      .click();
    await expect
      .poll(() => new URL(page.url()).pathname)
      .toBe(
        "/web%2Fpagination/request/51",
      );
    await page.reload();
    await expect(
      page.getByRole("heading", { name: "Request 51", exact: true }),
    ).toBeVisible();
    const status = page.getByRole("region", { name: "Request status" });
    await expect(status.getByText("failed", { exact: true })).toBeVisible();
    await expect(
      status.getByText("git://web/pagination/51?file=src%2Fmain#fragment", {
        exact: true,
      }),
    ).toBeVisible();
    await expect(status.getByText("Accepted", { exact: true })).toBeVisible();
    await expect(status.getByText("Updated", { exact: true })).toBeVisible();
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "History", exact: true }).click();
    await expect(
      page.getByRole("table", { name: "Request history" }).getByText("accepted", {
        exact: true,
      }),
    ).toBeVisible();
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "Project Results", exact: true }).click();
    await expect(page.getByRole("table", { name: "Recorded project results" })).toBeVisible();
    await page.screenshot({
      path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-request.png`,
    });
  });
  await test.step("Read recorded project results and reload their cursor", async () => {
    const status = page.getByRole("region", { name: "Request status", exact: true });
    const section = page.getByRole("region", { name: "Project Results", exact: true });
    const projects = section.getByRole("table", { name: "Recorded project results", exact: true });
    await expect(projects.locator("tbody tr")).toHaveCount(50);
    await expect(projects.locator("tbody tr").nth(0).getByRole("cell", { name: "project-50", exact: true })).toBeVisible();
    await expect(projects.locator("tbody tr").nth(1).getByRole("cell", { name: "project-01", exact: true })).toBeVisible();
    await expect(projects.locator("tbody tr").nth(2).getByRole("cell", { name: "project-02", exact: true })).toBeVisible();
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("Partial Breakage", { exact: true })).toBeVisible();
    await expect(projects.getByText(/Repository|%/)).toHaveCount(0);
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-01", exact: true }),
      }).getByRole("cell", { name: "Partial Breakage", exact: true }),
    ).toBeVisible();
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-50", exact: true }),
      }).getByRole("cell", { name: "Breakage", exact: true }),
    ).toBeVisible();
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-02", exact: true }),
      }).getByRole("cell", { name: "Green", exact: true }),
    ).toBeVisible();
    await page.reload();
    await expect(projects.locator("tbody tr")).toHaveCount(50);
    await section.getByRole("link", { name: "Next projects →", exact: true }).first().click();
    await expect(projects.locator("tbody tr")).toHaveCount(1);
    await expect(projects.getByRole("cell", { name: "project-51", exact: true })).toBeVisible();
    const cursor = new URL(page.url()).searchParams.get("projects");
    expect(cursor).toBeTruthy();
    await page.reload();
    await expect(projects.locator("tbody tr")).toHaveCount(1);
    await expect(status.getByText("Partial Breakage", { exact: true })).toBeVisible();
    expect(new URL(page.url()).searchParams.get("projects")).toBe(cursor);
    await expect(section.getByRole("link", { name: "Next projects →", exact: true })).toHaveCount(0);
    await section.getByRole("link", { name: "First projects", exact: true }).last().click();
    await expect(projects.locator("tbody tr")).toHaveCount(50);
    expect(new URL(page.url()).searchParams.has("projects")).toBe(false);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });
  await test.step("Keep another request's missing results distinct from green", async () => {
    await page.goto("/web%2Fpagination/request/49");
    const section = page.getByRole("region", { name: "Project Results", exact: true });
    await expect(section.getByText("No project results recorded on this page.", { exact: true })).toBeVisible();
    const projects = section.getByRole("table", { name: "Recorded project results", exact: true });
    await expect(projects.locator("tbody tr")).toHaveCount(0);
    const status = page.getByRole("region", { name: "Request status", exact: true });
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("No result recorded", { exact: true })).toBeVisible();
    await expect(page.getByText("Green", { exact: true })).toHaveCount(0);
  });
  await test.step("Scroll through the queue, then select an empty queue", async () => {
    await page.getByRole("link", { name: "← Queue requests" }).click();
    await page.getByRole("button", { name: "Load more requests", exact: true }).scrollIntoViewIfNeeded();
    await expect(requests.locator("tbody tr")).toHaveCount(51);
    expect(new URL(page.url()).searchParams.has("page")).toBe(false);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await queues.getByRole("link", { name: "web/empty", exact: true }).click();
    await expect(
      page.getByText("No retained requests on this queue."),
    ).toBeVisible();
  });
  expect(errors).toEqual([]);
});


test("fills the available height and scrolls long results within each tab", async ({ page }) => {
  for (const [view, label] of [["projects", "Recorded project results"], ["history", "Request history"]]) {
    await page.setViewportSize({ width: 1280, height: 900 });
    await page.goto(`/web%2Fpagination/request/51?view=${view}`);
    const table = page.getByRole("region", { name: label, exact: true });
    const initialHeight = await table.evaluate(element => element.clientHeight);
    await page.setViewportSize({ width: 1280, height: 1200 });
    await expect.poll(() => table.evaluate(element => element.clientHeight)).toBeGreaterThan(initialHeight);
    await table.focus();
    await page.keyboard.press("End");
    await expect.poll(() => table.evaluate(element => element.scrollTop)).toBeGreaterThan(0);
    expect(await page.evaluate(() => window.scrollY)).toBe(0);
  }
});

test("pages through project results without reloading or moving the surrounding page", async ({ page }) => {
  await page.setViewportSize({ width: 1000, height: 650 });
  await page.goto("/web%2Fpagination/request/51");
  const rows = page.getByRole("table", { name: "Recorded project results" }).locator("tbody tr");
  const projectScroll = page.getByRole("region", { name: "Recorded project results", exact: true });
  const pager = page.getByRole("navigation", { name: "Project pages (bottom)" });
  await expect(rows).toHaveCount(50);
  await page.evaluate(() => { document.documentElement.dataset.pageSession = "retained"; });
  await projectScroll.focus();
  await page.keyboard.press("End");
  await expect.poll(() => projectScroll.evaluate(element => element.scrollTop)).toBeGreaterThan(0);
  await pager.scrollIntoViewIfNeeded();
  const scrollBeforeNext = await page.evaluate(() => window.scrollY);
  await pager.getByRole("link", { name: "Next projects →", exact: true }).click();
  await expect(rows).toHaveCount(1);
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scrollBeforeNext);
  expect(await projectScroll.evaluate(element => element.scrollTop)).toBe(0);
  await expect(pager.getByRole("link", { name: "Next projects →", exact: true })).toHaveCount(0);
  await page.goBack();
  await expect(rows).toHaveCount(50);
  await page.goForward();
  await expect(rows).toHaveCount(1);
  await pager.scrollIntoViewIfNeeded();
  const scrollBeforeFirst = await page.evaluate(() => window.scrollY);
  await pager.getByRole("link", { name: "First projects", exact: true }).click();
  await expect(rows).toHaveCount(50);
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scrollBeforeFirst);
  expect(await page.evaluate(() => document.documentElement.dataset.pageSession)).toBe("retained");
});

test("retains project paging across views and resets the cursor when page size changes", async ({ page }) => {
  await page.goto("/web%2Fpagination/request/51");
  const rows = page.getByRole("table", { name: "Recorded project results" }).locator("tbody tr");
  const views = page.getByRole("navigation", { name: "Request views" });
  const pager = page.getByRole("navigation", { name: "Project pages (bottom)" });
  const selectSize = async (size: string) => {
    await pager.getByRole("combobox", { name: /Projects per page/u }).click();
    await page.getByRole("option", { name: size, exact: true }).click();
  };
  await expect(rows).toHaveCount(50);
  await page.evaluate(() => { document.documentElement.dataset.pageSession = "retained"; });
  await pager.scrollIntoViewIfNeeded();
  const scrollBeforeSize = await page.evaluate(() => window.scrollY);
  await selectSize("25");
  await expect(rows).toHaveCount(25);
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBe(scrollBeforeSize);
  const firstProjects = await rows.locator("td:first-child").allTextContents();
  await pager.getByRole("link", { name: "Next projects →", exact: true }).click();
  await expect.poll(() => rows.locator("td:first-child").allTextContents()).not.toEqual(firstProjects);
  const nextProjects = await rows.locator("td:first-child").allTextContents();
  expect(nextProjects.every(project => !firstProjects.includes(project))).toBe(true);
  const cursor = new URL(page.url()).searchParams.get("projects");
  expect(cursor).toBeTruthy();
  for (const label of ["History", "Build and Test Issues", "Fix Attempts"]) {
    await views.getByRole("link", { name: label, exact: true }).click();
    const section = page.getByRole("region", { name: label, exact: true });
    await expect(section).toBeVisible();
    if (label !== "History") await expect(section.getByText("Coming soon", { exact: true })).toBeVisible();
  }
  await views.getByRole("link", { name: "Project Results", exact: true }).click();
  await expect(rows).toHaveCount(25);
  expect(new URL(page.url()).searchParams.get("projects")).toBe(cursor);
  expect(new URL(page.url()).searchParams.get("projectPageSize")).toBe("25");
  expect(await rows.locator("td:first-child").allTextContents()).toEqual(nextProjects);
  await selectSize("100");
  await expect(rows).toHaveCount(51);
  expect(new URL(page.url()).searchParams.has("projects")).toBe(false);
  expect(await page.evaluate(() => document.documentElement.dataset.pageSession)).toBe("retained");
  await page.reload();
  await expect(rows).toHaveCount(51);
  expect(new URL(page.url()).searchParams.get("projectPageSize")).toBe("100");
});
