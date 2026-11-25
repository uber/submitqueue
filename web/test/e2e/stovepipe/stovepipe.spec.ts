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
  for (const scheme of ["light", "dark"] as const) {
    test(`follows a ${scheme} system on first visit before JavaScript`, async ({ page, context }) => {
      await page.emulateMedia({ colorScheme: scheme });
      await context.addCookies([{ name: "submitqueue.appearance", value: scheme, url: process.env.STOVEPIPE_E2E_WEB_URL! }]);
      await page.goto("/web%2Fpagination");
      const expected = await page.locator(".sq-app").evaluate(element => ({
        background: getComputedStyle(element).backgroundColor,
        color: getComputedStyle(element).color,
      }));
      await context.clearCookies({ name: "submitqueue.appearance" });
      await page.reload();
      await expect(page.locator(".sq-app")).toHaveCSS("background-color", expected.background);
      await expect(page.locator(".sq-app")).toHaveCSS("color", expected.color);
      await expect(page.locator("html")).toHaveCSS("background-color", expected.background);
      await expect(page.locator("html")).toHaveCSS("color-scheme", "light dark");
    });
  }
  test("styles the queue and its saved appearance before hydration", async ({ page, context }) => {
    await page.goto("/web%2Fpagination");
    await expect(page.getByRole("heading", { level: 1 })).toHaveCSS("font-size", "32px");
    await expect(page.getByRole("banner").getByRole("button", { name: /auto-refresh/i })).toHaveAttribute("aria-pressed", "false");
    await expect(page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr")).toHaveCount(50);
    await expect(page.getByRole("status")).toContainText("More requests available");
    const app = page.locator(".sq-app");
    const light = await app.evaluate(element => getComputedStyle(element).backgroundColor);
    await context.addCookies([{ name: "submitqueue.appearance", value: "dark", url: page.url() }]);
    await page.reload();
    await expect(app).not.toHaveCSS("background-color", light);
    await expect(page.locator("html")).toHaveCSS("color-scheme", "dark");
    await expect(page.locator("html")).toHaveCSS("background-color",
      await app.evaluate(element => getComputedStyle(element).backgroundColor));
  });
  test("renders grouped open issues and continuation links without JavaScript", async ({ page }) => {
    await page.goto("/web%2Fpagination?view=issues");
    await expect(page.getByRole("heading", { name: "buildtest", exact: true })).toBeVisible();
    const rows = page.getByRole("table", { name: "Issues for buildtest" }).locator("tbody tr");
    await expect(rows).toHaveCount(50);
    await expect(rows.first()).toContainText("Task-wide failure");
    await expect(page.getByRole("link", { name: "Load more", exact: true })).toHaveAttribute("href", /view=issues&page=/);
    await page.getByRole("link", { name: "Load more", exact: true }).click();
    await expect(page.getByRole("heading", { name: "expensive", exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: "First page", exact: true })).toHaveAttribute("href", "/web%2Fpagination?view=issues");
  });
  test("browses issue history across requests without JavaScript", async ({ page }) => {
    await page.goto("/web%2Fpagination?view=issues");
    await page.getByRole("table", { name: "Issues for buildtest" }).locator("tbody tr").first().getByRole("link", { name: "View history" }).click();
    const table = page.getByRole("table", { name: "Issue history events" });
    await expect(table.locator("tbody tr")).toHaveCount(50);
    await expect(table).toContainText("Reported");
    await expect(table).toContainText("Closed");
    await expect(page.getByText("Task-wide failure", { exact: true })).toBeVisible();
    await page.getByRole("link", { name: "Next events →" }).click();
    await expect(table.locator("tbody tr")).toHaveCount(1);
    await expect(table).toContainText("Opened");
    await page.reload();
    await expect(table.locator("tbody tr")).toHaveCount(1);
    await page.getByRole("link", { name: "First events", exact: true }).click();
    await expect(table.locator("tbody tr")).toHaveCount(50);
    await table.getByRole("link", { name: "51", exact: true }).first().click();
    await expect(page.getByRole("table", { name: "Reported Issues" })).toBeVisible();
  });
  test("renders request status and views without JavaScript", async ({ page }) => {
    await page.goto("/web%2Fpagination/request/51");
    const status = page.getByRole("region", { name: "Request status" });
    await expect(status.getByText("Broken projects", { exact: true })).toBeVisible();
    await expect(status.getByText("2 of 51", { exact: true })).toBeVisible();
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("Partial Breakage", { exact: true })).toBeVisible();
    await expect(status.getByText("git://web/pagination/51?file=src%2Fmain#fragment", { exact: true })).toBeVisible();
    const project = page.getByRole("table", { name: "Recorded project results" }).getByRole("row").filter({ has: page.getByRole("cell", { name: "project-01", exact: true }) }).first();
    await project.getByRole("link", { name: "Show issues" }).click();
    await expect(page.getByRole("table", { name: "Issues affecting project-01" }).locator("tbody tr")).toHaveCount(50);
    await project.getByRole("link", { name: "Hide issues" }).click();
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "History", exact: true }).click();
    await expect(page.getByRole("table", { name: "Request history" })).toBeVisible();
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "Build and Test Issues", exact: true }).click();
    const issues = page.getByRole("region", { name: "Build and Test Issues", exact: true });
    await expect(issues.getByRole("status")).toHaveText("Issue recording complete");
    await expect(issues.locator("tbody tr")).toHaveCount(50);
    await expect(issues.getByRole("table", { name: "Opened Issues" })).toContainText("Task-wide failure");
    const existing = issues.getByRole("table", { name: "Reported Issues" }).getByRole("row").filter({ hasText: "//packages/test-01:go_default_test" });
    await existing.getByRole("link", { name: "Show projects" }).click();
    await expect(issues.getByRole("region", { name: "Affected projects for buildtest //packages/test-01:go_default_test" }).getByRole("listitem")).toHaveCount(50);
    await existing.getByRole("link", { name: "Hide projects" }).click();
    await page.getByRole("link", { name: "Next issues →" }).click();
    await expect(issues.locator("tbody tr")).toHaveCount(1);
    await page.getByRole("link", { name: "First issues" }).click();
    await expect(issues.locator("tbody tr")).toHaveCount(50);
    await page.getByRole("navigation", { name: "Request views" }).getByRole("link", { name: "Fix Attempts", exact: true }).click();
    await expect(page.getByRole("region", { name: "Fix Attempts", exact: true }).getByText("Coming soon", { exact: true })).toBeVisible();
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

test("toggles and remembers appearance directly from the Stovepipe header", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/web%2Fpagination");
  const app = page.locator(".sq-app");
  const dark = await app.evaluate(element => getComputedStyle(element).backgroundColor);
  const toggle = page.getByRole("banner").getByRole("button", { name: /^Switch to (light|dark) mode$/ });
  await expect(toggle).toHaveAccessibleName("Switch to light mode");
  await toggle.click();
  await expect(toggle).toHaveAccessibleName("Switch to dark mode");
  await expect(page.getByRole("dialog", { name: "Appearance" })).toHaveCount(0);
  await expect(app).not.toHaveCSS("background-color", dark);
  const light = await app.evaluate(element => getComputedStyle(element).backgroundColor);
  await page.reload();
  await expect(app).toHaveCSS("background-color", light);
  await expect(toggle).toHaveAccessibleName("Switch to dark mode");
  await toggle.focus();
  await toggle.press("Enter");
  await expect(app).toHaveCSS("background-color", dark);
  await expect(toggle).toHaveAccessibleName("Switch to light mode");
});

test("keeps the timestamp popover themed and aligned while toggling appearance", async ({ page }) => {
  const closeIconVerticalOffset = (element: HTMLElement) => {
    const heading = element.querySelector("h2")!.getBoundingClientRect();
    const icon = element.querySelector("button svg")!.getBoundingClientRect();
    return Math.abs(heading.y + heading.height / 2 - icon.y - icon.height / 2);
  };
  const countShadowLayers = (element: HTMLElement) => {
    let count = 0;
    for (let node: HTMLElement | null = element; node && node !== document.body; node = node.parentElement) {
      if (getComputedStyle(node).boxShadow !== "none") count += 1;
    }
    return count;
  };
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/web%2Fpagination/request/51");
  const app = page.locator(".sq-app");
  for (const mode of ["system", "light", "dark"] as const) {
    if (mode !== "system") {
      await page.getByRole("banner").getByRole("button", { name: `Switch to ${mode} mode`, exact: true }).click();
    }
    const background = await app.evaluate(element => getComputedStyle(element).backgroundColor);
    const foreground = await app.evaluate(element => getComputedStyle(element).color);
    // A host may not theme its document; portaled controls still need the correct scheme.
    await page.evaluate(() => { document.documentElement.style.colorScheme = "light"; });
    await page.getByRole("button", { name: /timestamp options$/ }).first().click();
    const timestamp = page.getByRole("dialog", { name: "Timestamp", exact: true });
    await expect(timestamp).toHaveCSS("color", foreground);
    expect(await timestamp.evaluate(element => getComputedStyle(element.parentElement!).backgroundColor)).toBe(background);
    expect(await timestamp.evaluate(countShadowLayers)).toBe(1);
    expect(await timestamp.evaluate(closeIconVerticalOffset)).toBeLessThan(0.5);
    await expect(timestamp.getByRole("radio", { name: "Local", exact: true }))
      .toHaveCSS("color-scheme", mode === "light" ? "light" : "dark");
    await expect(page.getByRole("tooltip")).toHaveCount(0);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await timestamp.getByRole("button", { name: "Close timestamp options" }).click();
  }
});

test("expands request issue projects with paginated links and browser history", async ({ page }) => {
  await page.goto("/web%2Fpagination/request/51?view=issues");
  const row = page.getByRole("table", { name: "Reported Issues" }).getByRole("row").filter({ hasText: "//packages/test-01:go_default_test" });
  await row.getByRole("link", { name: "Show projects" }).click();
  const projects = page.getByRole("region", { name: "Affected projects for buildtest //packages/test-01:go_default_test" });
  await expect(projects.getByRole("listitem")).toHaveCount(50);
  await projects.getByRole("link", { name: "More affected projects →" }).click();
  await expect(projects.getByRole("listitem")).toHaveCount(1);
  await expect(projects).toContainText("project-51");
  await page.goBack();
  await expect(projects.getByRole("listitem")).toHaveCount(50);
  await row.getByRole("link", { name: "Hide projects" }).click();
  await expect(projects).toHaveCount(0);
});

test("expands a project's request-scoped issues and pages them independently", async ({ page }) => {
  await page.goto("/web%2Fpagination/request/51?projectPageSize=25");
  const results = page.getByRole("table", { name: "Recorded project results", exact: true });
  const row = results.getByRole("row").filter({ has: page.getByRole("cell", { name: "project-01", exact: true }) }).first();
  await row.getByRole("link", { name: "Show issues" }).click();
  const attribution = page.getByRole("region", { name: "Issue attribution for project-01" });
  const issues = attribution.getByRole("table", { name: "Issues affecting project-01" });
  await expect(issues.locator("tbody tr")).toHaveCount(50);
  await expect(issues).toContainText("//packages/test-01:go_default_test");
  await attribution.getByRole("link", { name: "More project issues →" }).click();
  await expect(issues.locator("tbody tr")).toHaveCount(1);
  expect(new URL(page.url()).searchParams.get("projectPageSize")).toBe("25");
  await page.goBack();
  await expect(issues.locator("tbody tr")).toHaveCount(50);
  await row.getByRole("link", { name: "Hide issues" }).click();
  await expect(attribution).toHaveCount(0);
  expect(new URL(page.url()).searchParams.get("projectPageSize")).toBe("25");
});

test("loads and retries issue pages, merges task groups, and refreshes from the beginning", async ({ page }) => {
  await page.goto("/web%2Fpagination");
  await page.getByRole("navigation", { name: "Queue views" }).getByRole("link", { name: "Open issues", exact: true }).click();
  const region = page.getByRole("region", { name: "Open issues", exact: true });
  const rows = region.locator("tbody tr");
  await expect(rows).toHaveCount(50);
  await expect(rows.first()).toContainText("Task-wide failure");
  await page.screenshot({ path: test.info().outputPath("open-issues-desktop.png") });
  const endpoint = "**/api/open-issues?*";
  await page.route(endpoint, route => route.fulfill({ status: 503 }));
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  await expect(region.getByRole("alert")).toContainText("Could not load more issues");
  await expect(rows).toHaveCount(50);
  await page.unroute(endpoint);
  await page.getByRole("button", { name: "Retry loading issues" }).click();
  await expect(rows).toHaveCount(53);
  await expect(page.getByRole("heading", { name: "buildtest", exact: true })).toHaveCount(1);
  await expect(page.getByRole("heading", { name: "expensive", exact: true })).toBeVisible();
  await expect(region.getByRole("status")).toContainText("53 issues loaded");
  await expect(rows.first().getByRole("link", { name: "51", exact: true })).toHaveAttribute("href", "/web%2Fpagination/request/51");
  for (const viewport of [{ width: 1280, height: 900 }, { width: 390, height: 844 }]) {
    await page.setViewportSize(viewport);
    const columns = await region.getByRole("table").evaluateAll(tables => tables.map(table =>
      [...table.querySelectorAll("thead th")].map(cell => {
        const { x, width } = cell.getBoundingClientRect();
        return { x, width };
      })));
    expect(columns).toHaveLength(2);
    expect(columns[1]).toEqual(columns[0]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  }
  await page.reload();
  await expect(rows).toHaveCount(50);
  await expect(page.getByRole("heading", { name: "expensive", exact: true })).toHaveCount(0);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: test.info().outputPath("open-issues-mobile.png") });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
});

test("shows retained history metadata, including zero values, in the details column", async ({ page }) => {
  await page.goto("/web%2Fpagination/request/51?view=history");
  const history = page.getByRole("table", { name: "Request history", exact: true });
  await expect(history.getByRole("columnheader", { name: "Details", exact: true })).toBeVisible();
  for (const label of ["build_triggered", "build_finished", "build_analysis_abandoned", "failed"]) {
    const row = history.getByRole("row").filter({ has: page.getByRole("cell", { name: label, exact: true }) });
    await expect(row.getByRole("cell", { name: "Build ID: runner/build/51?attempt=2", exact: true })).toBeVisible();
  }
  for (const text of ["Breakage degree: 0", "Breakage degree: 0.25", "Project facts: 0 Broken project facts: 0", "Project facts: 51 Broken project facts: 2"])
    await expect(history.getByRole("cell", { name: text, exact: true })).toBeVisible();
  const analysis = history.getByRole("row").filter({ has: page.getByRole("cell", { name: "build_analysis_completed", exact: true }) });
  await expect(analysis.filter({ hasText: "runner/build/51?attempt=2" }).getByRole("cell").last()).toHaveText("Build ID: runner/build/51?attempt=2Issues: 0");
  await expect(analysis.filter({ hasText: "runner/build/50" }).getByRole("cell").last()).toHaveText("Build ID: runner/build/50Issues: 0");
  const reconciled = history.getByRole("row").filter({ has: page.getByRole("cell", { name: "issues_reconciled", exact: true }) });
  await expect(reconciled.getByRole("cell").last()).toHaveText("Build ID: runner/build/51?attempt=2Closure mode: suppressed_task_wide_issue");
  for (const [event, unit, decision] of [
    ["fix_submitted", "Unit ID: //pkg:test", "Fix ID: fix/51?attempt=2"],
    ["fix_submission_skipped", "", "Reason: no_open_issue"],
    ["fix_submission_abandoned", "Unit ID: //pkg:test", "Reason: acceptance_unknown"],
  ]) {
    const row = history.getByRole("row").filter({ has: page.getByRole("cell", { name: event, exact: true }) });
    await expect(row.getByRole("cell").last()).toHaveText(`Build ID: runner/build/51?attempt=2Task ID: buildtest${unit}Executor ID: repair${decision}`);
  }
  const accepted = history.getByRole("row").filter({ has: page.getByRole("cell", { name: "accepted", exact: true }) });
  await expect(accepted.getByRole("cell").last()).toHaveText("—");
  await page.screenshot({ path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-history-details.png` });
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.goto("/web%2Fpagination/request/50?view=history");
  const abandoned = page.getByRole("table", { name: "Request history", exact: true }).getByRole("row").filter({ has: page.getByRole("cell", { name: "issue_tracking_abandoned", exact: true }) });
  await expect(abandoned.getByRole("cell").last()).toHaveText("Build ID: runner/build/50");
});

test("continues past an empty issue batch instead of claiming the queue has no issues", async ({ page }) => {
  await page.goto("/web%2Fempty?view=issues");
  await expect(page.getByText("No matching issues in this batch. More results are available.")).toBeVisible();
  await expect(page.getByText("No tracked open issues.", { exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Load more", exact: true }).click();
  await expect(page.getByRole("heading", { name: "z-open", exact: true })).toBeVisible();
  await expect(page.getByText("Task-wide failure", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Load more", exact: true })).toHaveCount(0);
});
test("keeps auto-refresh enabled when an issue cursor refresh returns to the first page", async ({ page }) => {
  const first = await client.listOpenIssues({ queue: "web/pagination", pageSize: 50 });
  await page.goto(`/web%2Fpagination?view=issues&page=${encodeURIComponent(first.nextPageToken)}`);
  const region = page.getByRole("region", { name: "Open issues", exact: true });
  await expect(region.getByRole("status")).toContainText("3 issues loaded");
  const now = new Date();
  await page.clock.install({ time: now });
  // Pause ahead of installation so the running clock cannot overtake the target.
  await page.clock.pauseAt(new Date(now.getTime() + 60_000));
  const automatic = page.getByRole("banner").getByRole("button", { name: /auto-refresh/i });
  await automatic.click();
  const response = page.waitForResponse(response =>
    response.request().headers()["rsc"] === "1" && new URL(response.url()).pathname === "/web%2Fpagination");
  await page.clock.runFor(10_000);
  expect((await response).ok()).toBe(true);
  await expect(region.getByRole("status")).toContainText("50 issues loaded");
  await expect(automatic).toHaveAttribute("aria-pressed", "true");
  expect(new URL(page.url()).searchParams.has("page")).toBe(false);
  await expect(page.getByRole("banner").getByRole("status")).toHaveText("Live");
});
test("keeps loaded issue pages across polls until the viewer selects refreshed results", async ({ page }) => {
  await page.goto("/web%2Fpagination?view=issues");
  const region = page.getByRole("region", { name: "Open issues", exact: true });
  await region.getByRole("button", { name: "Load more", exact: true }).click();
  await expect(region.getByRole("status")).toContainText("53 issues loaded");
  const now = new Date();
  await page.clock.install({ time: now });
  // Pause ahead of installation so the running clock cannot overtake the target.
  await page.clock.pauseAt(new Date(now.getTime() + 60_000));
  const automatic = page.getByRole("banner").getByRole("button", { name: /auto-refresh/i });
  await automatic.click();
  for (let cycle = 0; cycle < 2; cycle++) {
    const response = page.waitForResponse(response =>
      response.request().headers()["rsc"] === "1" && new URL(response.url()).pathname === "/web%2Fpagination");
    await page.clock.runFor(10_000);
    expect((await response).ok()).toBe(true);
    await expect(page.getByRole("banner").getByRole("status")).toHaveText("Live");
    await expect(region.getByRole("status")).toContainText("53 issues loaded");
    await expect(region.getByRole("heading", { name: "expensive", exact: true })).toBeVisible();
    await expect(automatic).toHaveAttribute("aria-pressed", "true");
  }
  await region.getByRole("button", { name: "Show latest issues", exact: true }).click();
  await expect(region.getByRole("status")).toContainText("50 issues loaded");
  await expect(region.getByRole("heading", { name: "expensive", exact: true })).toHaveCount(0);
  await expect(region.getByRole("button", { name: "Show latest issues", exact: true })).toHaveCount(0);
  await expect(automatic).toHaveAttribute("aria-pressed", "true");
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
  const automatic = page.getByRole("banner").getByRole("button", { name: /auto-refresh/i });
  await expect(automatic).toHaveAttribute("aria-pressed", "false");
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
  await automatic.click();
  await page.clock.runFor(9_999);
  expect(refreshRequests).toBe(0);
  for (let cycle = 0; cycle < 2; cycle++) {
    const response = page.waitForResponse(response => isRefresh(response.request()));
    await page.clock.runFor(cycle === 0 ? 1 : 10_000);
    expect((await response).ok()).toBe(true);
    await expect(page.getByRole("banner").getByRole("status")).toHaveText("Live");
    await expect(automatic).toHaveAttribute("aria-pressed", "true");
  }
  await automatic.click();
  await expect(automatic).toHaveAttribute("aria-pressed", "false");
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
    const timeZones = page.getByRole("dialog", { name: "Timestamp", exact: true });
    const openTimeZones = () => page.getByRole("button", { name: /timestamp options$/ }).first().click();
    const closeTimeZones = () => timeZones.getByRole("button", { name: "Close timestamp options" }).click();
    await page.goto("/web%2Fpagination");
    await expect(page.getByRole("banner").getByRole("listbox", { name: "Time zone", exact: true })).toHaveCount(0);
    await openTimeZones();
    await expect(timeZones.getByRole("radio", { name: "Local", exact: true })).toBeChecked();
    await expectTimeZone("America/Los_Angeles");
    await timeZones.getByRole("radiogroup", { name: "Display timezone" }).getByText("UTC", { exact: true }).click();
    await expectTimeZone("UTC");
    await closeTimeZones();
    await page.getByRole("link", { name: "51", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Request 51", exact: true })).toBeVisible();
    await openTimeZones();
    await expect(timeZones.getByRole("radio", { name: "UTC", exact: true })).toBeChecked();
    await expectTimeZone("UTC");
    await timeZones.getByRole("radiogroup", { name: "Display timezone" }).getByText("Local", { exact: true }).click();
    await expectTimeZone("America/Los_Angeles");
    const browser = timeZones.getByRole("radio", { name: "Local", exact: true });
    await browser.focus();
    await page.keyboard.press("ArrowRight");
    await expect(timeZones.getByRole("radio", { name: "UTC", exact: true })).toBeChecked();
    await expectTimeZone("UTC");
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.reload();
    await openTimeZones();
    await expect(timeZones.getByRole("radio", { name: "UTC", exact: true })).toBeChecked();
    await expectTimeZone("UTC");
    await timeZones.getByRole("radiogroup", { name: "Display timezone" }).getByText("Local", { exact: true }).click();
    await page.reload();
    await openTimeZones();
    await expect(browser).toBeChecked();
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
    await expect(requests.getByRole("columnheader", { name: "Broken projects", exact: true })).toBeVisible();
    await expect(requests.getByText("2 of 51", { exact: true })).toBeVisible();
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
    await expect(status.getByText("2 of 51", { exact: true })).toBeVisible();
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
  const projectColumns = () => page.getByRole("table", { name: "Recorded project results" })
    .locator("th").evaluateAll(cells => cells.map(cell => {
      const { x, width } = cell.getBoundingClientRect();
      return { x, width };
    }));
  const firstPageColumns = await projectColumns();
  await page.evaluate(() => { document.documentElement.dataset.pageSession = "retained"; });
  await projectScroll.focus();
  await page.keyboard.press("End");
  await expect.poll(() => projectScroll.evaluate(element => element.scrollTop)).toBeGreaterThan(0);
  await pager.scrollIntoViewIfNeeded();
  const scrollBeforeNext = await page.evaluate(() => window.scrollY);
  await pager.getByRole("link", { name: "Next projects →", exact: true }).click();
  await expect(rows).toHaveCount(1);
  expect(await projectColumns()).toEqual(firstPageColumns);
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
    if (label === "Build and Test Issues") {
      await expect(section.getByRole("status")).toHaveText("Issue recording complete");
      await expect(section.getByRole("table", { name: "Opened Issues" })).toBeVisible();
    }
    if (label === "Fix Attempts") await expect(section.getByText("Coming soon", { exact: true })).toBeVisible();
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


test("opens unit history from request issues with shareable URLs and accessible tables", async ({ page }) => {
  await page.goto("/web%2Fpagination/request/51?view=issues");
  const row = page.getByRole("table", { name: "Reported Issues" }).getByRole("row").filter({ hasText: "//packages/test-01:go_default_test" });
  await row.getByRole("link", { name: "View history" }).click();
  await expect(page).toHaveURL(/view=issue-history/);
  const url = new URL(page.url());
  expect(url.searchParams.get("task")).toBe("buildtest");
  expect(url.searchParams.get("unit")).toBe("//packages/test-01:go_default_test");
  const table = page.getByRole("table", { name: "Issue history events" });
  await expect(table.locator("tbody tr")).toHaveCount(4);
  await expect(table).toContainText("Closed");
  await expect(table).toContainText("Reported");
  await expect(page.getByRole("link", { name: "Next events →" })).toHaveCount(0);
  const accessibility = await new AxeBuilder({ page }).analyze();
  expect(accessibility.violations).toEqual([]);
  await page.screenshot({ path: test.info().outputPath("issue-history-desktop.png") });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(table).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: test.info().outputPath("issue-history-mobile.png") });
  await page.goBack();
  await expect(row).toBeVisible();
});
