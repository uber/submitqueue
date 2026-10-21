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
    await expect(page.getByRole("button", { name: "Refresh", exact: true })).toHaveCSS("background-color", "rgb(0, 0, 0)");
    await expect(page.getByRole("table", { name: "Requests", exact: true }).locator("tbody tr")).toHaveCount(50);
  });
  test("separates request status fields before hydration", async ({ page }) => {
    await page.goto("/web%2Fpagination/request/51");
    const status = page.getByRole("region", { name: "Request status" });
    const rows = status.locator("dl > div");
    await expect(rows).toHaveCount(7);
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("Breakage", { exact: true })).toBeVisible();
    const layout = await rows.evaluateAll(fields => fields.map(field => ({
      height: field.getBoundingClientRect().height,
      labelGap: field.querySelector("dd")!.getBoundingClientRect().top - field.querySelector("dt")!.getBoundingClientRect().bottom,
      separator: parseFloat(getComputedStyle(field).borderBottomWidth),
    })));
    for (const row of layout) {
      expect(row.height).toBeGreaterThanOrEqual(60);
      expect(row.labelGap).toBeGreaterThanOrEqual(6);
      expect(row.separator).toBeGreaterThan(0);
    }
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
  const history = page.getByRole("region", { name: "Request history", exact: true });
  await history.focus();
  await expect(history).toBeFocused();
  await page.keyboard.press("ArrowRight");
  await expect.poll(() => history.evaluate(element => element.scrollLeft)).toBeGreaterThan(0);
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
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
  const refresh = page.getByRole("button", { name: "Refresh", exact: true });

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
    await expect(
      page.getByRole("table", { name: "Request history" }).locator("tbody tr"),
    ).not.toHaveCount(0);
    await expect(page).toHaveURL(new RegExp(`/monorepo%2Fmain/request/${id}$`));
    await expect(
      page.getByRole("region", { name: "Request status" }),
    ).toBeVisible();
    await refresh.click();
    await expect(refresh).toBeEnabled();
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
    await expect(
      page.getByRole("table", { name: "Request history" }).getByText("accepted", {
        exact: true,
      }),
    ).toBeVisible();
    await expect(page.getByRole("heading", { level: 2 })).toHaveText([
      "Request status",
      "History",
      "Project results",
    ]);
    await page.screenshot({
      path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-request.png`,
      fullPage: true,
    });
  });
  await test.step("Read recorded project results and reload their cursor", async () => {
    const status = page.getByRole("region", { name: "Request status", exact: true });
    const section = page.getByRole("region", { name: "Project results", exact: true });
    const projects = section.getByRole("table", { name: "Recorded project results", exact: true });
    await expect(projects.locator("tbody tr")).toHaveCount(50);
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("Breakage", { exact: true })).toBeVisible();
    await expect(projects.getByText(/Repository|%/)).toHaveCount(0);
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-01", exact: true }),
      }).getByRole("cell", { name: "Breakage", exact: true }),
    ).toBeVisible();
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-02", exact: true }),
      }).getByRole("cell", { name: "Green", exact: true }),
    ).toBeVisible();
    await refresh.click();
    await expect(refresh).toBeEnabled();
    await expect(projects.locator("tbody tr")).toHaveCount(50);
    await section.getByRole("link", { name: "Next projects →", exact: true }).click();
    await expect(projects.locator("tbody tr")).toHaveCount(1);
    await expect(projects.getByRole("cell", { name: "project-51", exact: true })).toBeVisible();
    const cursor = new URL(page.url()).searchParams.get("projects");
    expect(cursor).toBeTruthy();
    await page.reload();
    await expect(projects.locator("tbody tr")).toHaveCount(1);
    await expect(status.getByText("Breakage", { exact: true })).toBeVisible();
    expect(new URL(page.url()).searchParams.get("projects")).toBe(cursor);
    await expect(section.getByRole("link", { name: "Next projects →", exact: true })).toHaveCount(0);
    await section.getByRole("link", { name: "First projects", exact: true }).click();
    await expect(projects.locator("tbody tr")).toHaveCount(50);
    expect(new URL(page.url()).searchParams.has("projects")).toBe(false);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });
  await test.step("Keep another request's missing results distinct from green", async () => {
    await page.goto("/web%2Fpagination/request/49");
    const section = page.getByRole("region", { name: "Project results", exact: true });
    await expect(section.getByText("No project results recorded on this page.", { exact: true })).toBeVisible();
    const projects = section.getByRole("table", { name: "Recorded project results", exact: true });
    await expect(projects.locator("tbody tr")).toHaveCount(0);
    const status = page.getByRole("region", { name: "Request status", exact: true });
    await expect(status.getByText("Repository result", { exact: true })).toBeVisible();
    await expect(status.getByText("No result recorded", { exact: true })).toBeVisible();
    await expect(page.getByText("Green", { exact: true })).toHaveCount(0);
  });
  await test.step("Follow and reload the queue cursor, then select an empty queue", async () => {
    await page.getByRole("link", { name: "← Queue requests" }).click();
    await page.getByRole("link", { name: "Older requests →" }).click();
    await expect(requests.locator("tbody tr")).toHaveCount(1);
    expect(new URL(page.url()).searchParams.has("page")).toBe(true);
    await page.reload();
    await expect(requests.locator("tbody tr")).toHaveCount(1);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await queues.getByRole("link", { name: "web/empty", exact: true }).click();
    await expect(
      page.getByText("No retained requests on this queue."),
    ).toBeVisible();
  });
  expect(errors).toEqual([]);
});
