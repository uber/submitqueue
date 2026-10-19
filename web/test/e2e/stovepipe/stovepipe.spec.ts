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
test("real ingestion, queue selection, cursors, request summaries, history, project results and refresh", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
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
    const section = page.getByRole("region", { name: "Project results", exact: true });
    const projects = section.getByRole("table", { name: "Project results", exact: true });
    await expect(projects.locator("tbody tr")).toHaveCount(51);
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("rowheader", { name: "Repository", exact: true }),
      }),
    ).toContainText("Breakage (25%)");
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-01", exact: true }),
      }),
    ).toContainText("Breakage (25%)");
    await expect(
      projects.getByRole("row").filter({
        has: page.getByRole("cell", { name: "project-02", exact: true }),
      }),
    ).toContainText("Green (0%)");
    await refresh.click();
    await expect(refresh).toBeEnabled();
    await expect(projects.locator("tbody tr")).toHaveCount(51);
    await section.getByRole("link", { name: "Next projects →", exact: true }).click();
    await expect(projects.locator("tbody tr")).toHaveCount(2);
    await expect(projects.getByRole("cell", { name: "project-51", exact: true })).toBeVisible();
    const cursor = new URL(page.url()).searchParams.get("projects");
    expect(cursor).toBeTruthy();
    await page.reload();
    await expect(projects.locator("tbody tr")).toHaveCount(2);
    expect(new URL(page.url()).searchParams.get("projects")).toBe(cursor);
    await expect(section.getByRole("link", { name: "Next projects →", exact: true })).toHaveCount(0);
    await section.getByRole("link", { name: "First projects", exact: true }).click();
    await expect(projects.locator("tbody tr")).toHaveCount(51);
    expect(new URL(page.url()).searchParams.has("projects")).toBe(false);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });
  await test.step("Keep another request's missing results distinct from green", async () => {
    await page.goto("/web%2Fpagination/request/49");
    const section = page.getByRole("region", { name: "Project results", exact: true });
    await expect(section.getByText("No project results recorded on this page.", { exact: true })).toBeVisible();
    const projects = section.getByRole("table", { name: "Project results", exact: true });
    await expect(projects.locator("tbody tr")).toHaveCount(1);
    await expect(projects.getByRole("rowheader", { name: "Repository", exact: true })).toBeVisible();
    await expect(projects.getByRole("cell", { name: "No result recorded", exact: true })).toBeVisible();
    await expect(section.getByText("Green (0%)", { exact: true })).toHaveCount(0);
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
