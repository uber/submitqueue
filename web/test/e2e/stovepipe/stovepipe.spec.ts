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
test("real ingestion, queue selection, cursors, request summaries, history and refresh", async ({
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
    await expect(status.getByText("succeeded", { exact: true })).toBeVisible();
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
    ]);
    await page.screenshot({
      path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-request.png`,
      fullPage: true,
    });
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
