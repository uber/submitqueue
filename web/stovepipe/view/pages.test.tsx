import { cleanup, render, screen, within } from "../../platform/test/rtl.js";
import { afterEach, expect, it } from "vitest";
import type { RequestSummaryModel } from "../entity/index.js";
import { RequestDetails, RequestList } from "./pages.js";
import { QueueRequests } from "./queue.js";

afterEach(cleanup);

const request: RequestSummaryModel = {
  id: "7",
  href: "/queue/request/7",
  changeUri: "git://repo/change",
  baseUri: "",
  state: "failed",
  acceptedAtMs: "1000",
  updatedAtMs: "2000",
  outcomeReason: "build_failed",
  projectCount: 0,
  brokenProjectCount: 0,
};

it.each([
  { name: "no recorded projects", total: 0, broken: 0, display: "—" },
  { name: "all green", total: 51, broken: 0, display: "0 of 51" },
  { name: "mixed outcomes", total: 51, broken: 2, display: "2 of 51" },
  { name: "all broken", total: 51, broken: 51, display: "51 of 51" },
])("shows $name in the queue and request summary", ({ total, broken, display }) => {
  const summary = { ...request, projectCount: total, brokenProjectCount: broken };
  render(
    <>
      <RequestList requests={[summary]} />
      <section aria-label="Summary"><RequestDetails request={summary} repositoryResult={null} /></section>
    </>,
  );
  const table = within(screen.getByRole("table", { name: "Requests" }));
  expect(table.getByRole("columnheader", { name: "Broken projects" })).toBeVisible();
  expect(table.getByRole("cell", { name: display })).toBeVisible();
  const details = within(screen.getByRole("region", { name: "Summary" }));
  expect(details.getByText("Broken projects")).toBeVisible();
  expect(details.getByText(display, { exact: true })).toBeVisible();
});

it("refreshes counts on an already displayed queue row", () => {
  const { rerender } = render(
    <QueueRequests routeKey="queue" initial={{ requests: [request], nextHref: null }} />,
  );
  const table = within(screen.getByRole("table", { name: "Requests" }));
  expect(table.getByRole("cell", { name: "—" })).toBeVisible();
  rerender(
    <QueueRequests routeKey="queue" initial={{
      requests: [{ ...request, projectCount: 3, brokenProjectCount: 1 }],
      nextHref: null,
    }} />,
  );
  expect(table.getByRole("cell", { name: "1 of 3" })).toBeVisible();
});
