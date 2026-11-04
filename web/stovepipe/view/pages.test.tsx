import { cleanup, render, screen, within } from "../../platform/test/rtl.js";
import { afterEach, expect, it } from "vitest";
import type { HistoryEventModel, RequestSummaryModel } from "../entity/index.js";
import { History, RequestDetails, RequestList } from "./pages.js";
import { QueueRequests } from "./queue.js";

afterEach(cleanup);

it("shows labeled history metadata and preserves zeros, opaque IDs and missing values", () => {
  const events: HistoryEventModel[] = [
    { id: "triggered", timestampMs: "1000", kind: "event", label: "build_triggered", outcomeReason: "",
      details: [{ label: "Build ID", value: "runner/build/7?attempt=2" }] },
    { id: "validation", timestampMs: "2000", kind: "event", label: "validation_fact_recorded", outcomeReason: "",
      details: [{ label: "Breakage degree", value: "0" }] },
    { id: "projects", timestampMs: "3000", kind: "event", label: "project_facts_recorded", outcomeReason: "",
      details: [{ label: "Project facts", value: "0" }] },
    { id: "legacy-finished", timestampMs: "4000", kind: "event", label: "build_finished", outcomeReason: "", details: [] },
  ];
  render(<History events={events} />);
  const table = within(screen.getByRole("table", { name: "Request history" }));
  expect(table.getByRole("columnheader", { name: "Details" })).toBeVisible();
  for (const text of ["Build ID: runner/build/7?attempt=2", "Breakage degree: 0", "Project facts: 0"])
    expect(table.getByRole("cell", { name: text })).toBeVisible();
  const lastRow = within(table.getAllByRole("row").at(-1)!);
  expect(lastRow.getAllByRole("cell").at(-1)).toHaveTextContent("—");
});

it("links resolved build IDs and leaves other values as text", () => {
  render(<History events={[{
    id: "build", timestampMs: "1000", kind: "event", label: "build_finished", outcomeReason: "",
    details: [
      { label: "Build ID", value: "runner/build/7?attempt=2", href: "https://builds.example.test/pipeline/builds/7?attempt=2" },
    ],
  }, {
    id: "unresolved", timestampMs: "2000", kind: "event", label: "build_triggered", outcomeReason: "",
    details: [{ label: "Build ID", value: "runner/build/8" }],
  }, {
    id: "validation", timestampMs: "3000", kind: "event", label: "validation_fact_recorded", outcomeReason: "",
    details: [{ label: "Breakage degree", value: "0.5" }],
  }]} />);
  expect(screen.getByRole("link", { name: "runner/build/7?attempt=2" })).toHaveAttribute("href", "https://builds.example.test/pipeline/builds/7?attempt=2");
  expect(screen.queryByRole("link", { name: "runner/build/8" })).toBeNull();
  expect(screen.getByText("runner/build/8")).toBeVisible();
  expect(screen.getByRole("cell", { name: "Breakage degree: 0.5" })).toBeVisible();
  expect(screen.getAllByRole("link")).toHaveLength(1);
});

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
