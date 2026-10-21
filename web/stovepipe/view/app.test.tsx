import { cleanup, render, screen, waitFor, within } from "../../platform/test/rtl.js";
import { afterEach, expect, it } from "vitest";
import { StovepipeApp } from "./app.js";
import type { StovepipePageModel } from "../entity/index.js";
afterEach(cleanup);
const model: StovepipePageModel = {
  kind: "request",
  key: "/repo%2Fmain/request/7?",
  queue: "repo/main",
  requestId: "7",
  queues: [{ name: "repo/main", href: "/repo%2Fmain" }],
  queueHref: "/repo%2Fmain",
  latestHref: "/repo%2Fmain",
  showLatestLink: false,
  loadedAtMs: 1000,
  projectStatus: {
    ok: true,
    data: {
      updatedAtMs: "1000",
      repositoryBreakageDegree: null,
      complete: false,
      projects: [
        { project: "green-project", breakageDegree: 0 },
        { project: "broken-project", breakageDegree: 0.25 },
        { project: "unknown-project", breakageDegree: null },
      ],
      firstHref: null,
      nextHref: "/repo%2Fmain/request/7?projects=opaque%2B%2F%3D",
    },
  },
  history: {
    ok: true,
    data: {
      request: {
        id: "7",
        href: "/repo%2Fmain/request/7",
        changeUri: "git://repo/sha",
        baseUri: "git://repo/base",
        state: "completed",
        updatedAtMs: "1000",
        acceptedAtMs: "100",
        outcomeReason: "all_targets_passed",
      },
      events: [
        {
          id: "e1",
          timestampMs: "100",
          kind: "requestState",
          label: "accepted",
          outcomeReason: "",
        },
      ],
    },
  },
};
it("displays the backend summary above the retained history", () => {
  render(<StovepipeApp model={model} />);
  const status = screen.getByRole("region", { name: "Request status" });
  expect(within(status).getByText("completed")).toBeVisible();
  expect(within(status).getByText("git://repo/sha")).toBeVisible();
  expect(within(status).getByText("git://repo/base")).toBeVisible();
  expect(within(status).getByText("all_targets_passed")).toBeVisible();
  expect(within(status).getByText("Accepted")).toBeVisible();
  expect(within(status).getByText("Updated")).toBeVisible();
  const history = screen.getByRole("region", { name: "History" });
  expect(within(history).getByText("accepted")).toBeVisible();
  expect(
    screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent),
  ).toEqual(["Request status", "History", "Project results"]);
});
it("links queue entries by request ID even when the change URI is missing", () => {
  if (!model.history.ok) throw new Error("No summary");
  render(
    <StovepipeApp model={{
      ...model,
      kind: "queue",
      requests: {
        ok: true,
        data: {
          requests: [{ ...model.history.data.request, changeUri: "" }],
          nextHref: null,
        },
      },
    }} />,
  );
  expect(screen.getByRole("link", { name: "7" })).toHaveAttribute(
    "href",
    "/repo%2Fmain/request/7",
  );
  expect(screen.getByText("Unknown URI")).toBeVisible();
});
it("retains the summary and history after a failed refresh", async () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(
    <StovepipeApp model={{
      ...model,
      history: { ok: false, error: "History unavailable" },
      projectStatus: { ok: false, error: "Projects unavailable" },
    }} />,
  );
  expect(screen.getByText("git://repo/sha")).toBeVisible();
  expect(screen.getByText("all_targets_passed")).toBeVisible();
  expect(screen.getByText("accepted")).toBeVisible();
  await waitFor(() => expect(screen.getAllByText("Showing the last successful result.")).toHaveLength(2));
  expect(screen.getByText("green-project")).toBeVisible();
});
it("discards previous results when the request changes", () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(
    <StovepipeApp model={{
      ...model,
      key: "/repo%2Fmain/request/8?",
      requestId: "8",
      history: { ok: false, error: "History unavailable" },
      projectStatus: { ok: false, error: "Projects unavailable" },
    }} />,
  );
  expect(screen.getByRole("heading", { name: "Request 8" })).toBeVisible();
  expect(screen.queryByText("git://repo/sha")).toBeNull();
  expect(screen.queryByText("all_targets_passed")).toBeNull();
  expect(screen.queryByText("green-project")).toBeNull();
});

it("renders recorded project results, completion and shareable pagination", () => {
  render(<StovepipeApp model={model} />);
  const projects = screen.getByRole("region", { name: "Project results" });
  const table = within(projects).getByRole("table", { name: "Recorded project results" });
  expect(within(table).getByRole("rowheader", { name: "Repository" })).toBeVisible();
  expect(within(projects).getByText("Green (0%)")).toBeVisible();
  expect(within(projects).getByText("Breakage (25%)")).toBeVisible();
  expect(within(projects).getAllByText("No result recorded")).toHaveLength(2);
  expect(within(projects).getByText(/still in progress/)).toBeVisible();
  expect(within(projects).getByRole("link", { name: /Next projects/ })).toHaveAttribute(
    "href", "/repo%2Fmain/request/7?projects=opaque%2B%2F%3D",
  );
  const timestamp = within(projects).getByText(/Results updated:/);
  const pagination = within(projects).getByRole("navigation", { name: "Project pages" });
  expect(table.compareDocumentPosition(timestamp) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(pagination.compareDocumentPosition(timestamp) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
});

it("does not show a previous project's page after a failed pagination load", () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(<StovepipeApp model={{
    ...model, key: "/repo%2Fmain/request/7?projects=next",
    projectStatus: { ok: false, error: "Projects unavailable" },
  }} />);
  expect(screen.queryByText("green-project")).toBeNull();
  expect(screen.getByText("git://repo/sha")).toBeVisible();
});

it("shows complete empty results without inventing green or a continuation", () => {
  if (!model.projectStatus.ok) throw new Error("No projects");
  render(<StovepipeApp model={{
    ...model,
    projectStatus: { ok: true, data: {
      ...model.projectStatus.data, complete: true, projects: [], nextHref: null,
    } },
  }} />);
  expect(screen.getByText("Project result recording is complete.")).toBeVisible();
  expect(screen.getByText("No project results recorded on this page.")).toBeVisible();
  expect(screen.queryByRole("link", { name: /Next projects/ })).toBeNull();
  const table = screen.getByRole("table", { name: "Recorded project results" });
  expect(within(table).getByRole("rowheader", { name: "Repository" })).toBeVisible();
  expect(within(table).getByText("No result recorded")).toBeVisible();
});
