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
      pageSize: 50,
      pageSizeChoices: [25, 50, 100, 200].map(value => ({
        value, href: "/repo%2Fmain/request/7?projectPageSize=" + value,
      })),
      projects: [
        { project: "green-project", breakageDegree: 0 },
        { project: "partial-breakage-project", breakageDegree: 0.25 },
        { project: "broken-project", breakageDegree: 1 },
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
  expect(within(status).getByText("Repository result")).toBeVisible();
  expect(within(status).getByText("No result recorded")).toBeVisible();
  const history = screen.getByText("accepted").closest("section")!;
  expect(history).not.toBeVisible();
  expect(status.compareDocumentPosition(history) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(
    screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent),
  ).toEqual(["Project results"]);
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
  expect(screen.getByText("accepted")).toBeInTheDocument();
  await waitFor(() => expect(screen.getAllByText("Showing the last successful result.")).toHaveLength(2));
  expect(screen.getByText("green-project")).toBeVisible();
  expect(within(screen.getByRole("region", { name: "Request status" }))
    .getByText("No result recorded")).toBeVisible();
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
  expect(within(table).queryByText(/Repository/)).toBeNull();
  expect(within(table).getByRole("columnheader", { name: "Project" })).toBeVisible();
  expect(within(projects).getByText("Green")).toBeVisible();
  expect(within(table).getByRole("row", { name: "partial-breakage-project Partial Breakage" })).toBeVisible();
  expect(within(projects).getByText("Breakage")).toBeVisible();
  expect(within(projects).getByText("No result recorded")).toBeVisible();
  expect(within(projects).queryByText(/%/)).toBeNull();
  expect(within(projects).getByText(/still in progress/)).toBeVisible();
  for (const link of within(projects).getAllByRole("link", { name: /Next projects/ })) {
    expect(link).toHaveAttribute("href", "/repo%2Fmain/request/7?projects=opaque%2B%2F%3D");
  }
  const timestamp = within(projects).getByText(/Results updated:/);
  const pagination = within(projects).getByRole("navigation", { name: "Project pages (bottom)" });
  const topPagination = within(projects).getByRole("navigation", { name: "Project pages (top)" });
  expect(topPagination.compareDocumentPosition(table) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(table.compareDocumentPosition(timestamp) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(pagination.compareDocumentPosition(timestamp) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
});

it.each(["projects=next", "projectPageSize=25"])("preserves the request summary and pager focus when loading %s", (query) => {
  if (!model.projectStatus.ok) throw new Error("No projects");
  const { rerender } = render(<StovepipeApp model={model} />);
  const next = within(screen.getByRole("navigation", { name: "Project pages (top)" }))
    .getByRole("link", { name: /Next projects/ });
  next.focus();
  rerender(<StovepipeApp model={{
    ...model,
    key: "/repo%2Fmain/request/7?" + query,
    projectStatus: { ok: true, data: {
      ...model.projectStatus.data,
      projects: [{ project: "next-project", breakageDegree: 0 }],
    } },
  }} />);
  expect(screen.getByText("git://repo/sha")).toBeVisible();
  expect(next).toHaveFocus();
  expect(screen.queryByText("green-project")).toBeNull();
  expect(screen.getByText("next-project")).toBeVisible();
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
  expect(within(table).getAllByRole("row")).toHaveLength(1);
  expect(within(screen.getByRole("region", { name: "Request status" }))
    .getByText("No result recorded")).toBeVisible();
});

it.each([
  [0, "Green"],
  [0.25, "Partial Breakage"],
  [1, "Breakage"],
  [null, "No result recorded"],
] as const)("shows repository result %s above history as %s", (degree, label) => {
  if (!model.projectStatus.ok) throw new Error("No projects");
  render(<StovepipeApp model={{
    ...model,
    projectStatus: { ok: true, data: {
      ...model.projectStatus.data, repositoryBreakageDegree: degree,
    } },
  }} />);
  const status = screen.getByRole("region", { name: "Request status" });
  expect(within(status).getByText(label)).toBeVisible();
  expect(within(status).queryByText(/%/)).toBeNull();
});

it("shows repository and project results when history is unavailable", async () => {
  render(<StovepipeApp model={{
    ...model, history: { ok: false, error: "History unavailable" },
  }} />);
  expect(await screen.findByRole("alert")).toHaveTextContent("History unavailable");
  expect(within(screen.getByRole("region", { name: "Request status" }))
    .getByText("No result recorded")).toBeVisible();
  expect(screen.getByText("green-project")).toBeVisible();
});

it("shows history when repository and project results are unavailable", async () => {
  render(<StovepipeApp model={{
    ...model, key: "/repo%2Fmain/request/7?view=history", projectStatus: { ok: false, error: "Projects unavailable" },
  }} />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Projects unavailable");
  expect(screen.getByText("accepted")).toBeInTheDocument();
  expect(screen.getByText("git://repo/sha")).toBeVisible();
  expect(screen.queryByText("Repository result")).toBeNull();
});


it.each([
  ["projects", "Project results"],
  ["history", "History"],
  ["issues", "Build and Test Issues"],
  ["fix-attempts", "Fix Attempts"],
] as const)("keeps project paging when selecting the %s view", (view, label) => {
  const query = new URLSearchParams({ projects: "opaque+/=", projectPageSize: "25", filter: "keep" });
  if (view !== "projects") query.set("view", view);
  render(<StovepipeApp model={{ ...model, key: `/repo%2Fmain/request/7?${query}` }} />);
  const navigation = screen.getByRole("navigation", { name: "Request views" });
  const projectsHref = "/repo%2Fmain/request/7?projects=opaque%2B%2F%3D&projectPageSize=25&filter=keep";
  expect(within(navigation).getByRole("link", { name: "Project results" })).toHaveAttribute("href", projectsHref);
  expect(within(navigation).getByRole("link", { name: "History" })).toHaveAttribute("href", projectsHref + "&view=history");
  expect(within(navigation).getByRole("link", { name: "Build and Test Issues" })).toHaveAttribute("href", projectsHref + "&view=issues");
  expect(within(navigation).getByRole("link", { name: "Fix Attempts" })).toHaveAttribute("href", projectsHref + "&view=fix-attempts");
  expect(within(navigation).getByRole("link", { name: label })).toHaveAttribute("aria-current", "page");
  const section = screen.getByRole("region", { name: label });
  expect(section).toBeVisible();
  expect(screen.getAllByRole("heading", { level: 2 })).toHaveLength(1);
  expect(screen.getByText("git://repo/sha")).toBeVisible();
  if (view === "issues" || view === "fix-attempts") {
    expect(within(section).getByText("Coming soon")).toBeVisible();
    expect(screen.queryByRole("table")).toBeNull();
  }
});
