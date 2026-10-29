import { act, cleanup, fireEvent, render, screen, waitFor, within } from "../../platform/test/rtl.js";
import { afterEach, expect, it, vi } from "vitest";
import { OpenIssuePagesProvider, OpenIssues } from "./issues.js";
import type { OpenIssueModel } from "../entity/index.js";

afterEach(cleanup);

const taskWide: OpenIssueModel = {
  taskId: "buildtest", unitId: "", firstSeenAtMs: "1000",
  requestId: "7", requestHref: "/queue/request/7",
};
const target = { ...taskWide, unitId: "//pkg:go_default_test" };

it("groups units under their task and distinguishes task-wide failures", () => {
  render(<OpenIssues initial={{ issues: [taskWide, target, { ...taskWide, taskId: "expensive" }], nextHref: null }} />);
  expect(screen.getAllByRole("heading", { level: 2 }).map(node => node.textContent)).toEqual(["buildtest", "expensive"]);
  const rows = within(screen.getByRole("table", { name: "Issues for buildtest" })).getAllByRole("row");
  expect(rows[1]).toHaveTextContent("Task-wide failure");
  expect(rows[2]).toHaveTextContent("//pkg:go_default_test");
  expect(within(rows[2]!).getByRole("link", { name: "7" })).toHaveAttribute("href", "/queue/request/7");
  expect(screen.getByRole("status")).toHaveTextContent("3 issues loaded");
});

it("keeps an empty batch with a continuation distinct from an exhausted empty list", () => {
  const { rerender } = render(<OpenIssues initial={{ issues: [], nextHref: "/queue?view=issues&page=cursor" }} />);
  expect(screen.getByText("No matching issues in this batch. More results are available.")).toBeVisible();
  expect(screen.getByRole("link", { name: "Load more" })).toHaveAttribute("href", "/queue?view=issues&page=cursor");
  expect(screen.queryByText("No tracked open issues.")).toBeNull();
  rerender(<OpenIssues initial={{ issues: [], nextHref: null }} />);
  expect(screen.getByText("No tracked open issues.")).toBeVisible();
  expect(screen.queryByRole("link", { name: "Load more" })).toBeNull();
});

it("appends pages into the same task group and deduplicates task/unit identities", async () => {
  const loadPage = vi.fn().mockResolvedValue({
    issues: [taskWide, target, { ...taskWide, taskId: "expensive" }], nextHref: null,
  });
  render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("3 issues loaded"));
  expect(screen.getAllByRole("heading", { name: "buildtest" })).toHaveLength(1);
  expect(screen.getAllByRole("table")).toHaveLength(2);
  expect(loadPage).toHaveBeenCalledWith("/next", expect.any(AbortSignal));
});

it("continues through empty batches without hiding or repeating a task group", async () => {
  const loadPage = vi.fn()
    .mockResolvedValueOnce({ issues: [], nextHref: "/last" })
    .mockResolvedValueOnce({ issues: [target], nextHref: null });
  render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [], nextHref: "/next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(loadPage).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(screen.getByRole("button", { name: "Load more" })).not.toBeDisabled());
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByText(target.unitId)).toBeVisible());
  expect(loadPage.mock.calls.map(call => call[0])).toEqual(["/next", "/last"]);
});

it("retains displayed rows on a page error and retries the same cursor", async () => {
  const loadPage = vi.fn().mockRejectedValueOnce(new Error("private upstream failure"))
    .mockResolvedValueOnce({ issues: [target], nextHref: null });
  render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("Could not load more issues"));
  expect(screen.getByText("Task-wide failure")).toBeVisible();
  expect(screen.queryByText("private upstream failure")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Retry loading issues" }));
  await waitFor(() => expect(screen.getByText(target.unitId)).toBeVisible());
  expect(loadPage.mock.calls.map(call => call[0])).toEqual(["/next", "/next"]);
  expect(screen.queryByRole("alert")).toBeNull();
});

it("replaces all loaded pages on a successful refresh and rejects a late old response", async () => {
  let resolve!: (value: { issues: OpenIssueModel[]; nextHref: null }) => void;
  const loadPage = vi.fn().mockImplementation(() => new Promise(done => {
    resolve = done;
  }));
  const { rerender } = render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/old-page" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  expect(screen.getByRole("status")).toHaveTextContent("Loading more issues");
  const signal = loadPage.mock.calls[0]![1] as AbortSignal;
  rerender(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [{ ...target, taskId: "new-task" }], nextHref: null }} />
  </OpenIssuePagesProvider>);
  expect(signal.aborted).toBe(true);
  await act(async () => resolve({ issues: [taskWide], nextHref: null }));
  expect(screen.getByRole("heading", { name: "new-task" })).toBeVisible();
  expect(screen.queryByRole("heading", { name: "buildtest" })).toBeNull();
  expect(screen.getByRole("status")).toHaveTextContent("1 issue loaded");
});

it("keeps loaded pages until the refreshed empty result is explicitly selected", async () => {
  const loadPage = vi.fn().mockResolvedValue({ issues: [target], nextHref: null });
  const { rerender } = render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("2 issues loaded"));
  rerender(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [], nextHref: null }} />
  </OpenIssuePagesProvider>);
  expect(screen.getByRole("heading", { name: "buildtest" })).toBeVisible();
  expect(screen.getByRole("status")).toHaveTextContent("2 issues loaded");
  expect(screen.queryByText("No tracked open issues.")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Show latest issues" }));
  expect(screen.getByText("No tracked open issues.")).toBeVisible();
  expect(screen.queryByRole("heading", { name: "buildtest" })).toBeNull();
  expect(screen.getByRole("status")).toHaveTextContent("0 issues loaded");
  expect(screen.queryByRole("button", { name: "Show latest issues" })).toBeNull();
});

it("selects the newest refreshed result and restarts its continuation", async () => {
  const latest = { ...target, taskId: "latest-task" };
  const loadPage = vi.fn()
    .mockResolvedValueOnce({ issues: [target], nextHref: null })
    .mockResolvedValueOnce({ issues: [{ ...latest, unitId: "//pkg:another_test" }], nextHref: null });
  const { rerender } = render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/old-next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("2 issues loaded"));
  rerender(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [{ ...target, taskId: "intermediate-task" }], nextHref: "/intermediate-next" }} />
  </OpenIssuePagesProvider>);
  rerender(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [latest], nextHref: "/latest-next" }} />
  </OpenIssuePagesProvider>);
  expect(screen.getByRole("heading", { name: "buildtest" })).toBeVisible();
  expect(screen.queryByRole("heading", { name: "latest-task" })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Show latest issues" }));
  expect(screen.getByRole("heading", { name: "latest-task" })).toBeVisible();
  expect(screen.queryByRole("heading", { name: "buildtest" })).toBeNull();
  expect(screen.queryByRole("heading", { name: "intermediate-task" })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("2 issues loaded"));
  expect(loadPage.mock.calls.map(call => call[0])).toEqual(["/old-next", "/latest-next"]);
});

it.each([false, true])("handles a pending continuation when a refresh is offered, apply=%s", async apply => {
  let finish!: (page: { issues: OpenIssueModel[]; nextHref: null }) => void;
  const loadPage = vi.fn()
    .mockResolvedValueOnce({ issues: [target], nextHref: "/last" })
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
  const { rerender } = render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("2 issues loaded"));
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  const signal = loadPage.mock.calls[1]![1] as AbortSignal;
  rerender(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [{ ...target, taskId: "fresh-task" }], nextHref: null }} />
  </OpenIssuePagesProvider>);
  expect(signal.aborted).toBe(false);
  expect(screen.getByRole("status")).toHaveTextContent("Loading more issues");
  if (apply) fireEvent.click(screen.getByRole("button", { name: "Show latest issues" }));
  await act(async () => finish({ issues: [{ ...taskWide, taskId: "late-task" }], nextHref: null }));
  if (apply) {
    expect(signal.aborted).toBe(true);
    expect(screen.getByRole("heading", { name: "fresh-task" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "late-task" })).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("1 issue loaded");
  } else {
    expect(screen.getByRole("heading", { name: "late-task" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "fresh-task" })).toBeNull();
    expect(screen.getByRole("status")).toHaveTextContent("3 issues loaded");
    expect(screen.getByRole("button", { name: "Show latest issues" })).toBeVisible();
  }
});

it("does not loop when a continuation repeats", async () => {
  const loadPage = vi.fn().mockResolvedValue({ issues: [target], nextHref: "/next" });
  render(<OpenIssuePagesProvider loadPage={loadPage}>
    <OpenIssues initial={{ issues: [taskWide], nextHref: "/next" }} />
  </OpenIssuePagesProvider>);
  fireEvent.click(screen.getByRole("button", { name: "Load more" }));
  await waitFor(() => expect(screen.getByRole("alert")).toBeVisible());
  expect(screen.getByRole("status")).toHaveTextContent("1 issue loaded");
  expect(loadPage).toHaveBeenCalledOnce();
});
