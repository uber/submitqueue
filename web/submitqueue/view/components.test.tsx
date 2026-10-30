import { act, fireEvent, render, screen, within } from "../../platform/test/rtl.js";
import { describe, expect, it, vi } from "vitest";
import { WebNavigationProvider } from "@submitqueue/web-platform";
import { WebApp } from "./web-app.js";
import { ChangeSubmissions, QueueDirectory, QueueRequests, RequestDetail, RequestList, RequestStatus } from "./components.js";
import type { ChangeDetailModel, RequestDetailModel } from "../entity/index.js";

const request = {
  sqid: "demo-queue/1",
  queue: "demo-queue",
  changeUris: ["github://github.com/uber/submitqueue/pull/1/abc"],
  receivedAtMs: 1_700_000_000_000,
  updatedAtMs: 1_700_000_065_000,
  status: "speculating",
  lastError: null,
  metadata: { batch: "batch-1" },
};

describe("request components", () => {
  it("renders a linked queue request with its complete sqid", () => {
    render(<RequestList queue="demo-queue" requests={[request]} />);

    const link = screen.getByRole("link", { name: "demo-queue/1" });
    expect(link.getAttribute("href")).toBe(
      "/queue/demo-queue/request/demo-queue/1",
    );
    expect(screen.getByText("Speculating").closest("[data-tone]")?.getAttribute("data-tone")).toBe("progress");
    expect(screen.getByText("Nov 14, 2023, 10:13:20 PM UTC")).toBeTruthy();
    expect(screen.getByText("Nov 14, 2023, 10:14:25 PM UTC")).toBeTruthy();
  });

  it("shows an unreported update time as a dash", () => {
    render(<RequestList queue="demo-queue" requests={[{ ...request, updatedAtMs: null }]} />);
    const row = screen.getAllByRole("row")[1]!;
    expect(within(row).getAllByRole("cell").at(-1)?.textContent).toBe("—");
  });

  it.each([
    { name: "an empty queue", filtered: false, complete: true, message: "No requests have been received on this queue." },
    { name: "no matches for a filter", filtered: true, complete: true, message: "No requests on this queue have the selected statuses." },
    { name: "a scan still in progress", filtered: true, complete: false, message: null },
  ])("describes $name accurately", ({ filtered, complete, message }) => {
    const { container } = render(<RequestList queue="demo-queue" requests={[]} filtered={filtered} complete={complete} />);
    expect(container.textContent).toBe(message ?? "");
  });

  it("renders ordered status/build history with expandable metadata", () => {
    const model: RequestDetailModel = {
      request: { ...request, status: "landed" },
      history: [
        {
          timestampMs: 1_700_000_000_000,
          type: "status",
          status: "started",
          event: null,
          lastError: null,
          metadata: {},
        },
        {
          timestampMs: 1_700_000_001_000,
          type: "event",
          status: null,
          event: "building",
          lastError: null,
          metadata: { build_url: "https://build.example/1" },
        },
      ],
      historyError: null,
    };
    render(<RequestDetail model={model} view="history" />);

    expect(screen.getByRole("heading", { level: 1 }).textContent).toContain("demo-queue/1");
    const history = screen.getByRole("list", { name: "Request history" });
    expect(within(history).getAllByRole("listitem")).toHaveLength(2);
    expect(within(history).getByText("Building")).toBeTruthy();
    fireEvent.click(within(history).getByText("Building"));
    expect(within(history).getByText("https://build.example/1")).toBeTruthy();
    fireEvent.click(screen.getByRole("combobox"));
    fireEvent.click(screen.getByRole("option", { name: "Occurrence events" }));
    expect(within(history).getAllByRole("listitem")).toHaveLength(1);
  });

  it("lists configured queues even when there is only one", () => {
    render(<QueueDirectory queues={[{ name: "demo-queue", description: "Demo gateway" }]} />);
    expect(screen.getByRole("link", { name: "demo-queue" }).getAttribute("href")).toBe("/queue/demo-queue");
  });

  it("links change URIs to their change page by canonical label without exposing fake file hints", () => {
    const sha = "0123456789abcdef0123456789abcdef01234567";
    const raw = `git://git.example.com/demo/refs%2Fheads%2Fmain/${sha}?sq-files=demo%2Ffile.txt`;
    const clean = raw.split("?")[0]!;
    render(<RequestList queue="demo-queue" requests={[{ ...request, changeUris: [raw] }]} basePath="/sq" />);
    expect(screen.getByRole("link", { name: clean }).getAttribute("href"))
      .toBe("/sq/queue/demo-queue/change/git/git.example.com/demo/refs%2Fheads%2Fmain");
    expect(screen.queryByText(/sq-files=/)).toBeNull();
  });

  it("retains the last successful list on a transient failure, but not a permanent failure", () => {
    const view = render(<QueueRequests queue="demo-queue" routeKey="demo-queue:live"
      result={{ ok: true, data: { requests: [request], nextHref: null } }} />);
    const error = { kind: "transient" as const, title: "Unavailable", message: "Retry", retryable: true };
    view.rerender(<QueueRequests queue="demo-queue" routeKey="demo-queue:live" result={{ ok: false, error }} />);
    expect(screen.getByRole("link", { name: request.sqid })).toBeTruthy();
    expect(screen.getByText(/last successful snapshot/)).toBeTruthy();
    view.rerender(<QueueRequests queue="demo-queue" routeKey="demo-queue:live" result={{ ok: false, error: { ...error, retryable: false } }} />);
    expect(screen.queryByRole("link", { name: request.sqid })).toBeNull();
  });

  it("does not describe unavailable history as empty and copies the opaque ID", async () => {
    const writeText = vi.fn(async () => undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    render(<RequestDetail model={{
      request, history: [], historyError: {
        kind: "transient", title: "Unavailable", message: "Try again", retryable: true,
      },
    }} />);
    expect(screen.getByText("History unavailable")).toBeTruthy();
    expect(screen.queryByText("No history has been retained for this request.")).toBeNull();
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Copy ID" })));
    await vi.waitFor(() => expect(writeText).toHaveBeenCalledWith(request.sqid));
  });

  it("keeps repeated submissions for the same version separate", () => {
    render(<ChangeSubmissions model={{
      queue: "demo-queue", provider: "GitHub", host: "github.com",
      repository: "uber/submitqueue", review: "PR #123", pinnedVersion: null,
      window: { fromMs: 1, toMs: 2 },
      submissions: ["42", "38"].map(sqid => ({
        request: { ...request, sqid }, version: "same-sha", versionHref: "/version",
      })),
    }} />);
    const table = screen.getByRole("table", { name: "Change submissions" });
    expect(within(table).getAllByText("same-sha")).toHaveLength(2);
    expect(within(table).getByRole("link", { name: "38" }).getAttribute("href")).toBe("/queue/demo-queue/request/38");
  });

  it("navigates between versions through the enclosing app and keeps full versions in links", () => {
    const push = vi.fn();
    const version = "0123456789abcdef0123456789abcdef01234567";
    const change: ChangeDetailModel = {
      queue: "demo-queue", provider: "GitHub", host: "github.com",
      repository: "uber/submitqueue", review: "PR #123", pinnedVersion: null,
      logicalHref: "/logical", window: { fromMs: 1, toMs: 2 },
      submissions: [{ request, version, versionHref: `/logical/${version}` }],
    };
    const page = { key: "change", title: "PR #123", refresh: { terminal: true, transientFailureCount: 0, refreshHref: null } };
    render(<WebNavigationProvider value={{ push }}>
      <WebApp model={page}><ChangeSubmissions model={change} /></WebApp>
    </WebNavigationProvider>);
    fireEvent.click(screen.getByRole("combobox"));
    fireEvent.click(screen.getByRole("option", { name: version }));
    expect(push).toHaveBeenCalledWith(`/logical/${version}`);
    expect(screen.getByRole("link", { name: version }).getAttribute("href")).toBe(`/logical/${version}`);
  });

  it.each([
    { nextHref: "/change?page=next", latestHref: null },
    { nextHref: null, latestHref: "/change" },
  ])("renders page-scoped history and host-provided navigation: %j", pagination => {
    render(<ChangeSubmissions model={{
      queue: "demo-queue", provider: "Git", host: "git.example.com",
      repository: "demo", review: "refs/heads/main", pinnedVersion: null,
      window: { fromMs: 1, toMs: 2 }, submissions: [], pagination,
    }} />);
    expect(screen.getByText("No submissions were found on this page.")).toBeTruthy();
    expect(screen.queryByText("No submissions were found in this scope.")).toBeNull();
    expect(screen.getByRole("status").textContent).toContain("Showing matches from this page");
    if (pagination.nextHref) {
      expect(screen.getByRole("link", { name: "Continue history scan" }).getAttribute("href")).toBe(pagination.nextHref);
      expect(screen.getByRole("status").textContent).toContain("More queue requests remain");
    } else {
      expect(screen.queryByRole("link", { name: "Continue history scan" })).toBeNull();
      expect(screen.getByRole("link", { name: "Latest history" }).getAttribute("href")).toBe(pagination.latestHref);
    }
  });

  it("shows unknown statuses safely", () => {
    render(<RequestStatus status="new_pipeline_step" />);
    expect(screen.getByText("New Pipeline Step").closest("[data-tone]")?.getAttribute("data-tone")).toBe("neutral");
  });
});
