import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ErrorState, RequestStatus, QueueDirectory, RequestList, RequestListView, RequestDetail } from "./components";
import type { RequestDetailModel, RequestListModel } from "./models";

const request = {
  sqid: "demo-queue/1",
  queue: "demo-queue",
  changeUris: ["github://github.com/uber/submitqueue/pull/1/abc"],
  receivedAtMs: 1_700_000_000_000,
  status: "speculating",
  lastError: null,
  metadata: { batch: "batch-1" },
};

describe("request components", () => {

  it("renders a linked queue request with its complete sqid", () => {
    const model: RequestListModel = {
      queue: "demo-queue",
      receivedAtOrAfterMs: 1,
      receivedBeforeMs: 2,
      requests: [request],
      nextPageToken: "opaque-token",
    };
    render(<RequestList model={model} />);

    const link = screen.getByRole("link", { name: "demo-queue/1" });
    expect(link.getAttribute("href")).toBe(
      "/demo-queue/request/demo-queue/1",
    );
    expect(screen.getByText("Speculating").getAttribute("data-tone")).toBe("progress");
    expect(screen.getByText("Nov 14, 2023, 10:13:20 PM UTC")).toBeTruthy();
    expect(screen.getByRole("link", { name: "Next page" }).getAttribute("href")).toBe(
      "/demo-queue?page=opaque-token",
    );
  });

  it("renders an accessible empty state", () => {
    render(
      <RequestList
        model={{
          queue: "demo-queue",
          receivedAtOrAfterMs: 1,
          receivedBeforeMs: 2,
          requests: [],
          nextPageToken: null,
        }}
      />,
    );
    expect(screen.getByText("No requests were received in this window.")).toBeTruthy();
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
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "event" } });
    expect(within(history).getAllByRole("listitem")).toHaveLength(1);
  });

  it("lists configured queues even when there is only one", () => {
    render(<QueueDirectory queues={[{ name: "demo-queue", description: "Demo gateway" }]} />);
    expect(screen.getByRole("link", { name: "demo-queue" }).getAttribute("href")).toBe("/demo-queue");
  });

  it("filters only the displayed page without discarding its pagination link", () => {
    render(<RequestList model={{
      queue: "demo-queue", receivedAtOrAfterMs: 1, receivedBeforeMs: 2,
      requests: [request, { ...request, sqid: "42", status: "landed" }],
      nextPageToken: "next",
    }} />);
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "landed" } });
    expect(screen.queryByRole("link", { name: request.sqid })).toBeNull();
    expect(screen.getByRole("link", { name: "42" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "Next page" })).toBeTruthy();
  });

  it("uses host-supplied change labels and links without exposing fake file hints", () => {
    const raw = "git://git.example.com/demo/refs%2Fheads%2Fmain/sha?sq-files=demo%2Ffile.txt";
    const clean = raw.split("?")[0]!;
    render(<RequestList model={{
      queue: "demo-queue", receivedAtOrAfterMs: 1, receivedBeforeMs: 2,
      requests: [{ ...request, changeUris: [raw] }], nextPageToken: null,
    }} changeLabels={{ [raw]: clean }} changeLinks={{ [raw]: "/change" }} />);
    expect(screen.getByRole("link", { name: clean }).getAttribute("href")).toBe("/change");
    expect(screen.queryByText(/sq-files=/)).toBeNull();
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "file.txt" } });
    expect(screen.getByText("No displayed requests match.")).toBeTruthy();
  });

  it("retains the last successful list on a transient failure, but not a permanent failure", () => {
    const view = render(<RequestListView result={{ ok: true, data: {
      queue: "demo-queue", receivedAtOrAfterMs: 1, receivedBeforeMs: 2,
      requests: [request], nextPageToken: null,
    } }} />);
    const error = { kind: "transient" as const, title: "Unavailable", message: "Retry", retryable: true };
    view.rerender(<RequestListView result={{ ok: false, error }} />);
    expect(screen.getByRole("link", { name: request.sqid })).toBeTruthy();
    expect(screen.getByText(/last successful snapshot/)).toBeTruthy();
    view.rerender(<RequestListView result={{ ok: false, error: { ...error, retryable: false } }} />);
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
    fireEvent.click(screen.getByRole("button", { name: "Copy ID" }));
    await vi.waitFor(() => expect(writeText).toHaveBeenCalledWith(request.sqid));
  });

  it("shows unknown statuses safely", () => {
    render(<RequestStatus status="new_pipeline_step" />);
    expect(screen.getByText("New Pipeline Step").getAttribute("data-tone")).toBe("neutral");
  });

  it("uses an alert for a full-page safe error", () => {
    render(
      <ErrorState
        error={{
          kind: "internal",
          title: "Something went wrong",
          message: "SubmitQueue could not load this information.",
          retryable: false,
        }}
      />,
    );
    expect(screen.getByRole("alert").textContent).toContain("Something went wrong");
  });
});
