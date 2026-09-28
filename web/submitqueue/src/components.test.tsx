import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ErrorState, RequestDetail, RequestList, RequestStatus } from "./components";
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
  it("renders a linked queue request with an encoded sqid", () => {
    const model: RequestListModel = {
      queue: "demo-queue",
      receivedAtOrAfterMs: 1,
      receivedBeforeMs: 2,
      requests: [request],
      nextPageToken: "opaque-token",
    };
    render(<RequestList model={model} />);

    const link = screen.getByRole("link", { name: "demo-queue/1" });
    expect(link.getAttribute("href")).toBe("/requests/ZGVtby1xdWV1ZS8x");
    expect(screen.getByText("Speculating").getAttribute("data-tone")).toBe("progress");
    expect(screen.getByRole("link", { name: "Next page" }).getAttribute("href")).toBe(
      "/requests?from=1&to=2&page=opaque-token",
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

  it("renders summary and ordered status/build history together", () => {
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
    render(<RequestDetail model={model} />);

    expect(screen.getByRole("heading", { level: 1 }).textContent).toContain("demo-queue/1");
    const history = screen.getByRole("list", { name: "Request history" });
    expect(within(history).getAllByRole("listitem")).toHaveLength(2);
    expect(within(history).getByText("Building")).toBeTruthy();
    expect(within(history).getByText("https://build.example/1")).toBeTruthy();
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
