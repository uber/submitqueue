import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ErrorState, RequestStatus } from "./components";
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
