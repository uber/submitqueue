import { cleanup, render, screen, within } from "@testing-library/react";
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
  ).toEqual(["Request status", "History"]);
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
it("retains the summary and history after a failed refresh", () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(
    <StovepipeApp model={{
      ...model,
      history: { ok: false, error: "History unavailable" },
    }} />,
  );
  expect(screen.getByText("git://repo/sha")).toBeVisible();
  expect(screen.getByText("all_targets_passed")).toBeVisible();
  expect(screen.getByText("accepted")).toBeVisible();
  expect(screen.getByText("Showing the last successful result.")).toBeVisible();
});
it("discards previous results when the request changes", () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(
    <StovepipeApp model={{
      ...model,
      key: "/repo%2Fmain/request/8?",
      requestId: "8",
      history: { ok: false, error: "History unavailable" },
    }} />,
  );
  expect(screen.getByRole("heading", { name: "Request 8" })).toBeVisible();
  expect(screen.queryByText("git://repo/sha")).toBeNull();
  expect(screen.queryByText("all_targets_passed")).toBeNull();
});
