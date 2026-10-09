import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { StovepipeApp } from "./app.js";
import type { StovepipePageModel } from "../entity/index.js";
afterEach(cleanup);
const model: StovepipePageModel = { kind: "request", key: "/repo%2Fmain/change/git%3A%2F%2Frepo%2Fsha?", queue: "repo/main", requestId: "7", changeUri: "git://repo/sha", queues: [{ name: "repo/main", href: "/repo%2Fmain" }], queueHref: "/repo%2Fmain", latestHref: "/repo%2Fmain/change/git%3A%2F%2Frepo%2Fsha", showLatestLink: false, loadedAtMs: 1000, status: { ok: true, data: { request: { id: "7", href: "/repo%2Fmain/change/git%3A%2F%2Frepo%2Fsha", changeUri: "git://repo/sha", baseUri: "", state: "completed", updatedAtMs: "1000", acceptedAtMs: "0", outcomeReason: "" }, repositoryDegree: 0, complete: false, projects: [{ name: "green-project", degree: 0 }, { name: "pending-project", degree: null }], nextHref: null, firstHref: null, rawHref: "/repo%2Fmain/change/git%3A%2F%2Frepo%2Fsha/projects.json" } }, history: { ok: true, data: [{ id: "e1", timestampMs: "1000", kind: "requestState", label: "completed", outcomeReason: "all_targets_passed" }] } };
it("displays green and missing project results separately, plus history reasons", () => {
  render(<StovepipeApp model={model} />);
  expect(screen.getAllByText("Green (0)")).toHaveLength(2);
  expect(screen.getByText("No recorded result")).toBeVisible();
  expect(screen.getByText("all_targets_passed")).toBeVisible();
});
it("retains successful status and history after a failed refresh", () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(<StovepipeApp model={{ ...model, status: { ok: false, error: "Status unavailable" }, history: { ok: false, error: "History unavailable" } }} />);
  expect(screen.getByText("git://repo/sha")).toBeVisible();
  expect(screen.getByText("all_targets_passed")).toBeVisible();
  expect(screen.getAllByText("Showing the last successful result.")).toHaveLength(2);
});
it("discards previous results when the request changes", () => {
  const { rerender } = render(<StovepipeApp model={model} />);
  rerender(<StovepipeApp model={{ ...model, key: "/repo%2Fmain/change/git%3A%2F%2Frepo%2Fother?", requestId: null, changeUri: "git://repo/other", status: { ok: false, error: "Status unavailable" }, history: { ok: false, error: "History unavailable" } }} />);
  expect(screen.queryByText("git://repo/sha")).toBeNull();
  expect(screen.queryByText("all_targets_passed")).toBeNull();
});
