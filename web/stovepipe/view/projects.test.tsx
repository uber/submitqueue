import { act, cleanup, fireEvent, render, screen } from "../../platform/test/rtl.js";
import { afterEach, expect, it, vi } from "vitest";
import type { ProjectStatusModel } from "../entity/index.js";
import { ProjectResults } from "./projects.js";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const model: ProjectStatusModel = {
  requestState: "processing",
  updatedAtMs: "1000",
  repositoryBreakageDegree: null,
  complete: false,
  pageSize: 50,
  pageSizeChoices: [{ value: 50, href: "/queue/request/7" }],
  projects: [],
  firstHref: null,
  nextHref: null,
};

it.each([
  ["accepted", false, "Waiting to start"],
  ["processing", false, "Waiting for builds"],
  ["succeeded", false, "Results pending"],
  ["failed", false, "Results pending"],
  ["cancelled", false, "Cancelled"],
  ["superseded", false, "Superseded"],
  ["", false, "Status unknown"],
  ["future-state", false, "Status unknown"],
  ["succeeded", true, "Recording finished"],
  ["failed", true, "Recording finished"],
  ["future-state", true, "Recording finished"],
] as const)("shows project recording status for %s with completion %s as %s", (requestState, complete, label) => {
  render(<ProjectResults model={{ ...model, requestState, complete }} pageKey="/queue/request/7" />);
  expect(screen.getByLabelText("Project result recording")).toHaveTextContent(label);
});

it.each([
  ["hover", false, "Waiting for builds to finish before recording project results."],
  ["focus", true, "Project result recording has finished. See the recorded results for the outcome."],
] as const)("explains project recording on %s", async (trigger, complete, explanation) => {
  vi.useFakeTimers();
  render(<ProjectResults model={{ ...model, complete }} pageKey="/queue/request/7" />);
  expect(screen.queryByRole("tooltip")).toBeNull();
  const badge = screen.getByLabelText("Project result recording");
  if (trigger === "hover") fireEvent.mouseEnter(badge);
  else act(() => badge.focus());
  await act(async () => vi.runOnlyPendingTimersAsync());
  expect(screen.getByRole("tooltip")).toHaveTextContent(explanation);
});

it.each([false, true])("does not confuse missing attribution with a completed empty issue list (%s)", complete => {
  render(<ProjectResults pageKey="request/7" model={{ ...model, projects: [
    { project: "p", projectType: "service", breakageDegree: 1, issuesHref: "/issues" },
  ] }} issues={{ project: "p", key: "p", closeHref: "/projects", result: { ok: true, data: {
    complete, issues: [], firstHref: null, nextHref: null,
  } } }} />);
  expect(screen.getByText(complete ? "No issues attributed to this project in this request." : "Project attribution is not available yet or did not complete.")).toBeVisible();
  expect(screen.getByText("Breakage")).toBeVisible();
  expect(screen.getByRole("link", { name: "Hide issues" })).toHaveAttribute("href", "/projects");
});

it("shows associated task/unit identities and keeps recorded results when a page fails", () => {
  const projectModel = { ...model, projects: [{ project: "p", projectType: "", breakageDegree: 0.25, issuesHref: "/issues" }] };
  const selection = { project: "p", key: "p/first", closeHref: "/projects" };
  const { rerender } = render(<ProjectResults model={projectModel} pageKey="request/7" issues={{ ...selection, result: { ok: true, data: {
    complete: true, issues: [{ taskId: "test", unitId: "//pkg:failed" }], firstHref: null, nextHref: "/next",
  } } }} />);
  expect(screen.getByRole("table", { name: "Issues affecting p" })).toHaveTextContent("//pkg:failed");
  expect(screen.getByRole("link", { name: "More project issues →" })).toHaveAttribute("href", "/next");
  rerender(<ProjectResults model={projectModel} pageKey="request/7" issues={{ ...selection, key: "p/next", result: { ok: false, error: "Issues unavailable" } }} />);
  expect(screen.getByRole("alert")).toHaveTextContent("Issues unavailable");
  expect(screen.queryByText("//pkg:failed")).toBeNull();
  expect(screen.getByText("Partial Breakage")).toBeVisible();
  expect(screen.getByRole("link", { name: "Hide issues" })).toBeVisible();
});
