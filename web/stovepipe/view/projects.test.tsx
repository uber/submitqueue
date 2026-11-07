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
