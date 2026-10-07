import { describe, expect, it } from "vitest";
import { isTerminalStatus, knownRequestStatuses, statusDisplay } from "./status";

describe("request status display", () => {
  it.each(["landed", "error", "cancelled"])("marks %s terminal", (status) => {
    expect(isTerminalStatus(status)).toBe(true);
  });

  it.each(["accepted", "validating", "speculating", "landing", "cancelling"])(
    "keeps %s active",
    (status) => {
      expect(isTerminalStatus(status)).toBe(false);
    },
  );

  it("renders newly-added statuses with a readable fallback", () => {
    expect(statusDisplay("waiting_for_owner")).toEqual({
      label: "Waiting For Owner",
      tone: "neutral",
      terminal: false,
    });
    expect(knownRequestStatuses).toContain("landed");
  });
});
