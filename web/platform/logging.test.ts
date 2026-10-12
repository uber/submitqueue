import { describe, expect, it, vi } from "vitest";
import { createLogCallback } from "./logging.js";

describe("log delivery", () => {
  it("preserves errors and request context without mutating caller metadata", () => {
    const error = new Error("upstream detail");
    const metadata = Object.freeze({ error, queue: "repo/main", requestId: "other" });
    const onLog = vi.fn();
    const log = createLogCallback(onLog, { requestId: "request-1" });

    log("warn", "Backend call failed", metadata);

    expect(onLog).toHaveBeenCalledWith("warn", "Backend call failed", {
      error,
      queue: "repo/main",
      requestId: "request-1",
    });
    expect(metadata.requestId).toBe("other");
  });

  it("isolates a failing logging sink", () => {
    const log = createLogCallback(() => {
      throw new Error("logging unavailable");
    });
    expect(() => log("error", "Backend call failed", {})).not.toThrow();
  });
});
