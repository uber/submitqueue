import { describe, expect, it } from "vitest";
import { pino } from "pino";
import { createPinoLogCallback } from "./logging.js";

describe("Pino log delivery", () => {
  it("preserves severity, message, metadata and the serialized error stack", () => {
    const lines: string[] = [];
    const log = createPinoLogCallback(
      pino({ level: "info" }, {
        write: (line) => { lines.push(line); },
      }),
    );
    const error = new Error("backend unavailable");
    log("info", "Access denied", { requestId: "request-1" });
    log("warn", "Backend call failed", { requestId: "request-2", error });
    log("error", "Page load failed", { requestId: "request-3", error });

    const entries = lines.map((line) => JSON.parse(line));
    expect(entries).toMatchObject([
      { level: 30, msg: "Access denied", requestId: "request-1" },
      {
        level: 40, msg: "Backend call failed", requestId: "request-2",
        err: { message: error.message, stack: error.stack },
      },
      {
        level: 50, msg: "Page load failed", requestId: "request-3",
        err: { message: error.message, stack: error.stack },
      },
    ]);
    expect(entries[1]).not.toHaveProperty("error");
  });
});
