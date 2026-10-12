import type { Meter } from "@opentelemetry/api";
import { describe, expect, it, vi } from "vitest";

import { beginOperation } from "./observability.js";

describe("beginOperation", () => {
  it("records one duration tagged with the start attributes and outcome", () => {
    const record = vi.fn();
    const createHistogram = vi.fn(() => ({ record }));
    const operation = beginOperation({ createHistogram } as unknown as Meter, "web.page_load", { page: "queue" });

    operation.end("success", { module: "submitqueue" });

    expect(createHistogram).toHaveBeenCalledWith("web.page_load.duration", expect.objectContaining({ unit: "ms" }));
    expect(record).toHaveBeenCalledWith(expect.any(Number), { page: "queue", module: "submitqueue", outcome: "success" });
  });
});
