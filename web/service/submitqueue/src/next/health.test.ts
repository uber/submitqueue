import { describe, expect, it, vi } from "vitest";

import type { Logger } from "pino";
import { createNextHealthRoute } from "./health";

function recordingLogger() {
  const error = vi.fn();
  const logger = { child: () => logger, debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error } as unknown as Logger;
  return { logger, error };
}

describe("createNextHealthRoute", () => {
  it("is ready and uncached when every check passes", async () => {
    const { logger } = recordingLogger();
    const response = await createNextHealthRoute({ configuration: () => {}, gateway: async () => {} }, logger)();
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({ status: "ok" });
  });

  it("stops at the first failing check and logs it once until it recovers", async () => {
    const cause = new Error("connection refused");
    const gateway = vi.fn().mockRejectedValueOnce(cause).mockRejectedValueOnce(cause)
      .mockResolvedValueOnce(undefined).mockRejectedValueOnce(cause);
    const later = vi.fn();
    const { logger, error } = recordingLogger();
    const GET = createNextHealthRoute({ gateway, later }, logger);

    expect((await GET()).status).toBe(503);
    expect((await GET()).status).toBe(503);
    expect(error).toHaveBeenCalledOnce();
    expect(error).toHaveBeenCalledWith({ check: "gateway", err: cause }, "Web readiness check failed");
    expect(later).not.toHaveBeenCalled();
    expect((await GET()).status).toBe(200);
    expect((await GET()).status).toBe(503);
    expect(error).toHaveBeenCalledTimes(2);
  });
});
