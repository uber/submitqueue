import { describe, expect, it, vi } from "vitest";

import { createNextHealthRoute } from "./health";

describe("createNextHealthRoute", () => {
  it("still returns unavailable if log delivery throws", async () => {
    const GET = createNextHealthRoute(
      { gateway: () => { throw new Error("gateway unavailable"); } },
      () => { throw new Error("logging unavailable"); },
    );
    expect((await GET()).status).toBe(503);
  });
  it("is ready and uncached when every check passes", async () => {
    const response = await createNextHealthRoute({ configuration: () => {}, gateway: async () => {} })();
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({ status: "ok" });
  });

  it("stops at the first failing check and logs it once until it recovers", async () => {
    const cause = new Error("connection refused");
    const gateway = vi.fn().mockRejectedValueOnce(cause).mockRejectedValueOnce(cause)
      .mockResolvedValueOnce(undefined).mockRejectedValueOnce(cause);
    const later = vi.fn();
    const onLog = vi.fn();
    const GET = createNextHealthRoute({ gateway, later }, onLog);

    expect((await GET()).status).toBe(503);
    expect((await GET()).status).toBe(503);
    expect(onLog).toHaveBeenCalledOnce();
    expect(onLog).toHaveBeenCalledWith("error", "Web readiness check failed", { check: "gateway", error: cause });
    expect(later).not.toHaveBeenCalled();
    expect((await GET()).status).toBe(200);
    expect((await GET()).status).toBe(503);
    expect(onLog).toHaveBeenCalledTimes(2);
  });
});
