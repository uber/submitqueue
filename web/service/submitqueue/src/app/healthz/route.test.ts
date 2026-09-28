import { describe, expect, it } from "vitest";

import { GET } from "./route";

describe("GET /healthz", () => {
  it("is a public no-store liveness response", async () => {
    const response = GET();

    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    await expect(response.json()).resolves.toEqual({ status: "ok" });
  });
});
