import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { LoadResult } from "../entity/index.js";
import { useLastSuccessfulData } from "./last-successful-data.js";

const loaded: LoadResult<string> = { ok: true, data: "snapshot" };
const transient: LoadResult<string> = { ok: false, error: { kind: "transient", title: "Unavailable", message: "Try again", retryable: true } };
const permanent: LoadResult<string> = { ok: false, error: { kind: "internal", title: "Failed", message: "Contact support", retryable: false } };

describe("useLastSuccessfulData", () => {
  it("keeps the last data through retryable failures and clears it on a permanent one", () => {
    const { result, rerender } = renderHook(({ value }: { value: LoadResult<string> }) => useLastSuccessfulData(value), {
      initialProps: { value: loaded as LoadResult<string> },
    });
    expect(result.current).toBe("snapshot");
    rerender({ value: transient });
    expect(result.current).toBe("snapshot");
    rerender({ value: { ...transient } });
    expect(result.current).toBe("snapshot");
    rerender({ value: permanent });
    expect(result.current).toBeUndefined();
    rerender({ value: { ...transient } });
    expect(result.current).toBeUndefined();
  });

  it("has nothing to show when the first result fails", () => {
    const { result } = renderHook(() => useLastSuccessfulData(transient));
    expect(result.current).toBeUndefined();
  });
});
