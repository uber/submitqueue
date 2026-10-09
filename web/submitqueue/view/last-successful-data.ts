"use client";

import { useState } from "react";
import type { LoadResult } from "../entity/index.js";

/**
 * Returns the current data, or the last successful data while the current
 * result is a retryable failure. Permanent failures clear the snapshot, so key
 * the caller by resource to avoid carrying data across resources.
 */
export function useLastSuccessfulData<T>(result: LoadResult<T>): T | undefined {
  const [snapshot, setSnapshot] = useState<{ result: LoadResult<T>; data: T | undefined }>(() => ({
    result, data: result.ok ? result.data : undefined,
  }));
  if (snapshot.result !== result) {
    setSnapshot({
      result,
      data: result.ok ? result.data : result.error.retryable ? snapshot.data : undefined,
    });
  }
  return result.ok ? result.data : result.error.retryable ? snapshot.data : undefined;
}
