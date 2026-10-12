import { createLogCallback, type LogCallback } from "@submitqueue/web-platform/logging";
import type { ReadResult } from "../entity/index.js";

export async function readSection<T>(
  operation: () => Promise<T>,
  onLog: LogCallback | undefined,
  section: string,
): Promise<ReadResult<T>> {
  try {
    return { ok: true, data: await operation() };
  } catch (err) {
    createLogCallback(onLog)("warn", "Stovepipe page section failed", { error: err, section });
    return {
      ok: false,
      error: `Unable to load ${section}. Try refreshing. If the problem continues, check that the request exists and that the service is available.`,
    };
  }
}
