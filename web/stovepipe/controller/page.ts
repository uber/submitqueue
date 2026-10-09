import type { Logger } from "pino";
import type { ReadResult } from "../entity/index.js";

export async function readSection<T>(operation: () => Promise<T>, logger: Logger, section: string): Promise<ReadResult<T>> {
  try { return { ok: true, data: await operation() }; }
  catch (err) {
    logger.warn({ err, section }, "Stovepipe page section failed");
    return { ok: false, error: `Unable to load ${section}. Try refreshing. If the problem continues, check that the request exists and that the service is available.` };
  }
}
