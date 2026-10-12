import type { LogCallback } from "@submitqueue/web-platform/logging";
import type { Logger } from "pino";

export function createPinoLogCallback(logger: Logger): LogCallback {
  return (level, message, metadata) => {
    const { error, ...fields } = metadata;
    logger[level]("error" in metadata ? { ...fields, err: error } : fields, message);
  };
}
