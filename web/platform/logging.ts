export type LogLevel = "info" | "warn" | "error";
export type LogMetadata = Readonly<Record<string, unknown>>;

/** Synchronous host callback; metadata may contain an unmodified Error under `error`. */
export type LogCallback = (
  level: LogLevel,
  message: string,
  metadata: LogMetadata,
) => void;

/** Absent callbacks are silent; callback failures cannot change application outcomes. */
export function createLogCallback(
  onLog?: LogCallback,
  context: LogMetadata = {},
): LogCallback {
  return (level, message, metadata) => {
    try {
      onLog?.(level, message, { ...metadata, ...context });
    } catch {
      // Log delivery must not turn an upstream failure into a page failure.
    }
  };
}
