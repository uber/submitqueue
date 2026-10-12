import { createLogCallback, type LogCallback } from "@submitqueue/web-platform/logging";

export type HealthChecks = Readonly<Record<string, () => void | Promise<void>>>;

const NO_STORE = { "Cache-Control": "no-store" };

/**
 * A readiness route handler: runs the named checks in order and answers 200
 * when all pass, otherwise 503. Each check's failure is logged once until it
 * recovers, so a polling orchestrator does not flood the logs.
 */
export function createNextHealthRoute(checks: HealthChecks, onLog?: LogCallback) {
  const failing = new Set<string>();
  const log = createLogCallback(onLog);
  return async function GET(): Promise<Response> {
    for (const [name, check] of Object.entries(checks)) {
      try {
        await check();
      } catch (cause) {
        if (!failing.has(name)) {
          failing.add(name);
          log("error", "Web readiness check failed", { check: name, error: cause });
        }
        return Response.json({ status: "unavailable" }, { status: 503, headers: NO_STORE });
      }
      failing.delete(name);
    }
    return Response.json({ status: "ok" }, { headers: NO_STORE });
  };
}
