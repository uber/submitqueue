import "server-only";
import { parseServiceBaseUrl } from "@submitqueue/web-service-shared/transport";
import type { Queue } from "@submitqueue/web-stovepipe/server";
export function loadConfiguration(
  environment: Readonly<Record<string, string | undefined>> = process.env,
) {
  const baseUrl = parseServiceBaseUrl(
    environment.STOVEPIPE_URL ?? "",
    environment.STOVEPIPE_ALLOW_PLAINTEXT === "true",
  );
  const value: unknown = JSON.parse(environment.STOVEPIPE_WEB_QUEUES ?? "[]");
  if (!Array.isArray(value) || value.length === 0)
    throw new Error("STOVEPIPE_WEB_QUEUES must contain at least one queue");
  const queues: Queue[] = [];
  for (const queue of value) {
    if (
      typeof queue !== "object" ||
      queue === null ||
      typeof queue.name !== "string" ||
      !queue.name
    )
      throw new Error("Each queue needs a name");
    if (queues.some((item) => item.name === queue.name))
      throw new Error("Queue names must be unique");
    queues.push({ name: queue.name });
  }
  return {
    baseUrl,
    allowPlaintext: environment.STOVEPIPE_ALLOW_PLAINTEXT === "true",
    queues,
  };
}
