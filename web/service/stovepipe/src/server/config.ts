import "server-only";
import { parseStovepipeBaseUrl } from "./transport";
import type { Queue } from "@submitqueue/web-stovepipe/server";
export function loadConfiguration(environment: Readonly<Record<string, string | undefined>> = process.env) {
  const baseUrl = parseStovepipeBaseUrl(environment.STOVEPIPE_URL ?? "", environment.STOVEPIPE_ALLOW_PLAINTEXT === "true");
  const value: unknown = JSON.parse(environment.STOVEPIPE_WEB_QUEUES ?? "[]");
  if (!Array.isArray(value) || value.length === 0) throw new Error("STOVEPIPE_WEB_QUEUES must contain at least one queue");
  const queues: Queue[] = [];
  for (const queue of value) {
    if (typeof queue !== "object" || queue === null || typeof queue.name !== "string" || !queue.name || !Array.isArray(queue.projects) || queue.projects.length === 0 || queue.projects.some((project: unknown) => typeof project !== "string" || !project)) throw new Error("Each queue needs a name and at least one project ID");
    if (queues.some(item => item.name === queue.name) || new Set(queue.projects).size !== queue.projects.length) throw new Error("Queue names and project IDs must be unique");
    queues.push({ name: queue.name, projects: [...queue.projects] });
  }
  return { baseUrl, allowPlaintext: environment.STOVEPIPE_ALLOW_PLAINTEXT === "true", queues };
}
