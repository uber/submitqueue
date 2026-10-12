import { decodePathSegment } from "@submitqueue/web-platform/path-segment";
import type { Queue } from "../entity/index.js";
import type { StovepipePaths } from "./paths.js";

export function parseStovepipeRoute(
  path: string,
  paths: StovepipePaths,
  queues: readonly Queue[],
) {
  if (!path.startsWith(`${paths.basePath}/`)) return null;
  let segments: string[];
  try {
    segments = path
      .slice(paths.basePath.length + 1)
      .replace(/\/$/u, "")
      .split("/")
      .map((segment) => decodePathSegment(decodeURIComponent(segment)));
  } catch {
    return null;
  }
  const queue = queues.find((queue) => queue.name === segments[0]);
  if (!queue) return null;
  if (segments.length === 1)
    return { queue, requestId: "" };
  if (
    segments.length !== 3 ||
    segments[1] !== "request" ||
    !segments[2]
  )
    return null;
  return { queue, requestId: segments[2] };
}
