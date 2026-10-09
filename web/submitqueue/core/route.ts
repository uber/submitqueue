import { decodePathSegment } from "./path-segment.js";

/** Raw query parameters of a page URL, as frameworks commonly expose them. */
export type WebSearchParams = Readonly<Record<string, string | string[] | undefined>>;

/** A decoded SubmitQueue page address below the host's mount point. */
export type SubmitQueueWebRoute =
  | { kind: "queues" }
  | { kind: "queue"; queue: string; search?: WebSearchParams }
  | { kind: "request"; queue: string; sqid: string; search?: WebSearchParams }
  | { kind: "change"; queue: string; reference: readonly string[]; search?: WebSearchParams };

function decodeComponent(value: string): string | null {
  try {
    return decodeURIComponent(value);
  } catch {
    // Malformed percent-encoding is an unroutable address, not a failure.
    return null;
  }
}

function decodeResourceSegment(value: string): string | null {
  const decoded = decodeComponent(value);
  return decoded === null ? null : decodePathSegment(decoded);
}

/**
 * Maps the still percent-encoded path segments below the mount point, as
 * produced by {@link WebPaths}, to a route; `null` when no page matches.
 */
export function parseRoute(segments: readonly string[], search: WebSearchParams = {}): SubmitQueueWebRoute | null {
  if (segments.length === 0) {
    return { kind: "queues" };
  }
  if (segments.some(segment => segment === "")) {
    return null;
  }
  const [encodedQueue, resource, ...rest] = segments;
  const queue = decodeResourceSegment(encodedQueue!);
  if (queue === null || queue === "") {
    return null;
  }
  if (resource === undefined) {
    return { kind: "queue", queue, search };
  }
  if (rest.length === 0) {
    return null;
  }
  if (resource === "request") {
    const parts = rest.map(decodeResourceSegment);
    return parts.includes(null) ? null : { kind: "request", queue, sqid: parts.join("/"), search };
  }
  if (resource === "change") {
    const reference = rest.map(decodeComponent);
    return reference.includes(null) ? null : { kind: "change", queue, reference: reference as string[], search };
  }
  return null;
}
