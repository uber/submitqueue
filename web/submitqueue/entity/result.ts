/** Origin of a failure shown to a viewer; unknown failures are `"internal"`. */
export type WebErrorKind = "user" | "not-found" | "transient" | "internal";

/** A sanitized, viewer-safe description of a failed load. */
export interface WebError {
  /** Failure origin, used to select presentation and retry behavior. */
  kind: WebErrorKind;
  /** Short heading for the failure. */
  title: string;
  /** Viewer-facing explanation; never contains raw backend messages. */
  message: string;
  /** Whether repeating the same load can succeed without viewer action. */
  retryable: boolean;
}

/** Outcome of a load: either serializable data or a sanitized error. */
export type LoadResult<T> =
  | { ok: true; data: T }
  | { ok: false; error: WebError };
