/** Visual emphasis of a request status. */
export type StatusTone = "neutral" | "progress" | "success" | "danger" | "warning";

/** How one request status string is presented. */
export interface StatusDisplay {
  /** Human-readable status name. */
  label: string;
  /** Visual emphasis for the status. */
  tone: StatusTone;
  /** Whether a request in this status can no longer change. */
  terminal: boolean;
}
