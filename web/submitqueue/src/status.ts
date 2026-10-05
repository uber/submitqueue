import type { StatusDisplay } from "./models.js";

const STATUS_DISPLAY: Readonly<Record<string, StatusDisplay>> = {
  accepting: { label: "Accepting", tone: "progress", terminal: false },
  accepted: { label: "Accepted", tone: "progress", terminal: false },
  started: { label: "Started", tone: "progress", terminal: false },
  validating: { label: "Validating", tone: "progress", terminal: false },
  validated: { label: "Validated", tone: "progress", terminal: false },
  batching: { label: "Batching", tone: "progress", terminal: false },
  batched: { label: "Batched", tone: "progress", terminal: false },
  speculating: { label: "Speculating", tone: "progress", terminal: false },
  speculated: { label: "Speculated", tone: "progress", terminal: false },
  landing: { label: "Landing", tone: "progress", terminal: false },
  landed: { label: "Landed", tone: "success", terminal: true },
  cancelling: { label: "Cancelling", tone: "warning", terminal: false },
  cancelled: { label: "Cancelled", tone: "neutral", terminal: true },
  error: { label: "Error", tone: "danger", terminal: true },
};

const UNKNOWN_STATUS: StatusDisplay = {
  label: "Unknown",
  tone: "neutral",
  terminal: false,
};

export const knownRequestStatuses = Object.freeze(Object.keys(STATUS_DISPLAY));

export function statusDisplay(status: string): StatusDisplay {
  const known = STATUS_DISPLAY[status];
  if (known !== undefined) {
    return known;
  }
  const normalized = status.trim();
  if (normalized === "") {
    return UNKNOWN_STATUS;
  }
  return {
    ...UNKNOWN_STATUS,
    label: normalized
      .split(/[-_\s]+/u)
      .filter(Boolean)
      .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
      .join(" "),
  };
}

export function isTerminalStatus(status: string): boolean {
  return statusDisplay(status).terminal;
}
