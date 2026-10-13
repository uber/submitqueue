import type { WebError } from "../entity/index.js";
import type { GatewayErrorCode } from "../extension/gateway/index.js";

export const INVALID_INPUT: WebError = {
  kind: "user",
  title: "Invalid request",
  message: "The request parameters are invalid.",
  retryable: false,
};

export const INTERNAL_ERROR: WebError = {
  kind: "internal",
  title: "Something went wrong",
  message: "SubmitQueue could not load this information.",
  retryable: false,
};

/** Viewer-safe presentation of a gateway failure; raw gateway messages are never shown. */
export function webErrorForGatewayCode(code: GatewayErrorCode): WebError {
  switch (code) {
    case "invalid-argument":
      return INVALID_INPUT;
    case "not-found":
      return {
        kind: "not-found",
        title: "Request not found",
        message: "The requested SubmitQueue request was not found.",
        retryable: false,
      };
    case "resource-exhausted":
      return {
        kind: "user",
        title: "Request limit reached",
        message: "The requested result set is too large. Narrow the time window and try again.",
        retryable: false,
      };
    case "unavailable":
      return {
        kind: "transient",
        title: "SubmitQueue is unavailable",
        message: "SubmitQueue could not be reached. Try again shortly.",
        retryable: true,
      };
    case "internal":
      return INTERNAL_ERROR;
  }
}
