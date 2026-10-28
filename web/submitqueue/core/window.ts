export const REQUEST_WINDOW_MS = 24 * 60 * 60 * 1_000;
export const REQUEST_PAGE_SIZE = 50;

/** Half-open receipt window, Unix epoch milliseconds. */
export type RequestWindow = Readonly<{
  fromMs: number;
  toMs: number;
}>;

/** Where a queue read starts: a new window, or a gateway continuation token, which carries its own window. */
export type RequestPageStart = Readonly<{ window: RequestWindow }> | Readonly<{ pageToken: string }>;

export function defaultRequestWindow(nowMs = Date.now()): RequestWindow {
  return { fromMs: nowMs - REQUEST_WINDOW_MS, toMs: nowMs };
}

/** Every receipt before `nowMs`, so a queue list starts at its newest request however old it is. */
export function allReceiptsWindow(nowMs = Date.now()): RequestWindow {
  return { fromMs: 0, toMs: nowMs };
}
