import type { CursorCodec } from "../extension/cursor/index.js";

export const REQUEST_WINDOW_MS = 24 * 60 * 60 * 1_000;
export const REQUEST_PAGE_SIZE = 50;

const CURSOR_VERSION = "v1";

export type RequestWindow = Readonly<{
  fromMs: number;
  toMs: number;
  pageToken?: string;
}>;

export function defaultRequestWindow(nowMs = Date.now()): RequestWindow {
  return { fromMs: nowMs - REQUEST_WINDOW_MS, toMs: nowMs };
}

function parseMillisecond(value: string | undefined): number | undefined {
  if (!value || !/^\d+$/u.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

/** Encodes a snapshot page — the original window bounds plus the gateway's token — as a scoped cursor. */
export async function encodeWindowCursor(codec: CursorCodec, scope: string, window: RequestWindow, token: string): Promise<string> {
  return codec.encode(scope, [CURSOR_VERSION, window.fromMs, window.toMs, token].join("\n"));
}

/** Decodes a cursor from {@link encodeWindowCursor}; `undefined` when it is foreign, tampered, or malformed. */
export async function decodeWindowCursor(codec: CursorCodec, scope: string, cursor: string): Promise<RequestWindow | undefined> {
  const payload = await codec.decode(scope, cursor);
  if (payload === undefined) {
    return undefined;
  }
  const [version, from, to, ...tokenParts] = payload.split("\n");
  const fromMs = parseMillisecond(from);
  const toMs = parseMillisecond(to);
  const pageToken = tokenParts.join("\n");
  if (version !== CURSOR_VERSION || fromMs === undefined || toMs === undefined ||
    toMs - fromMs !== REQUEST_WINDOW_MS || !pageToken) {
    return undefined;
  }
  return { fromMs, toMs, pageToken };
}
