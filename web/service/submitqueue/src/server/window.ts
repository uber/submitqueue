import { createHmac, timingSafeEqual } from "node:crypto";

export const REQUEST_WINDOW_MS = 24 * 60 * 60 * 1_000;
export const REQUEST_PAGE_SIZE = 50;

export type RequestWindow = Readonly<{
  fromMs: number;
  toMs: number;
  pageToken?: string;
}>;

export type RequestSearchParams = Readonly<{
  page?: string | string[];
  from?: string | string[];
  to?: string | string[];
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

export function encodeRequestPage(queue: string, window: RequestWindow, token: string, secret: string): string {
  const body = Buffer.from(
    ["v1", encodeURIComponent(queue), window.fromMs, window.toMs, token].join("\n"),
  ).toString("base64url");
  const signature = createHmac("sha256", secret).update(body).digest("base64url");
  return `${body}.${signature}`;
}

export function decodeRequestPage(queue: string, cursor: string, secret: string): RequestWindow | undefined {
  if (cursor.length > 16_384 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u.test(cursor)) {
    return undefined;
  }
  const [body, signature] = cursor.split(".");
  const actual = Buffer.from(signature!, "base64url");
  const expected = createHmac("sha256", secret).update(body!).digest();
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
    return undefined;
  }
  const [version, scope, from, to, ...tokenParts] = Buffer.from(body!, "base64url").toString("utf8").split("\n");
  const fromMs = parseMillisecond(from);
  const toMs = parseMillisecond(to);
  const pageToken = tokenParts.join("\n");
  if (version !== "v1" || scope !== encodeURIComponent(queue) || fromMs === undefined ||
    toMs === undefined || toMs - fromMs !== REQUEST_WINDOW_MS || !pageToken) {
    return undefined;
  }
  return { fromMs, toMs, pageToken };
}
