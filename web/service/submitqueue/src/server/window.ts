export const REQUEST_WINDOW_MS = 24 * 60 * 60 * 1_000;
export const REQUEST_PAGE_SIZE = 50;

export type RequestWindow = Readonly<{
  fromMs: number;
  toMs: number;
  pageToken?: string;
}>;

export type RequestSearchParams = Readonly<{
  from?: string | string[];
  to?: string | string[];
  page?: string | string[];
}>;

function one(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? undefined : value;
}

function parseMillisecond(value: string | undefined): number | undefined {
  if (!value || !/^\d+$/u.test(value)) {
    return undefined;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

export function parseRequestWindow(
  searchParams: RequestSearchParams,
): RequestWindow | undefined {
  const fromMs = parseMillisecond(one(searchParams.from));
  const toMs = parseMillisecond(one(searchParams.to));
  if (fromMs === undefined || toMs === undefined || fromMs >= toMs) {
    return undefined;
  }

  const pageToken = one(searchParams.page);
  return pageToken ? { fromMs, toMs, pageToken } : { fromMs, toMs };
}

export function defaultRequestWindow(nowMs = Date.now()): RequestWindow {
  return { fromMs: nowMs - REQUEST_WINDOW_MS, toMs: nowMs };
}

export function requestWindowSearch(window: RequestWindow): URLSearchParams {
  const search = new URLSearchParams({
    from: String(window.fromMs),
    to: String(window.toMs),
  });
  if (window.pageToken) {
    search.set("page", window.pageToken);
  }
  return search;
}
