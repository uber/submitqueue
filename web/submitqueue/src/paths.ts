export interface RequestsPathOptions {
  fromMs?: number;
  toMs?: number;
  pageToken?: string | null;
}

export interface WebPathsOptions {
  requestsPath?: string;
}

function encodeBase64Url(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return globalThis
    .btoa(binary)
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/u, "");
}

function decodeBase64Url(value: string): string | undefined {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) {
    return undefined;
  }
  const standard = value.replaceAll("-", "+").replaceAll("_", "/");
  const padding = "=".repeat((4 - (standard.length % 4)) % 4);
  try {
    const binary = globalThis.atob(`${standard}${padding}`);
    const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return undefined;
  }
}

export class WebPaths {
  readonly requestsPath: string;

  constructor(options: WebPathsOptions = {}) {
    const requestedPath = options.requestsPath ?? "/requests";
    this.requestsPath = requestedPath.endsWith("/")
      ? requestedPath.slice(0, -1)
      : requestedPath;
  }

  requests(options: RequestsPathOptions = {}): string {
    const parameters = new URLSearchParams();
    if (options.fromMs !== undefined) {
      parameters.set("from", String(options.fromMs));
    }
    if (options.toMs !== undefined) {
      parameters.set("to", String(options.toMs));
    }
    if (options.pageToken) {
      parameters.set("page", options.pageToken);
    }
    const query = parameters.toString();
    return query === "" ? this.requestsPath : `${this.requestsPath}?${query}`;
  }

  request(sqid: string): string {
    return `${this.requestsPath}/${encodeBase64Url(sqid)}`;
  }

  decodeRequest(segment: string): string | undefined {
    return decodeBase64Url(segment);
  }
}

export function encodeSqidSegment(sqid: string): string {
  return encodeBase64Url(sqid);
}

export function decodeSqidSegment(segment: string): string | undefined {
  return decodeBase64Url(segment);
}
