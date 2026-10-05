import type { RequestSummaryModel } from "@submitqueue/web-submitqueue";
import { WebPaths } from "@submitqueue/web-submitqueue/server";

export interface ChangeReference {
  scheme: "github" | "phab" | "git";
  host: string;
  repository: string;
  review: string;
  logicalPath: string;
  version: string | null;
  uri: string | null;
}

function encodeUriSegment(value: string): string {
  return encodeURIComponent(value)
    .replace(/[!'()*]/gu, character => `%${character.charCodeAt(0).toString(16).toUpperCase()}`)
    .replace(/%(24|26|2B|3A|3D|40)/gu, encoded => String.fromCharCode(Number.parseInt(encoded.slice(1), 16)));
}

function decodeCanonicalUriSegment(value: string): string | null {
  if (Array.from(value).some(character => character.charCodeAt(0) > 127) ||
    /%(?![a-f0-9]{2})/iu.test(value)) {
    return null;
  }
  const byteString = value.replace(/%([a-f0-9]{2})/giu, (_, hex: string) =>
    String.fromCharCode(Number.parseInt(hex, 16)));
  const decoded = new TextDecoder("utf-8", { ignoreBOM: true }).decode(
    Uint8Array.from(byteString, character => character.charCodeAt(0)),
  );
  return encodeUriSegment(decoded) === value ? decoded : null;
}

export function parseChangePath(parts: readonly string[]): ChangeReference | null {
  const [scheme, host, ...path] = parts;
  if ((scheme !== "github" && scheme !== "phab" && scheme !== "git") || !host ||
    host !== host.toLowerCase() || !/^[a-z0-9.-]+(?::[0-9]+)?$/u.test(host) ||
    host === "." || host === ".." ||
    path.some(value => !value || value === "." || value === ".." || /[?#]/u.test(value) ||
      Array.from(value).some(character => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127))) {
    return null;
  }
  let repository: string;
  let review: string;
  let version: string | null;
  let logicalParts: string[];
  if (scheme === "git") {
    const pinned = /^[a-f0-9]{40}$/u.test(path.at(-1) ?? "");
    const refIndex = path.length - (pinned ? 2 : 1);
    const ref = path[refIndex];
    if (refIndex < 1 || !ref?.startsWith("refs/") || ref === "refs/" ||
      path.slice(0, refIndex).some(value => value.includes("/"))) {
      return null;
    }
    repository = path.slice(0, refIndex).join("/");
    review = ref;
    version = pinned ? path.at(-1)! : null;
    logicalParts = path.slice(0, refIndex + 1);
  } else if (scheme === "github") {
    if (path.some(value => value.includes("/"))) {
      return null;
    }
    const separator = path.lastIndexOf("pull");
    if (separator < 2 || (path.length !== separator + 2 && path.length !== separator + 3) ||
      !/^[1-9][0-9]*$/u.test(path[separator + 1] ?? "")) {
      return null;
    }
    repository = path.slice(0, separator).join("/");
    review = `PR #${path[separator + 1]}`;
    version = path[separator + 2] ?? null;
    if (version !== null && !/^[a-f0-9]{40}$/u.test(version)) {
      return null;
    }
    logicalParts = path.slice(0, separator + 2);
  } else {
    if ((path.length !== 1 && path.length !== 2) || !/^D[1-9][0-9]*$/u.test(path[0] ?? "") ||
      (path[1] !== undefined && !/^[1-9][0-9]*$/u.test(path[1]))) {
      return null;
    }
    repository = "";
    review = path[0]!;
    version = path[1] ?? null;
    logicalParts = [review];
  }
  const logicalPath = [scheme, host, ...logicalParts.map(encodeUriSegment)].join("/");
  const uri = version === null ? null :
    `${scheme}://${host}/${[...logicalParts, version].map(encodeUriSegment).join("/")}`;
  return { scheme, host, repository, review, logicalPath, version, uri };
}

export function parseChangeUri(uri: string): ChangeReference | null {
  const match = /^(github|phab|git):\/\/([^/?#]+)\/([^?#]+)(?:\?([^#]*))?$/u.exec(uri);
  if (!match) {
    return null;
  }
  if (match[4] !== undefined) {
    const metadata = new URLSearchParams(match[4]);
    if (match[1] !== "git" || [...metadata.keys()].some(key => key !== "sq-files" && key !== "sq-fake")) {
      return null;
    }
  }
  const path: string[] = [];
  for (const segment of match[3]!.split("/")) {
    const decoded = decodeCanonicalUriSegment(segment);
    if (decoded === null) {
      return null;
    }
    path.push(decoded);
  }
  const reference = parseChangePath([match[1]!, match[2]!, ...path]);
  const canonicalUri = `${match[1]}://${match[2]}/${match[3]}`;
  return reference?.uri === canonicalUri ? reference : null;
}

export function requestChangeLabels(requests: readonly RequestSummaryModel[]): Record<string, string> {
  const labels: Record<string, string> = {};
  for (const uri of requests.flatMap(request => request.changeUris)) {
    const reference = parseChangeUri(uri);
    if (reference?.scheme === "git" && reference.uri !== null) {
      labels[uri] = reference.uri;
    }
  }
  return labels;
}

export function changeHref(queue: string, reference: ChangeReference, pinned = false): string {
  return `${new WebPaths().requests(queue)}/change/${reference.logicalPath}${pinned && reference.version ? `/${reference.version}` : ""}`;
}

export function requestChangeLinks(queue: string, requests: readonly RequestSummaryModel[]): Record<string, string> {
  const links: Record<string, string> = {};
  for (const uri of requests.flatMap(request => request.changeUris)) {
    const reference = parseChangeUri(uri);
    if (reference) {
      links[uri] = changeHref(queue, reference);
    }
  }
  return links;
}
