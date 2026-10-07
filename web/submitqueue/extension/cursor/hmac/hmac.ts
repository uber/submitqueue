import { createHmac, timingSafeEqual } from "node:crypto";
import type { CursorCodec } from "../cursor.js";

const MAX_CURSOR_LENGTH = 16_384;
const CURSOR_PATTERN = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u;

/**
 * An HMAC-SHA256 {@link CursorCodec} for Node. Cursors are readable base64url
 * payloads with a signature over scope and payload, so they are tamper-evident
 * but not confidential; never put secrets in a payload.
 */
export function newHmacCursorCodec(secret: string): CursorCodec {
  if (!secret) {
    throw new Error("The cursor signing secret must be nonempty");
  }
  const sign = (scope: string, body: string) =>
    createHmac("sha256", secret).update(`${encodeURIComponent(scope)}\n${body}`).digest();
  return {
    encode(scope, payload) {
      const body = Buffer.from(payload, "utf8").toString("base64url");
      return `${body}.${sign(scope, body).toString("base64url")}`;
    },
    decode(scope, cursor) {
      if (cursor.length > MAX_CURSOR_LENGTH || !CURSOR_PATTERN.test(cursor)) {
        return undefined;
      }
      const [body, signature] = cursor.split(".") as [string, string];
      const actual = Buffer.from(signature, "base64url");
      const expected = sign(scope, body);
      if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
        return undefined;
      }
      return Buffer.from(body, "base64url").toString("utf8");
    },
  };
}
