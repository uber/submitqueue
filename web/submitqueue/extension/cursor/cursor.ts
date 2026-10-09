/**
 * Turns server-side continuation state into an opaque, tamper-evident URL
 * value and back. A cursor is bound to `scope`: decoding it under any other
 * scope, or decoding a modified cursor, yields `undefined`. Either method may
 * be asynchronous so a codec can use remote key management or WebCrypto.
 */
export interface CursorCodec {
  encode(scope: string, payload: string): string | Promise<string>;
  decode(scope: string, cursor: string): string | undefined | Promise<string | undefined>;
}
