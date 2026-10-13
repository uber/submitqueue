/**
 * A protobuf `int64` as produced by common TypeScript protobuf runtimes:
 * `bigint` (Protobuf-ES), `number`/`string`, or a `Long`-style object whose
 * `toString()` is its decimal value (protobufjs).
 */
export type Int64Value = bigint | number | string | { toString(): string };

/** Converts an `int64` to a number, or `null` when it is not a safe integer. */
export function safeInt64ToNumber(value: Int64Value): number | null {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? value : null;
  }
  let converted: bigint;
  if (typeof value === "bigint") {
    converted = value;
  } else {
    const decimal = typeof value === "string" ? value : value.toString();
    if (!/^-?\d+$/u.test(decimal)) {
      return null;
    }
    converted = BigInt(decimal);
  }
  if (converted > BigInt(Number.MAX_SAFE_INTEGER) || converted < BigInt(Number.MIN_SAFE_INTEGER)) {
    return null;
  }
  return Number(converted);
}
