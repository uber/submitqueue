/**
 * Percent-encodes one opaque path segment. Dot-only values and values that
 * already start with `~` gain a visible `~` prefix so browsers cannot
 * normalize them away; {@link decodePathSegment} reverses it.
 */
export function encodePathSegment(value: string): string {
  return value === "." || value === ".." || value.startsWith("~")
    ? `~${encodeURIComponent(value)}`
    : encodeURIComponent(value);
}

/** Removes the `~` escape from an already URL-decoded path segment. */
export function decodePathSegment(value: string): string {
  return value.startsWith("~") ? value.slice(1) : value;
}
