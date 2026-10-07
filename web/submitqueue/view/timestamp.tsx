"use client";

const timestampFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium", timeStyle: "long", timeZone: "UTC",
});

export function Timestamp({ value }: { value: number }) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? <time>{value}</time> :
    <time dateTime={date.toISOString()}>{timestampFormatter.format(date)}</time>;
}
