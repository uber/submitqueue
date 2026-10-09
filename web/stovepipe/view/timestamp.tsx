"use client";

import { useSyncExternalStore } from "react";

const formatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "long",
});
const subscribe = () => () => {};

/** Displays Unix milliseconds in browser time; zero or invalid values are unknown. */
export function Timestamp({ value }: { value: string | number }) {
  const hydrated = useSyncExternalStore(subscribe, () => true, () => false);
  const date = new Date(Number(value));
  if (Number(value) <= 0 || Number.isNaN(date.getTime())) return <span>Unknown</span>;
  return <time dateTime={date.toISOString()}>{hydrated ? formatter.format(date) : "…"}</time>;
}
