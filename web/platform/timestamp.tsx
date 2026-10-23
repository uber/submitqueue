"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { Block } from "baseui/block";
import { Segment, SegmentedControl } from "baseui/segmented-control";
import { LabelSmall } from "baseui/typography";

const utcFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium", timeStyle: "long", timeZone: "UTC",
});
const preferenceKey = "submitqueue.time-zone";

const TimeZoneContext = createContext<{
  selection: "browser" | "utc";
  selectTimeZone(selection: "browser" | "utc"): void;
  browserTimeZone: string;
  formatter: Intl.DateTimeFormat;
} | null>(null);

/** Remembers the browser's selection across page loads; UTC is the server-rendering fallback. */
export function TimeZoneProvider({ children }: { children: ReactNode }) {
  const [selection, setSelection] = useState<"browser" | "utc">("browser");
  const [browserTimeZone, setBrowserTimeZone] = useState("UTC");
  useEffect(() => {
    setBrowserTimeZone(new Intl.DateTimeFormat().resolvedOptions().timeZone);
    try {
      if (window.localStorage.getItem(preferenceKey) === "utc") setSelection("utc");
    } catch {
      // Storage may be unavailable; the toggle still works for this page.
    }
  }, []);
  const selectTimeZone = useCallback((next: "browser" | "utc") => {
    setSelection(next);
    try {
      window.localStorage.setItem(preferenceKey, next);
    } catch {
      // Storage may be unavailable; keep the in-memory selection.
    }
  }, []);
  const formatter = useMemo(() => new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium", timeStyle: "long", timeZone: selection === "utc" ? "UTC" : browserTimeZone,
  }), [selection, browserTimeZone]);
  const value = useMemo(() => ({ selection, selectTimeZone, browserTimeZone, formatter }), [selection, selectTimeZone, browserTimeZone, formatter]);
  return <TimeZoneContext.Provider value={value}>{children}</TimeZoneContext.Provider>;
}

export function TimeZoneToggle() {
  const context = useContext(TimeZoneContext);
  if (!context) return null;
  return <Block display="flex" alignItems="center" flexWrap gridGap="scale300">
    <LabelSmall as="span">Time zone</LabelSmall>
    <SegmentedControl uid="time-zone" activeKey={context.selection}
      onChange={({ activeKey }) => context.selectTimeZone(activeKey === "utc" ? "utc" : "browser")}
      overrides={{ SegmentList: { props: { "aria-label": "Time zone" } } }}>
      <Segment key="browser" label="Browser" overrides={{ Segment: { props: { title: context.browserTimeZone } } }} />
      <Segment key="utc" label="UTC" />
    </SegmentedControl>
  </Block>;
}

export function Timestamp({ value }: { value: string | number }) {
  const formatter = useContext(TimeZoneContext)?.formatter ?? utcFormatter;
  const date = new Date(Number(value));
  return Number.isNaN(date.getTime()) ? <time>{value}</time> :
    <time dateTime={date.toISOString()}>{formatter.format(date)}</time>;
}
