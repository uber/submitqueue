"use client";

import { createContext, useCallback, useContext, useEffect, useId, useMemo, useRef, useState, type ReactNode } from "react";
import { styled, withStyle, type Theme } from "baseui";
import { Block } from "baseui/block";
import { Delete } from "baseui/icon";
import { Popover, PLACEMENT, type PopoverOverrides } from "baseui/popover";
import { ALIGN, Radio, RadioGroup, type RadioOverrides } from "baseui/radio";
import { LabelMedium } from "baseui/typography";

const utcFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium", timeStyle: "long", timeZone: "UTC",
});
const preferenceKey = "submitqueue.time-zone";

const TimeZoneContext = createContext<{
  selection: "browser" | "utc";
  selectTimeZone(selection: "browser" | "utc"): void;
  browserTimeZone: string;
  browserFormatter: Intl.DateTimeFormat;
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
      // Storage may be unavailable; the controls still work for this page.
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
  const browserFormatter = useMemo(() => new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium", timeStyle: "long", timeZone: browserTimeZone,
  }), [browserTimeZone]);
  const formatter = selection === "utc" ? utcFormatter : browserFormatter;
  const value = useMemo(() => ({ selection, selectTimeZone, browserTimeZone, browserFormatter, formatter }),
    [selection, selectTimeZone, browserTimeZone, browserFormatter, formatter]);
  return <TimeZoneContext.Provider value={value}>{children}</TimeZoneContext.Provider>;
}

const timeZoneRadioOverrides: RadioOverrides = {
  Label: { style: ({ $theme }: { $theme: Theme }) => $theme.typography.ParagraphSmall },
  RadioMarkOuter: { style: ({ $theme }: { $theme: Theme }) => ({
    width: $theme.sizing.scale600, height: $theme.sizing.scale600,
  }) },
  Root: { style: ({ $theme, $align }: { $theme: Theme; $align?: string }) => ({
    marginTop: 0, marginBottom: 0,
    marginRight: $align === "horizontal" ? $theme.sizing.scale600 : 0,
    minHeight: $theme.sizing.scale850,
    "@media (pointer: coarse)": { minHeight: "44px" },
  }) },
};

const timestampPopoverCorners = {
  borderTopLeftRadius: "10px", borderTopRightRadius: "10px",
  borderBottomLeftRadius: "10px", borderBottomRightRadius: "10px",
};

const timestampPopoverOverrides: PopoverOverrides = {
  Body: { style: { zIndex: 1000, backgroundColor: "transparent", ...timestampPopoverCorners } },
  Inner: { style: ({ $theme }: { $theme: Theme }) => ({
    backgroundColor: $theme.colors.backgroundPrimary,
    color: $theme.colors.contentPrimary,
    // Portaled controls cannot inherit the appearance provider's color scheme.
    colorScheme: $theme.name?.includes("dark") ? "dark" : "light",
    ...timestampPopoverCorners,
    border: `1px solid ${$theme.colors.borderOpaque}`,
    maxHeight: "calc(100dvh - 24px)", overflowY: "auto",
  }) },
};

const IconButton = styled("button", ({ $theme }) => ({
  display: "inline-flex", alignItems: "center", justifyContent: "center", flexShrink: 0,
  boxSizing: "border-box", width: "28px", height: "28px", padding: 0, border: 0,
  borderRadius: "4px", backgroundColor: "transparent", color: $theme.colors.contentSecondary,
  cursor: "pointer",
  ":hover": { color: $theme.colors.contentPrimary },
  ":focus-visible": { outline: `2px solid ${$theme.colors.borderSelected}`, outlineOffset: "2px" },
  "@media (pointer: coarse)": { width: "44px", height: "44px" },
}));

const TimestampButton = styled("button", ({ $theme }) => ({
  display: "inline", border: 0, borderRadius: "2px", padding: 0, backgroundColor: "transparent",
  color: "inherit", fontFamily: "inherit", fontSize: "inherit", fontWeight: "inherit", lineHeight: "inherit",
  textAlign: "inherit", cursor: "pointer",
  whiteSpace: "normal", fontVariantNumeric: "tabular-nums", textDecoration: "underline dotted",
  textUnderlineOffset: "3px",
  ":hover": { color: $theme.colors.linkText },
  ":focus-visible": { outline: `2px solid ${$theme.colors.borderSelected}`, outlineOffset: "3px" },
}));

const Panel = styled("div", ({ $theme }) => ({
  ...$theme.typography.ParagraphSmall, boxSizing: "border-box",
  width: "min(340px, calc(100vw - 24px))", padding: "12px", color: $theme.colors.contentPrimary,
}));
const Values = styled("dl", { margin: "10px 0" });
const ValueGroup = styled("div", ({ $theme }) => ({
  ":not(:first-child)": { marginTop: "8px", paddingTop: "6px", borderTop: `1px solid ${$theme.colors.borderOpaque}` },
}));
const ValueLabel = styled("dt", ({ $theme }) => ({
  ...$theme.typography.ParagraphXSmall, marginBottom: "2px",
  color: $theme.colors.contentSecondary, fontWeight: 500,
}));
const Value = styled("dd", { margin: 0, overflowWrap: "anywhere" });
const IsoValue = withStyle(Value, { display: "flex", alignItems: "center", gap: "6px" });
const IsoText = styled("code", ({ $theme }) => ({
  ...$theme.typography.MonoParagraphXSmall, overflowWrap: "anywhere",
}));
const Divider = styled("hr", ({ $theme }) => ({
  margin: "10px 0 8px", border: 0, borderTop: `1px solid ${$theme.colors.borderOpaque}`,
}));
const TimeZoneLabel = styled("div", ({ $theme }) => ({
  ...$theme.typography.ParagraphXSmall, padding: "0 0 4px",
  color: $theme.colors.contentSecondary, fontWeight: 500,
}));
const ScopeNote = styled("p", ({ $theme }) => ({
  margin: "6px 0 0", color: $theme.colors.contentSecondary, fontSize: "11px", lineHeight: "16px",
}));
const CopyStatus = styled<"p", { $error: boolean }>("p", ({ $theme, $error }) => $error ? ({
  ...$theme.typography.ParagraphXSmall, color: $theme.colors.contentNegative, margin: "6px 0",
}) : ({
  position: "absolute", width: "1px", height: "1px", padding: 0, margin: "-1px",
  overflow: "hidden", clipPath: "inset(50%)", whiteSpace: "nowrap",
}));

function TimestampPopover({ date, context }: {
  date: Date;
  context: NonNullable<React.ContextType<typeof TimeZoneContext>>;
}) {
  const [open, setOpen] = useState(false);
  const [copyStatus, setCopyStatus] = useState("");
  const trigger = useRef<HTMLButtonElement>(null);
  const panelId = useId();
  const isoTimestamp = date.toISOString();
  const formatted = context.formatter.format(date);

  useEffect(() => {
    if (!open || copyStatus !== "Copied") return;
    const timer = window.setTimeout(() => setCopyStatus(""), 2000);
    return () => window.clearTimeout(timer);
  }, [open, copyStatus]);

  function closePopover(restoreFocus = false) {
    setOpen(false);
    if (restoreFocus) trigger.current?.focus({ preventScroll: true });
  }

  async function copyTimestamp() {
    try {
      await navigator.clipboard.writeText(isoTimestamp);
      setCopyStatus("Copied");
    } catch {
      setCopyStatus("Unable to copy. Select the ISO timestamp above to copy it.");
    }
  }

  return <Popover
    isOpen={open}
    accessibilityType="none"
    autoFocus
    focusLock={false}
    focusOptions={{ preventScroll: true }}
    placement={PLACEMENT.bottomLeft}
    onClick={event => {
      trigger.current = event.currentTarget as HTMLButtonElement;
      setCopyStatus("");
      setOpen(!open);
    }}
    onClickOutside={() => closePopover()}
    onEsc={() => closePopover(true)}
    popperOptions={{ modifiers: {
      preventOverflow: { boundariesElement: "viewport", padding: 12 },
      flip: { boundariesElement: "viewport", padding: 12 },
      // A wide table can scroll the anchor offscreen; keep the popover in the viewport.
      keepTogether: { enabled: false },
    } }}
    overrides={timestampPopoverOverrides}
    content={<Panel id={panelId} role="dialog" aria-modal="false" aria-labelledby={`${panelId}-title`}
      aria-describedby={`${panelId}-scope`} onBlur={event => {
        const next = event.relatedTarget;
        if (next instanceof Node && !event.currentTarget.contains(next) && !trigger.current?.contains(next)) closePopover();
      }}>
      <Block display="flex" alignItems="center" justifyContent="space-between" gridGap="scale500">
        <LabelMedium as="h2" id={`${panelId}-title`} margin="0">Timestamp</LabelMedium>
        <IconButton type="button" aria-label="Close timestamp options" onClick={() => closePopover(true)}>
          <Delete size={28} title="" aria-hidden="true" />
        </IconButton>
      </Block>
      <Values>
        <ValueGroup><ValueLabel>Local · {context.browserTimeZone}</ValueLabel><Value>{context.browserFormatter.format(date)}</Value></ValueGroup>
        <ValueGroup><ValueLabel>UTC</ValueLabel><Value>{utcFormatter.format(date)}</Value></ValueGroup>
        <ValueGroup>
          <ValueLabel>ISO 8601</ValueLabel>
          <IsoValue>
            <IsoText>{isoTimestamp}</IsoText>
            <IconButton type="button" aria-label={copyStatus === "Copied" ? "Copied" : "Copy ISO timestamp"}
              title={copyStatus === "Copied" ? "Copied" : "Copy ISO timestamp"} onClick={() => void copyTimestamp()}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                {copyStatus === "Copied" ? <path d="m5 12 4 4 10-10" />
                  : <><rect x="9" y="9" width="12" height="12" rx="2" /><path d="M5 15H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v1" /></>}
              </svg>
            </IconButton>
          </IsoValue>
        </ValueGroup>
      </Values>
      <CopyStatus $error={Boolean(copyStatus && copyStatus !== "Copied")} role="status">{copyStatus}</CopyStatus>
      <Divider />
      <TimeZoneLabel id={`${panelId}-timezone-label`}>Display timezone</TimeZoneLabel>
      <RadioGroup name={`${panelId}-timezone`} aria-labelledby={`${panelId}-timezone-label`} align={ALIGN.horizontal}
        value={context.selection} onChange={event => context.selectTimeZone(event.currentTarget.value as "browser" | "utc")}>
        <Radio value="browser" overrides={timeZoneRadioOverrides}>Local</Radio>
        <Radio value="utc" overrides={timeZoneRadioOverrides}>UTC</Radio>
      </RadioGroup>
      <ScopeNote id={`${panelId}-scope`}>Applies to all timestamps.</ScopeNote>
    </Panel>}
  >
    <TimestampButton type="button" aria-label={`${formatted} — timestamp options`}
      aria-haspopup="dialog" aria-expanded={open} aria-controls={open ? panelId : undefined}>
      <time dateTime={isoTimestamp}>{formatted}</time>
    </TimestampButton>
  </Popover>;
}

export function Timestamp({ value }: { value: string | number }) {
  const context = useContext(TimeZoneContext);
  const date = new Date(Number(value));
  if (Number.isNaN(date.getTime())) return <time>{value}</time>;
  return context ? <TimestampPopover date={date} context={context} />
    : <time dateTime={date.toISOString()}>{utcFormatter.format(date)}</time>;
}
