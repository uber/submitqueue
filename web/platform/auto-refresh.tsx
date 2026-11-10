"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { styled, type Theme } from "baseui";
import { Block } from "baseui/block";
import { Button, KIND, SHAPE, SIZE } from "baseui/button";
import { StatefulTooltip } from "baseui/tooltip";

const RefreshIcon = styled<"svg", { $spinning: boolean }>("svg", ({ $spinning }) => ({
  animationName: $spinning ? { from: { transform: "rotate(0deg)" }, to: { transform: "rotate(360deg)" } } : "none",
  animationDuration: "1s", animationIterationCount: "infinite", animationTimingFunction: "linear",
  "@media (prefers-reduced-motion: reduce)": { animationName: "none" },
}));

export interface AutoRefreshProps {
  terminal?: boolean;
  /** Whether the host supports refreshing in place. */
  available?: boolean;
  intervalMs?: number;
  jitterMs?: number;
  maxBackoffMs?: number;
  transientFailureCount?: number;
  refresh: () => void | Promise<void>;
  random?: () => number;
}

function pageCanRefresh(): boolean {
  return document.visibilityState !== "hidden" && navigator.onLine;
}

export function AutoRefresh({
  terminal = false,
  available = true,
  intervalMs = 10_000,
  jitterMs = 0,
  maxBackoffMs = 30_000,
  transientFailureCount = 0,
  refresh,
  random = Math.random,
}: AutoRefreshProps) {
  const [enabled, setEnabled] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [paused, setPaused] = useState(false);
  const [cycle, setCycle] = useState(0);
  const inFlight = useRef(false);
  const localFailures = useRef(transientFailureCount);
  const latestTransientFailureCount = useRef(transientFailureCount);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  latestTransientFailureCount.current = transientFailureCount;

  const runRefresh = useCallback(async () => {
    if (inFlight.current || !pageCanRefresh()) {
      setPaused(!pageCanRefresh());
      return;
    }
    inFlight.current = true;
    setRefreshing(true);
    try {
      await refresh();
      const serverFailures = latestTransientFailureCount.current;
      localFailures.current =
        serverFailures === 0
          ? 0
          : Math.max(localFailures.current + 1, serverFailures);
    } catch {
      localFailures.current = Math.max(
        localFailures.current + 1,
        latestTransientFailureCount.current,
      );
    } finally {
      inFlight.current = false;
      setRefreshing(false);
      setCycle((value) => value + 1);
    }
  }, [refresh]);

  useEffect(() => {
    const updateAvailability = () => setPaused(!pageCanRefresh());
    document.addEventListener("visibilitychange", updateAvailability);
    window.addEventListener("online", updateAvailability);
    window.addEventListener("offline", updateAvailability);
    updateAvailability();
    return () => {
      document.removeEventListener("visibilitychange", updateAvailability);
      window.removeEventListener("online", updateAvailability);
      window.removeEventListener("offline", updateAvailability);
    };
  }, []);

  useEffect(() => {
    if (terminal || paused || !available || !enabled) {
      return;
    }
    const failures = Math.max(transientFailureCount, localFailures.current);
    const multiplier = 2 ** Math.min(failures, 20);
    const backoff = Math.min(intervalMs * multiplier, maxBackoffMs);
    const delay = backoff + Math.floor(random() * (jitterMs + 1));
    timer.current = setTimeout(() => {
      void runRefresh();
    }, delay);
    return () => {
      if (timer.current !== null) {
        clearTimeout(timer.current);
        timer.current = null;
      }
    };
  }, [available, enabled, cycle, intervalMs, jitterMs, maxBackoffMs, paused, random, runRefresh, terminal, transientFailureCount]);

  if (!available) return null;

  const action = terminal ? "Auto-refresh stopped" : enabled ? "Disable auto-refresh" : "Enable auto-refresh";
  const failed = transientFailureCount > 0 || localFailures.current > 0;
  const quiet = !terminal && !paused && !failed;
  return (
    <Block display="flex" alignItems="center" flexWrap gridGap="scale300" data-paused={paused || terminal}>
      <StatefulTooltip content={action} placement="bottomRight" onMouseEnterDelay={150} onMouseLeaveDelay={0}>
        <Button type="button" kind={KIND.tertiary} shape={SHAPE.circle} size={SIZE.mini}
          isSelected={enabled && !terminal} disabled={terminal}
          aria-label={action} aria-pressed={enabled && !terminal} aria-busy={refreshing}
          onClick={() => setEnabled(current => !current)}
          overrides={{ BaseButton: { style: ({ $theme, $isSelected, $isFocusVisible }: {
            $theme: Theme; $isSelected?: boolean; $isFocusVisible?: boolean;
          }) => ({
            backgroundColor: $isSelected ? $theme.colors.backgroundAccentLight : "transparent",
            color: $isSelected ? $theme.colors.contentAccent : $theme.colors.contentSecondary,
            boxShadow: $isFocusVisible ? `inset 0 0 0 2px ${$theme.colors.borderAccent}` : "none",
            ":hover": {
              backgroundColor: $isSelected ? $theme.colors.backgroundAccentLight : $theme.colors.backgroundSecondary,
              color: $isSelected ? $theme.colors.contentAccent : $theme.colors.contentPrimary,
            },
            "@media (pointer: coarse)": { width: "44px", height: "44px" },
          }) } }}>
          <RefreshIcon $spinning={refreshing} width="16" height="16" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
            <path d="M21 3v5h-5M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16M8 16H3v5" />
          </RefreshIcon>
        </Button>
      </StatefulTooltip>
      {enabled && <Block as="span" font="ParagraphXSmall" color="contentSecondary" role="status"
        $style={quiet ? {
          position: "absolute", width: "1px", height: "1px", padding: 0, margin: "-1px",
          overflow: "hidden", clipPath: "inset(50%)", whiteSpace: "nowrap",
        } : {}}>
        {terminal ? "Automatic refresh stopped" : paused ? "Automatic refresh paused" : refreshing ? "Refreshing…" :
          failed ? "Updates unavailable · retrying" : "Live"}
      </Block>}
    </Block>
  );
}
