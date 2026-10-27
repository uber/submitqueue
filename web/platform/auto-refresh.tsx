"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { Theme } from "baseui";
import { Block } from "baseui/block";
import { Checkbox } from "baseui/checkbox";
import { LabelXSmall } from "baseui/typography";

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

  return (
    <Block display="flex" alignItems="center" flexWrap gridGap="scale500" data-paused={paused || terminal}>
      <Checkbox checked={enabled} disabled={terminal} onChange={event => setEnabled(event.currentTarget.checked)}
        overrides={{
          Root: { style: { alignItems: "center" } },
          Label: { style: ({ $theme }: { $theme: Theme }) => $theme.typography.LabelSmall },
        }}>
        Auto refresh every {intervalMs / 1_000}s
      </Checkbox>
      {enabled && <LabelXSmall as="span" color="contentSecondary" aria-live="polite">
        {terminal ? "Automatic refresh stopped" : paused ? "Automatic refresh paused" : refreshing ? "Refreshing…" :
          transientFailureCount > 0 || localFailures.current > 0 ? "Updates unavailable · retrying" : "Live"}
      </LabelXSmall>}
    </Block>
  );
}
