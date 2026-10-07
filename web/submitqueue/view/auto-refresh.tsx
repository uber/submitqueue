"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export interface AutoRefreshProps {
  terminal?: boolean;
  /** Whether to poll; when false only the manual button refreshes. */
  automatic?: boolean;
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
  automatic = true,
  intervalMs = 2_000,
  jitterMs = 500,
  maxBackoffMs = 30_000,
  transientFailureCount = 0,
  refresh,
  random = Math.random,
}: AutoRefreshProps) {
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
    if (terminal || paused || !automatic) {
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
  }, [automatic, cycle, intervalMs, jitterMs, maxBackoffMs, paused, random, runRefresh, terminal, transientFailureCount]);

  return (
    <div className="sq-refresh" data-paused={paused || terminal}>
      <button disabled={refreshing || paused} onClick={() => void runRefresh()} type="button">
        {refreshing ? "Refreshing…" : "Refresh"}
      </button>
      <span aria-live="polite">
        {terminal ? "Automatic refresh stopped" : !automatic ? "Manual refresh" : paused ? "Automatic refresh paused" :
          transientFailureCount > 0 || localFailures.current > 0 ? "Updates unavailable · retrying" : "Live"}
      </span>
    </div>
  );
}
