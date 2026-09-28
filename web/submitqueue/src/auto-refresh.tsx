"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";

export interface AutoRefreshProps {
  terminal?: boolean;
  intervalMs?: number;
  jitterMs?: number;
  maxBackoffMs?: number;
  transientFailureCount?: number;
  refresh?: () => void | Promise<void>;
  random?: () => number;
}

function pageCanRefresh(): boolean {
  return document.visibilityState !== "hidden" && navigator.onLine;
}

export function AutoRefresh({
  terminal = false,
  intervalMs = 2_000,
  jitterMs = 500,
  maxBackoffMs = 30_000,
  transientFailureCount = 0,
  refresh,
  random = Math.random,
}: AutoRefreshProps) {
  const router = useRouter();
  const [refreshing, setRefreshing] = useState(false);
  const [paused, setPaused] = useState(false);
  const [cycle, setCycle] = useState(0);
  const inFlight = useRef(false);
  const localFailures = useRef(0);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const routerRefresh = useCallback(() => router.refresh(), [router]);
  const refreshPage = refresh ?? routerRefresh;

  const runRefresh = useCallback(async () => {
    if (inFlight.current || !pageCanRefresh()) {
      setPaused(!pageCanRefresh());
      return;
    }
    inFlight.current = true;
    setRefreshing(true);
    try {
      await refreshPage();
      localFailures.current = 0;
    } catch {
      localFailures.current += 1;
    } finally {
      inFlight.current = false;
      setRefreshing(false);
      setCycle((value) => value + 1);
    }
  }, [refreshPage]);

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
    if (terminal || paused) {
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
  }, [cycle, intervalMs, jitterMs, maxBackoffMs, paused, random, runRefresh, terminal, transientFailureCount]);

  return (
    <div className="sq-refresh" data-paused={paused || terminal}>
      <button disabled={refreshing || paused} onClick={() => void runRefresh()} type="button">
        {refreshing ? "Refreshing…" : "Refresh"}
      </button>
      <span aria-live="polite">
        {terminal ? "Automatic refresh stopped" : paused ? "Automatic refresh paused" : ""}
      </span>
    </div>
  );
}
