"use client";

import { AutoRefresh, type AutoRefreshProps } from "@submitqueue/web-submitqueue";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useRef, useTransition } from "react";

export function NextRefresh({ refreshHref, ...props }: Omit<AutoRefreshProps, "refresh"> & { refreshHref?: string }) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  const completion = useRef<(() => void) | null>(null);
  const transitionStarted = useRef(false);
  const refresh = useCallback(() => new Promise<void>((resolve) => {
    completion.current = resolve;
    transitionStarted.current = false;
    startTransition(() => refreshHref ? router.replace(refreshHref) : router.refresh());
  }), [router, startTransition, refreshHref]);

  useEffect(() => {
    if (!completion.current) {
      return;
    }
    if (pending) {
      transitionStarted.current = true;
    } else if (transitionStarted.current) {
      completion.current();
      completion.current = null;
      transitionStarted.current = false;
    }
  }, [pending]);

  useEffect(() => () => {
    completion.current?.();
    completion.current = null;
  }, []);

  return <AutoRefresh {...props} refresh={refresh} />;
}
