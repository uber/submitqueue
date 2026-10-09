"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useTransition, type ReactNode } from "react";
import { WebNavigationProvider, type WebLinkProps, type WebNavigation } from "@submitqueue/web-stovepipe";

// Prefetching dynamic pages would turn every visible link into a backend read.
function NextWebLink(props: WebLinkProps) {
  return <Link prefetch={false} {...props} />;
}

/**
 * Next.js navigation for a `WebApp`. A refresh re-renders the server page
 * inside a transition and settles only once that transition finishes, so
 * polling never overlaps an in-flight refresh.
 */
export function useNextNavigation(): WebNavigation {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  const completion = useRef<(() => void) | null>(null);
  const transitionStarted = useRef(false);

  const refresh = useCallback((href: string | null) => new Promise<void>((resolve) => {
    completion.current = resolve;
    transitionStarted.current = false;
    startTransition(() => href ? router.replace(href) : router.refresh());
  }), [router, startTransition]);

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

  return useMemo(() => ({
    Link: NextWebLink,
    push: (href: string) => router.push(href),
    refresh,
  }), [refresh, router]);
}

export function NextNavigationProvider({ children }: { children: ReactNode }) {
  return <WebNavigationProvider value={useNextNavigation()}>{children}</WebNavigationProvider>;
}
