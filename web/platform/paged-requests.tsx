"use client";

import { createContext, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Block } from "baseui/block";
import { Button, KIND, SIZE } from "baseui/button";
import { StyledLink } from "baseui/link";
import { Spinner } from "baseui/spinner";
import { ParagraphSmall, ParagraphXSmall } from "baseui/typography";
import { useWebNavigation } from "./navigation.js";
import { appendUniqueItems, usePageLoader } from "./use-page-loader.js";

const useBrowserLayoutEffect = typeof window === "undefined" ? useEffect : useLayoutEffect;

/** One page of a request list ordered newest first. */
export interface RequestPage<T> {
  /** Requests on this page, newest first. */
  requests: T[];
  /** Opaque in-app URL of the next page; null on the final page. */
  nextHref: string | null;
}

/** Fetches a continuation through the host's authenticated transport. */
export type RequestPageLoader = (href: string, signal: AbortSignal) => Promise<RequestPage<unknown>>;

interface RequestSnapshot<T> extends RequestPage<T> {
  latest: RequestPage<T>;
  firstPageKeys: string[];
  loadedHrefs: string[];
}
interface SavedRequests {
  snapshot: RequestSnapshot<unknown>;
  scrollY: number;
}
const RequestPagesContext = createContext<{
  loadPage: RequestPageLoader;
  pages: Map<string, SavedRequests>;
} | null>(null);

/** Mount above route changes to retain loaded request rows and scroll positions. */
export function RequestPagesProvider({ loadPage, children }: { loadPage: RequestPageLoader; children: ReactNode }) {
  const pages = useRef(new Map<string, SavedRequests>());
  const value = useMemo(() => ({ loadPage, pages: pages.current }), [loadPage]);
  return <RequestPagesContext.Provider value={value}>{children}</RequestPagesContext.Provider>;
}

function createSnapshot<T>(page: RequestPage<T>, requestKey: (request: T) => string): RequestSnapshot<T> {
  return { ...page, latest: page, firstPageKeys: page.requests.map(requestKey), loadedHrefs: [] };
}

/**
 * Renders `initial` and appends older pages as the viewer scrolls near the end. A new `initial` updates displayed
 * rows in place; arrivals stay behind an explicit action. Without a provider, continuation links navigate normally.
 */
export function PagedRequests<T>({ initial, routeKey, requestKey, children }: {
  initial: RequestPage<T>;
  /** Identity of the list for retained rows and scroll position; independent of refresh time. */
  routeKey: string;
  requestKey: (request: T) => string;
  /** `complete` is true once no older page remains. */
  children: (requests: T[], complete: boolean) => ReactNode;
}) {
  const context = useContext(RequestPagesContext);
  const { Link: HostLink } = useWebNavigation();
  const [snapshot, setSnapshot] = useState(() =>
    (context?.pages.get(routeKey)?.snapshot as RequestSnapshot<T> | undefined) ?? createSnapshot(initial, requestKey));
  const [hydrated, setHydrated] = useState(false);
  const sentinel = useRef<HTMLElement | null>(null);
  const saved = useRef<SavedRequests>({ snapshot, scrollY: context?.pages.get(routeKey)?.scrollY ?? 0 });
  saved.current.snapshot = snapshot;

  if (snapshot.latest !== initial) {
    const current = new Map(initial.requests.map(request => [requestKey(request), request]));
    setSnapshot({ ...snapshot, latest: initial, requests: snapshot.requests.map(request => current.get(requestKey(request)) ?? request) });
  }

  const { loading, error, loadNextPage: loadOlderRequests, cancelPageLoad } = usePageLoader({
    loadPage: context?.loadPage as ((href: string, signal: AbortSignal) => Promise<RequestPage<T>>) | undefined,
    nextHref: snapshot.nextHref,
    loadedHrefs: snapshot.loadedHrefs,
    resetKey: routeKey,
    onPage: (next, href) => setSnapshot(current => current.nextHref !== href ? current : {
      ...current,
      requests: appendUniqueItems(current.requests, next.requests, requestKey),
      nextHref: next.nextHref,
      loadedHrefs: [...current.loadedHrefs, href],
    }),
  });

  useBrowserLayoutEffect(() => {
    setHydrated(true);
    const previous = context?.pages.get(routeKey);
    const frame = previous ? requestAnimationFrame(() => window.scrollTo(0, previous.scrollY)) : null;
    const recordScroll = () => {
      saved.current.scrollY = window.scrollY;
    };
    window.addEventListener("scroll", recordScroll, { passive: true });
    return () => {
      if (frame !== null) cancelAnimationFrame(frame);
      window.removeEventListener("scroll", recordScroll);
      // Save before navigation can shrink the document and reset its scroll offset.
      context?.pages.set(routeKey, saved.current);
    };
  }, [context, routeKey]);

  useEffect(() => {
    if (!context || !snapshot.nextHref || error || loading || !sentinel.current || typeof IntersectionObserver === "undefined") return;
    const observer = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting)) void loadOlderRequests();
    }, { rootMargin: "400px" });
    observer.observe(sentinel.current);
    return () => observer.disconnect();
  }, [context, snapshot.nextHref, error, loading, loadOlderRequests]);

  let paginationMessage: string;
  if (loading) {
    paginationMessage = "Loading older requests…";
  } else if (error) {
    paginationMessage = "Loading paused";
  } else if (snapshot.nextHref && context && hydrated && typeof IntersectionObserver !== "undefined") {
    paginationMessage = "Scroll for more requests";
  } else if (snapshot.nextHref) {
    paginationMessage = "More requests available";
  } else if (snapshot.requests.length) {
    paginationMessage = "All available requests loaded";
  } else {
    paginationMessage = "No requests to load";
  }

  const newRequests = initial.requests.some(request => !snapshot.firstPageKeys.includes(requestKey(request)));
  return <>
    {newRequests && <Block marginBottom="scale600">
      <Button size={SIZE.compact} onClick={() => {
        cancelPageLoad();
        setSnapshot(createSnapshot(initial, requestKey));
        window.scrollTo(0, 0);
      }}>New requests available · Show latest requests</Button>
    </Block>}
    {children(snapshot.requests, snapshot.nextHref === null)}
    <section ref={sentinel} aria-label="Queue pagination">
      {error && <ParagraphSmall role="alert" color="contentNegative">Could not load older requests. Your displayed requests are unchanged.</ParagraphSmall>}
      <Block display="flex" flexDirection="column" alignItems="center" gridGap="scale400" paddingTop="scale600" paddingBottom="scale600">
        <Block role="status" display="flex" flexDirection="column" alignItems="center" gridGap="scale300">
          <Block display="flex" alignItems="center" gridGap="scale300" height="scale900">
            <Block width="scale800" height="scale800" display="flex" alignItems="center" justifyContent="center" $style={{ flexShrink: 0 }}>
              {loading && <Block role="progressbar" aria-label="Loading older requests" aria-busy="true" $style={{
                animationName: { from: { opacity: 0 }, to: { opacity: 1 } },
                animationDuration: "160ms",
                // Fast batches finish before the indicator becomes visible.
                animationDelay: "120ms",
                animationFillMode: "backwards",
                animationTimingFunction: "ease-out",
                "@media (prefers-reduced-motion: reduce)": { animationName: "none" },
              }}>
                <Spinner role="presentation" aria-hidden="true" $size="20px" $borderWidth="2px" $style={{
                  "@media (prefers-reduced-motion: reduce)": { animationName: "none" },
                }} />
              </Block>}
            </Block>
            <ParagraphSmall margin="0" color="contentSecondary">
              {paginationMessage}
            </ParagraphSmall>
          </Block>
          <ParagraphXSmall margin="0" color="contentSecondary">{snapshot.requests.length} displayed requests</ParagraphXSmall>
        </Block>
        <Block minHeight="scale1000">
          {snapshot.nextHref && (context && hydrated
            ? <Button kind={KIND.tertiary} size={SIZE.compact} aria-disabled={loading} aria-busy={loading} onClick={() => void loadOlderRequests()}>{error ? "Retry loading older requests" : "Load more requests"}</Button>
            : <StyledLink $as={HostLink ?? "a"} href={snapshot.nextHref}>Older requests →</StyledLink>)}
        </Block>
      </Block>
    </section>
  </>;
}
