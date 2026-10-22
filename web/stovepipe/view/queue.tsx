"use client";

import { createContext, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Block } from "baseui/block";
import { Button, KIND, SIZE } from "baseui/button";
import { Spinner } from "baseui/spinner";
import { ParagraphSmall, ParagraphXSmall } from "baseui/typography";
import type { QueueRequestsModel } from "../entity/index.js";
import { Link } from "./components.js";
import { RequestList } from "./pages.js";

const useBrowserLayoutEffect = typeof window === "undefined" ? useEffect : useLayoutEffect;

/** Fetches a continuation through the host's authenticated transport. */
export type QueuePageLoader = (href: string, signal: AbortSignal) => Promise<QueueRequestsModel>;

interface QueueSnapshot extends QueueRequestsModel {
  latest: QueueRequestsModel;
  firstPageIds: string[];
  loadedHrefs: string[];
}
interface SavedQueue {
  snapshot: QueueSnapshot;
  scrollY: number;
}
const QueueContext = createContext<{
  loadPage: QueuePageLoader;
  pages: Map<string, SavedQueue>;
} | null>(null);

/** Mount above route changes to retain loaded queue rows and scroll positions. */
export function StovepipeQueueProvider({ loadPage, children }: { loadPage: QueuePageLoader; children: ReactNode }) {
  const pages = useRef(new Map<string, SavedQueue>());
  const value = useMemo(() => ({ loadPage, pages: pages.current }), [loadPage]);
  return <QueueContext.Provider value={value}>{children}</QueueContext.Provider>;
}

function createQueueSnapshot(page: QueueRequestsModel): QueueSnapshot {
  return { ...page, latest: page, firstPageIds: page.requests.map(request => request.id), loadedHrefs: [] };
}

export function QueueRequests({ initial, routeKey }: { initial: QueueRequestsModel; routeKey: string }) {
  const context = useContext(QueueContext);
  const [snapshot, setSnapshot] = useState(() => context?.pages.get(routeKey)?.snapshot ?? createQueueSnapshot(initial));
  const [hydrated, setHydrated] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);
  const sentinel = useRef<HTMLElement | null>(null);
  const inFlight = useRef<AbortController | null>(null);
  const saved = useRef<SavedQueue>({ snapshot, scrollY: context?.pages.get(routeKey)?.scrollY ?? 0 });
  saved.current.snapshot = snapshot;

  if (snapshot.latest !== initial) {
    const current = new Map(initial.requests.map(request => [request.id, request]));
    setSnapshot({ ...snapshot, latest: initial, requests: snapshot.requests.map(request => current.get(request.id) ?? request) });
  }

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
      inFlight.current?.abort();
      context?.pages.set(routeKey, saved.current);
    };
  }, [context, routeKey]);

  const loadOlderRequests = useCallback(async () => {
    const href = snapshot.nextHref;
    if (!context || !href || inFlight.current) return;
    const controller = new AbortController();
    inFlight.current = controller;
    setLoading(true);
    setError(false);
    try {
      const next = await context.loadPage(href, controller.signal);
      if (controller.signal.aborted) return;
      if (next.nextHref && [href, ...snapshot.loadedHrefs].includes(next.nextHref)) throw new Error("Repeated continuation");
      setSnapshot(current => {
        if (current.nextHref !== href) return current;
        const existing = new Set(current.requests.map(request => request.id));
        const additions = next.requests.filter(request => {
          if (existing.has(request.id)) return false;
          existing.add(request.id);
          return true;
        });
        return { ...current, requests: [...current.requests, ...additions], nextHref: next.nextHref, loadedHrefs: [...current.loadedHrefs, href] };
      });
    } catch {
      if (!controller.signal.aborted) setError(true);
    } finally {
      if (inFlight.current === controller) {
        inFlight.current = null;
        setLoading(false);
      }
    }
  }, [context, snapshot.nextHref, snapshot.loadedHrefs]);

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

  const newRequests = initial.requests.some(request => !snapshot.firstPageIds.includes(request.id));
  return <>
    {newRequests && <Block marginBottom="scale600">
      <Button size={SIZE.compact} onClick={() => {
        inFlight.current?.abort();
        inFlight.current = null;
        setLoading(false);
        setError(false);
        setSnapshot(createQueueSnapshot(initial));
        window.scrollTo(0, 0);
      }}>New requests available · Show latest requests</Button>
    </Block>}
    <RequestList requests={snapshot.requests} />
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
            : <Link href={snapshot.nextHref}>Older requests →</Link>)}
        </Block>
      </Block>
    </section>
  </>;
}
