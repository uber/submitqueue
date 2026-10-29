"use client";

import { useCallback, useEffect, useRef, useState } from "react";

/** Ignores late responses after a traversal is reset or unmounted. Each view owns its merge and refresh policy. */
export function usePageLoader<Page extends { nextHref: string | null }>({
  loadPage, nextHref, loadedHrefs, resetKey, onPage,
}: {
  loadPage: ((href: string, signal: AbortSignal) => Promise<Page>) | null | undefined;
  nextHref: string | null;
  loadedHrefs: readonly string[];
  /** Identity of the traversal; change it to discard an in-flight continuation. */
  resetKey: unknown;
  onPage(page: Page, href: string): void;
}) {
  const [status, setStatus] = useState({ key: resetKey, loading: false, error: false });
  const inFlight = useRef<AbortController | null>(null);
  const currentKey = useRef(resetKey);
  const onPageRef = useRef(onPage);
  currentKey.current = resetKey;
  onPageRef.current = onPage;

  useEffect(() => () => {
    inFlight.current?.abort();
    inFlight.current = null;
  }, [resetKey]);

  const cancelPageLoad = useCallback(() => {
    inFlight.current?.abort();
    inFlight.current = null;
    setStatus({ key: currentKey.current, loading: false, error: false });
  }, []);

  const loadNextPage = useCallback(async () => {
    const href = nextHref;
    if (!loadPage || !href || inFlight.current) return;
    const controller = new AbortController();
    inFlight.current = controller;
    setStatus({ key: resetKey, loading: true, error: false });
    try {
      const page = await loadPage(href, controller.signal);
      if (controller.signal.aborted || currentKey.current !== resetKey) return;
      if (page.nextHref && [href, ...loadedHrefs].includes(page.nextHref)) throw new Error("Repeated continuation");
      onPageRef.current(page, href);
    } catch {
      if (!controller.signal.aborted && currentKey.current === resetKey) {
        setStatus({ key: resetKey, loading: false, error: true });
      }
    } finally {
      if (inFlight.current === controller) {
        inFlight.current = null;
        setStatus(current => current.key === resetKey ? { ...current, loading: false } : current);
      }
    }
  }, [loadPage, nextHref, loadedHrefs, resetKey]);

  return {
    loading: status.key === resetKey && status.loading,
    error: status.key === resetKey && status.error,
    loadNextPage,
    cancelPageLoad,
  };
}

/** Preserves existing items and input arrays; duplicate incoming identities are appended only once. */
export function appendUniqueItems<T>(current: readonly T[], incoming: readonly T[], itemKey: (item: T) => string): T[] {
  const seen = new Set(current.map(itemKey));
  return [...current, ...incoming.filter(item => {
    const key = itemKey(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  })];
}
