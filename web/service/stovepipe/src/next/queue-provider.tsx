"use client";

import type { ReactNode } from "react";
import { RequestPagesProvider, type RequestPageLoader } from "@submitqueue/web-stovepipe";

const loadPage: RequestPageLoader = async (href, signal) => {
  const continuation = new URL(href, window.location.origin);
  const search = new URLSearchParams({ path: continuation.pathname, page: continuation.searchParams.get("page") ?? "" });
  const response = await fetch(`/api/queue-requests?${search}`, { signal, cache: "no-store" });
  if (!response.ok) throw new Error("Queue page unavailable");
  return response.json();
};

export function QueueProvider({ children }: { children: ReactNode }) {
  return <RequestPagesProvider loadPage={loadPage}>{children}</RequestPagesProvider>;
}
