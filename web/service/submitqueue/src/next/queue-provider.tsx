"use client";

import type { ReactNode } from "react";
import { RequestPagesProvider, type RequestPageLoader } from "@submitqueue/web-submitqueue";

// A queue page `/queue/<queue>?…` reads its requests from `/api/queue/<queue>/requests?…`.
const loadPage: RequestPageLoader = async (href, signal) => {
  const continuation = new URL(href, window.location.origin);
  const response = await fetch(`/api${continuation.pathname}/requests${continuation.search}`, { signal, cache: "no-store" });
  if (!response.ok) throw new Error("Queue page unavailable");
  return response.json();
};

export function QueueProvider({ children }: { children: ReactNode }) {
  return <RequestPagesProvider loadPage={loadPage}>{children}</RequestPagesProvider>;
}
