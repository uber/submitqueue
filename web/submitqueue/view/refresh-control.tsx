"use client";

import type { WebRefreshModel } from "../entity/index.js";
import { AutoRefresh } from "./auto-refresh.js";
import { useWebApp } from "./web-app.js";

/** Refresh button and polling status for the page rendered by the enclosing `WebApp`; renders nothing outside one. */
export function WebRefreshControl({ refresh }: { refresh: WebRefreshModel }) {
  const app = useWebApp();
  if (!app) {
    return null;
  }
  return <AutoRefresh terminal={refresh.terminal} transientFailureCount={refresh.transientFailureCount}
    automatic={app.automaticRefresh} refresh={app.refresh} />;
}
