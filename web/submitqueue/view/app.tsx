"use client";

import { SubmitQueueFrame } from "./theme.js";
import { WebApp } from "./web-app.js";
import type { SubmitQueuePageModel } from "../entity/index.js";
import { SubmitQueuePage } from "./pages.js";

/** The complete client experience for a loaded page: navigation, refresh, and polling through the host's `WebNavigationProvider`. */
export function SubmitQueueApp({ model }: { model: SubmitQueuePageModel }) {
  return <SubmitQueueFrame><WebApp model={model} Page={SubmitQueuePage} /></SubmitQueueFrame>;
}
