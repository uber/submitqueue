"use client";

import { WebApp } from "./web-app.js";
import type { SubmitQueuePageModel } from "../entity/index.js";
import { SubmitQueuePage, SubmitQueueShell, type SubmitQueueShellProps } from "./pages.js";
import { WebRefreshControl } from "./refresh-control.js";

/** The complete client experience for a loaded page: navigation, refresh, and polling through the host's `WebNavigationProvider`. */
export function SubmitQueueApp({ model, shell }: {
  model: SubmitQueuePageModel;
  shell?: Omit<SubmitQueueShellProps, "children" | "controls">;
}) {
  return <WebApp model={model}>
    <SubmitQueueShell homeHref={model.basePath || "/"} {...shell} controls={<WebRefreshControl refresh={model.refresh} />}>
      <SubmitQueuePage key={model.key} model={model} />
    </SubmitQueueShell>
  </WebApp>;
}
