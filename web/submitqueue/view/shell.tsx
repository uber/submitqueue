"use client";

import type { ReactNode } from "react";
import { SubmitQueueFrame } from "./theme.js";

export interface WebShellProps {
  children: ReactNode;
  homeHref?: string;
  /** Accessible name of the home link. */
  homeLabel?: string;
  brand?: ReactNode;
  /** Short monogram shown beside the brand; omitted when empty. */
  brandMark?: string;
  environmentLabel?: string;
  footer?: ReactNode;
}

export function WebShell({
  children, homeHref = "/", homeLabel, brand, brandMark = "", environmentLabel = "Read-only",
  footer = "Read-only · updates automatically",
}: WebShellProps) {
  return <SubmitQueueFrame className="site-frame">
    <header className="site-header">
      <a className="brand" href={homeHref} {...(homeLabel ? { "aria-label": homeLabel } : {})}>
        {brandMark ? <span className="brand-mark" aria-hidden="true">{brandMark}</span> : null}<span>{brand}</span>
      </a>
      {environmentLabel ? <span className="environment-pill">{environmentLabel}</span> : null}
    </header>
    {children}
    {footer ? <footer className="site-footer">{footer}</footer> : null}
  </SubmitQueueFrame>;
}
