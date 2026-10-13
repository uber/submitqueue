"use client";

import { createContext, useContext, type AnchorHTMLAttributes, type ComponentType, type ReactNode } from "react";

export type WebLinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & { href: string };

/**
 * Host-supplied navigation, usually backed by the host framework's router.
 * Every member is optional: with none, links and refreshes are ordinary
 * document navigations and polling is manual.
 */
export interface WebNavigation {
  /** Renders in-app links, e.g. a framework router link. */
  Link?: ComponentType<WebLinkProps>;
  /** Moves to an in-app href. */
  push?: (href: string) => void;
  /** Re-renders the current page with a fresh model, or moves to `href`; settles once rendered. Enables polling. */
  refresh?: (href: string | null) => Promise<void>;
}

const NavigationContext = createContext<WebNavigation>({});

export function WebNavigationProvider({ value, children }: { value: WebNavigation; children: ReactNode }) {
  return <NavigationContext.Provider value={value}>{children}</NavigationContext.Provider>;
}

export function useWebNavigation(): WebNavigation {
  return useContext(NavigationContext);
}
