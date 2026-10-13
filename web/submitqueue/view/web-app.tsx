"use client";

import { createContext, useContext, useMemo, useRef, type ComponentType } from "react";
import type { WebPageModel } from "../entity/index.js";
import { useWebNavigation, type WebLinkProps } from "./navigation.js";

/** Client-side controls of the enclosing {@link WebApp}. */
export interface WebAppControls {
  /** Moves to an in-app href. */
  navigate(href: string): Promise<void>;
  /** Refreshes the current page; rejects when the refresh fails. */
  refresh(): Promise<void>;
  /** Whether the host refreshes without a document reload, so polling is worthwhile. */
  readonly automaticRefresh: boolean;
}

const AppContext = createContext<WebAppControls | null>(null);

export function useWebApp(): WebAppControls | null {
  return useContext(AppContext);
}

export interface WebAppProps<Model extends WebPageModel> {
  model: Model;
  Page: ComponentType<{ model: Model }>;
}

/**
 * Renders the host's current page model and gives its components navigation
 * and refresh through the host's `WebNavigationProvider`. The host owns the
 * model: a refresh or navigation re-renders `WebApp` with a new one.
 */
export function WebApp<Model extends WebPageModel>({ model, Page }: WebAppProps<Model>) {
  const navigation = useWebNavigation();
  const modelRef = useRef(model);
  modelRef.current = model;

  const controls = useMemo<WebAppControls>(() => ({
    automaticRefresh: navigation.refresh !== undefined,
    async navigate(href) {
      if (navigation.push) {
        navigation.push(href);
      } else {
        window.location.assign(href);
      }
    },
    async refresh() {
      const refreshHref = modelRef.current.refresh.refreshHref;
      if (navigation.refresh) {
        await navigation.refresh(refreshHref);
      } else {
        window.location.assign(refreshHref ?? `${window.location.pathname}${window.location.search}`);
      }
    },
  }), [navigation]);

  return <AppContext.Provider value={controls}><Page key={model.key} model={model} /></AppContext.Provider>;
}

/** An in-app link: the host's `Link` when supplied, otherwise a plain anchor. */
export function WebLink(props: WebLinkProps) {
  const { Link } = useWebNavigation();
  return Link ? <Link {...props} /> : <a {...props} />;
}
