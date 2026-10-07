import type { Metadata } from "next";
import { forbidden, notFound, redirect } from "next/navigation";
import { connection } from "next/server";
import { cache } from "react";
import { SubmitQueueApp } from "@submitqueue/web-submitqueue";
import type { SubmitQueueWeb, WebSearchParams } from "@submitqueue/web-submitqueue/server";
import { NextNavigationProvider } from "./navigation";

export interface NextPageProps {
  params: Promise<{ path?: string[] }>;
  searchParams: Promise<WebSearchParams>;
}

export interface NextPageOptions {
  /** The web UI, or a resolver called per request so configuration is read at runtime rather than build time. */
  web: SubmitQueueWeb | (() => SubmitQueueWeb);
  /**
   * Authenticates the request and returns the principal passed to the web UI.
   * It may call Next's `unauthorized()`; it runs before every load.
   */
  authenticate?: () => Promise<unknown>;
}

/**
 * The default export and `generateMetadata` for an optional catch-all route,
 * `app/[[...path]]/page.tsx`. Both share one load per request; the page maps
 * results to Next's redirect, not-found, and forbidden responses and renders
 * `SubmitQueueApp` with Next navigation.
 */
export function createNextPage(options: NextPageOptions) {
  // React's cache keys on primitives, so the request is passed as strings.
  const loadPage = cache(async (path: string, encodedSearch: string) => {
    await connection();
    const principal = await options.authenticate?.();
    const web = typeof options.web === "function" ? options.web() : options.web;
    return web.handle({
      path, search: JSON.parse(encodedSearch) as WebSearchParams,
      ...(principal === undefined ? {} : { principal }),
    });
  });

  async function load(props: NextPageProps) {
    const [{ path = [] }, search] = await Promise.all([props.params, props.searchParams]);
    return loadPage(`/${path.join("/")}`, JSON.stringify(search));
  }

  async function generateMetadata(props: NextPageProps): Promise<Metadata> {
    const result = await load(props);
    return result.kind === "render" ? { title: result.model.title } : {};
  }

  async function Page(props: NextPageProps) {
    const result = await load(props);
    switch (result.kind) {
      case "not-found":
        notFound();
      case "forbidden":
        forbidden();
      case "redirect":
        redirect(result.href);
      case "render":
        return <NextNavigationProvider><SubmitQueueApp model={result.model} /></NextNavigationProvider>;
    }
  }

  return { Page, generateMetadata };
}
