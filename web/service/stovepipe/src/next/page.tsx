import { forbidden, notFound, redirect } from "next/navigation";
import { requireAuthorization } from "./authorize";
import { resolveHost } from "../server/host";
import { StovepipeApp } from "@submitqueue/web-stovepipe";
import { NextNavigationProvider } from "./navigation";
export async function StovepipePage({ params, searchParams }: { params: Promise<{ path?: string[] }>; searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  await requireAuthorization();
  const [{ path = [] }, values] = await Promise.all([params, searchParams]);
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (typeof value === "string") search.set(key, value);
    else if (Array.isArray(value)) for (const item of value) search.append(key, item);
  }
  // Next preserves encoded slashes in catch-all params; the module decodes each segment once.
  const result = await resolveHost().handle({ path: `/${path.join("/")}`, search });
  if (result.kind === "not-found") notFound();
  if (result.kind === "forbidden") forbidden();
  if (result.kind === "redirect") redirect(result.href);
  if (result.kind === "project-json" || result.kind === "unavailable") notFound();
  return <NextNavigationProvider><StovepipeApp model={result.model} /></NextNavigationProvider>;
}
