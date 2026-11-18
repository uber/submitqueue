import "server-only";

import { headers } from "next/headers";
import { getAppearanceColorScheme, getAppearanceTheme } from "@submitqueue/web-platform/appearance-theme";
import { readAppearanceMode } from "@submitqueue/web-platform/appearance-preference";

/** Adapts request preferences to the framework-neutral theme provider. */
export async function getAppearancePreferences() {
  const request = await headers();
  return {
    initialMode: readAppearanceMode(request.get("cookie") ?? ""),
    initialSystemMode: request.get("sec-ch-prefers-color-scheme") === "dark" ? "dark" as const : "light" as const,
  };
}

/** Colors the document canvas before streamed content or client JavaScript arrives. */
export function getAppearanceDocumentStyle({
  initialMode,
  initialSystemMode,
}: Awaited<ReturnType<typeof getAppearancePreferences>>) {
  const theme = getAppearanceTheme(initialMode, initialSystemMode);
  return {
    colorScheme: getAppearanceColorScheme(initialMode),
    backgroundColor: theme.colors.backgroundPrimary,
    color: theme.colors.contentPrimary,
  };
}
