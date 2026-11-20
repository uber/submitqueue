"use client";

import { AppearanceProvider, useAppearance, type AppearanceProviderProps } from "@submitqueue/web-platform";
import { getAppearanceColorScheme, getAppearanceTheme } from "@submitqueue/web-platform/appearance-theme";
import { useServerInsertedHTML } from "next/navigation";
import { useLayoutEffect, useState, type ReactNode } from "react";
import { BaseProvider, useStyletron } from "baseui";
import { Block } from "baseui/block";
import { Client, Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";

function DocumentTheme() {
  const [, theme] = useStyletron();
  const { mode } = useAppearance();
  useLayoutEffect(() => {
    // Keep the host's initial document canvas in sync with later theme changes.
    Object.assign(document.documentElement.style, {
      colorScheme: getAppearanceColorScheme(mode),
      backgroundColor: theme.colors.backgroundPrimary,
      color: theme.colors.contentPrimary,
    });
  }, [mode, theme]);
  return null;
}

function ApplicationTheme({ children }: { children: ReactNode }) {
  const { mode, resolvedMode } = useAppearance();
  return <BaseProvider theme={getAppearanceTheme(mode, resolvedMode)}>
    <DocumentTheme />
    <Block minHeight="100vh" backgroundColor="backgroundPrimary" color="contentPrimary"
      overrides={{ Block: { style: { colorScheme: getAppearanceColorScheme(mode) } } }}>{children}</Block>
  </BaseProvider>;
}

export function ThemeProvider({ children, ...appearance }: AppearanceProviderProps) {
  const [engine] = useState(() => typeof window === "undefined"
    ? new Server()
    : new Client({ hydrate: document.querySelectorAll("style.sq-styletron") }));
  const [emitted] = useState(() => new Map<string, number>());
  useServerInsertedHTML(() => {
    if (!(engine instanceof Server)) return null;
    return <>{engine.getStylesheets().map(({ css, attrs }) => {
      const key = `${attrs["data-hydrate"] ?? "atomic"}:${attrs.media ?? ""}`;
      const offset = emitted.get(key) ?? 0;
      emitted.set(key, css.length);
      // Streaming renders append rules; emit only the new suffix at each flush.
      return css.length === offset ? null : <style key={key} {...attrs} className="sq-styletron"
        dangerouslySetInnerHTML={{ __html: css.slice(offset) }} />;
    })}</>;
  });
  return <Provider value={engine}><AppearanceProvider {...appearance}><ApplicationTheme>{children}</ApplicationTheme></AppearanceProvider></Provider>;
}
