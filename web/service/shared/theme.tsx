"use client";

import { BaseProvider, LightTheme } from "baseui";
import { useServerInsertedHTML } from "next/navigation";
import { useState, type ReactNode } from "react";
import { Client, Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";

export function ThemeProvider({ children }: { children: ReactNode }) {
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
  return <Provider value={engine}><BaseProvider theme={LightTheme}>{children}</BaseProvider></Provider>;
}
