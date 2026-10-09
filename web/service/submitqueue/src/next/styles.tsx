"use client";

import { BaseProvider, LightTheme } from "baseui";
import { useServerInsertedHTML } from "next/navigation";
import { useState, type ReactNode } from "react";
import { Client, Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";

/** Styletron expects one sheet per media query; streaming can produce several. */
export function hydrateReferenceStyles(): Client {
  const sheets = new Map<string, HTMLStyleElement>();
  for (const sheet of document.querySelectorAll<HTMLStyleElement>("style[data-submitqueue-styletron]")) {
    const key = `${sheet.media}/${sheet.getAttribute("data-hydrate") ?? ""}`;
    const previous = sheets.get(key);
    if (previous) {
      previous.textContent += sheet.textContent;
      sheet.remove();
    } else {
      sheets.set(key, sheet);
    }
  }
  return new Client({ prefix: "sq", hydrate: [...sheets.values()] });
}

/** The reference host owns its theme and the Styletron SSR/hydration engine. */
export function ReferenceStyleProvider({ children }: { children: ReactNode }) {
  const [engine] = useState(() => typeof window === "undefined"
    ? new Server({ prefix: "sq" })
    : hydrateReferenceStyles());
  const [emitted] = useState(() => new Map<string, number>());

  useServerInsertedHTML(() => {
    if (!(engine instanceof Server)) return null;
    return <>{engine.getStylesheets().map(({ css, attrs }) => {
      const key = `${attrs.media ?? ""}/${attrs["data-hydrate"] ?? ""}`;
      const offset = emitted.get(key) ?? 0;
      emitted.set(key, css.length);
      if (offset === css.length) return null;
      return <style key={`${key}/${offset}`} {...attrs} data-submitqueue-styletron=""
        dangerouslySetInnerHTML={{ __html: css.slice(offset) }} />;
    })}</>;
  });

  return <Provider value={engine}><BaseProvider theme={LightTheme}>{children}</BaseProvider></Provider>;
}
