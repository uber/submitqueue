import { beforeEach, expect, it, vi } from "vitest";
import { DarkTheme, LightTheme } from "baseui";
import { getAppearanceDocumentStyle, getAppearancePreferences } from "./appearance.js";

const request = vi.hoisted(() => new Headers());
vi.mock("next/headers", () => ({ headers: async () => request }));

beforeEach(() => {
  for (const key of [...request.keys()]) request.delete(key);
});

it("supplies saved appearance and server-known system preference to both hosts", async () => {
  request.set("cookie", "other=1; submitqueue.appearance=dark");
  request.set("sec-ch-prefers-color-scheme", "light");
  expect(await getAppearancePreferences()).toEqual({ initialMode: "dark", initialSystemMode: "light" });
});

it("defaults invalid or absent preferences to System with a light SSR fallback", async () => {
  request.set("cookie", "submitqueue.appearance=invalid");
  expect(await getAppearancePreferences()).toEqual({ initialMode: "system", initialSystemMode: "light" });
  request.set("sec-ch-prefers-color-scheme", "dark");
  expect(await getAppearancePreferences()).toEqual({ initialMode: "system", initialSystemMode: "dark" });
});

it.each([
  ["dark", "light", DarkTheme, "dark"],
  ["light", "dark", LightTheme, "light"],
] as const)("paints the document for %s appearance and a %s system", (initialMode, initialSystemMode, theme, colorScheme) => {
  expect(getAppearanceDocumentStyle({ initialMode, initialSystemMode })).toEqual({
    colorScheme,
    backgroundColor: theme.colors.backgroundPrimary,
    color: theme.colors.contentPrimary,
  });
});

it.each(["light", "dark"] as const)("lets the browser paint System mode without a %s server hint", initialSystemMode => {
  expect(getAppearanceDocumentStyle({ initialMode: "system", initialSystemMode })).toEqual({
    colorScheme: "light dark",
    backgroundColor: `light-dark(${LightTheme.colors.backgroundPrimary}, ${DarkTheme.colors.backgroundPrimary})`,
    color: `light-dark(${LightTheme.colors.contentPrimary}, ${DarkTheme.colors.contentPrimary})`,
  });
});
