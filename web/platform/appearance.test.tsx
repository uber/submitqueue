import { afterEach, expect, it, vi } from "vitest";
import { BaseProvider, createTheme, useStyletron } from "baseui";
import { renderToString } from "react-dom/server";
import { hydrateRoot } from "react-dom/client";
import { Client, Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";
import { act, cleanup, fireEvent, render, screen } from "./test/rtl.js";
import { AppearanceControl, AppearanceProvider, useAppearance } from "./appearance.js";
import { appearanceCookieName, readAppearanceMode } from "./appearance-preference.js";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  document.cookie = `${appearanceCookieName}=; Path=/; Max-Age=0`;
  vi.unstubAllGlobals();
});

function mockSystem(dark: boolean) {
  const media = new EventTarget();
  const query = Object.assign(media, { matches: dark });
  vi.stubGlobal("matchMedia", vi.fn(() => query));
  return (next: boolean) => {
    query.matches = next;
    act(() => media.dispatchEvent(new Event("change")));
  };
}

function AppearanceProbe() {
  const { mode, resolvedMode } = useAppearance();
  return <output aria-label="Appearance" data-mode={mode}>{resolvedMode}</output>;
}

function ThemeProbe() {
  const [, theme] = useStyletron();
  return <output aria-label="Theme background">{theme.colors.backgroundPrimary}</output>;
}

function Page({ initialMode }: { initialMode?: "system" | "light" | "dark" }) {
  return <AppearanceProvider {...(initialMode ? { initialMode } : {})}><AppearanceProbe /><AppearanceControl /></AppearanceProvider>;
}

it.each([
  ["", "system"],
  ["other=dark", "system"],
  [`${appearanceCookieName}=dark`, "dark"],
  [`other=1; ${appearanceCookieName}=light; extra=2`, "light"],
  [`${appearanceCookieName}=system`, "system"],
  [`${appearanceCookieName}=invalid`, "system"],
])("reads a validated appearance choice from %s", (header, expected) => {
  expect(readAppearanceMode(header)).toBe(expected);
});

it("keeps the control hidden when a host supplies its own theme", () => {
  render(<AppearanceControl />);
  expect(screen.queryByRole("button")).toBeNull();
});

it("toggles directly with an icon-only action button and never opens a dialog", () => {
  mockSystem(false);
  render(<Page />);
  const trigger = screen.getByRole("button", { name: "Switch to dark mode" });
  expect(trigger.textContent).toBe("");
  expect(trigger.querySelector('svg[aria-hidden="true"]')).not.toBeNull();
  expect(trigger).not.toHaveAttribute("aria-haspopup");
  act(() => trigger.focus());
  fireEvent.click(trigger);
  expect(screen.getByRole("button", { name: "Switch to light mode" })).toBe(trigger);
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(trigger).toHaveFocus();
  expect(readAppearanceMode(document.cookie)).toBe("dark");
  fireEvent.click(trigger);
  expect(trigger).toHaveAccessibleName("Switch to dark mode");
  expect(readAppearanceMode(document.cookie)).toBe("light");
});

it("follows the system until the first click, then remembers the explicit override", () => {
  const changeSystem = mockSystem(false);
  const page = render(<Page />);
  const appearance = screen.getByRole("status", { name: "Appearance" });
  const light = appearance.textContent;
  changeSystem(true);
  const dark = appearance.textContent;
  expect(dark).not.toBe(light);
  expect(document.cookie).not.toContain(appearanceCookieName);
  fireEvent.click(screen.getByRole("button", { name: "Switch to light mode" }));
  expect(appearance.textContent).toBe(light);
  changeSystem(false);
  changeSystem(true);
  expect(appearance.textContent).toBe(light);
  expect(readAppearanceMode(document.cookie)).toBe("light");
  page.unmount();
  render(<Page />);
  expect(screen.getByRole("status", { name: "Appearance" }).textContent).toBe(light);
  fireEvent.click(screen.getByRole("button", { name: "Switch to dark mode" }));
  expect(screen.getByRole("status", { name: "Appearance" }).textContent).toBe(dark);
  expect(readAppearanceMode(document.cookie)).toBe("dark");
});

it("reads the current system preference when clicked before a media-change notification", () => {
  mockSystem(false);
  render(<Page />);
  Object.assign(window.matchMedia(""), { matches: true });
  fireEvent.click(screen.getByRole("button", { name: "Switch to dark mode" }));
  expect(readAppearanceMode(document.cookie)).toBe("light");
  expect(screen.getByRole("button", { name: "Switch to dark mode" })).toBeInTheDocument();
});

it("allows toggling when cookies and system preference APIs are unavailable", () => {
  vi.stubGlobal("matchMedia", undefined);
  vi.spyOn(document, "cookie", "get").mockImplementation(() => { throw new Error("blocked"); });
  vi.spyOn(document, "cookie", "set").mockImplementation(() => { throw new Error("blocked"); });
  render(<Page />);
  fireEvent.click(screen.getByRole("button", { name: "Switch to dark mode" }));
  expect(screen.getByRole("status", { name: "Appearance" })).toHaveTextContent("dark");
  fireEvent.click(screen.getByRole("button", { name: "Switch to light mode" }));
  expect(screen.getByRole("button", { name: "Switch to dark mode" })).toBeInTheDocument();
});

it("preserves the application's custom BaseUI theme when appearance changes", () => {
  mockSystem(false);
  const theme = createTheme({ colors: { backgroundPrimary: "#102030" } });
  render(<BaseProvider theme={theme}><AppearanceProvider>
    <ThemeProbe /><AppearanceControl />
  </AppearanceProvider></BaseProvider>);
  const background = screen.getByRole("status", { name: "Theme background" });
  expect(background).toHaveTextContent("#102030");
  fireEvent.click(screen.getByRole("button", { name: "Switch to dark mode" }));
  expect(background).toHaveTextContent("#102030");
  expect(screen.getByRole("button", { name: "Switch to light mode" })).toBeInTheDocument();
});

it("unsubscribes from system changes when unmounted", () => {
  mockSystem(false);
  const media = window.matchMedia("");
  const unsubscribe = vi.spyOn(media, "removeEventListener");
  const page = render(<Page />);
  page.unmount();
  expect(unsubscribe).toHaveBeenCalledWith("change", expect.any(Function));
});

it("server-renders a saved dark choice and hydrates it without switching to light", async () => {
  mockSystem(false);
  const server = new Server();
  const html = renderToString(<Provider value={server}><Page initialMode="dark" /></Provider>);
  const container = document.createElement("div");
  container.innerHTML = html;
  document.body.append(container);
  const before = container.querySelector("output")?.textContent;
  expect(before).toBe("dark");
  const onRecoverableError = vi.fn();
  const client = new Client();
  let root: ReturnType<typeof hydrateRoot>;
  try {
    await act(async () => {
      root = hydrateRoot(container, <Provider value={client}><Page initialMode="dark" /></Provider>, { onRecoverableError });
    });
    expect(onRecoverableError).not.toHaveBeenCalled();
    expect(container.querySelector("output")?.textContent).toBe(before);
  } finally {
    await act(async () => root!.unmount());
    container.remove();
    for (const stylesheet of Object.values(client.styleElements)) stylesheet.remove();
  }
});
