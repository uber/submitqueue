import { createTheme, DarkTheme, LightTheme, ThemeProvider } from "baseui";
import { AutoRefresh } from "./auto-refresh.js";
import { render } from "../test-render.js";
import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ErrorState } from "./error-state.js";
import { WebShell } from "./shell.js";

const internalError = {
  kind: "internal",
  title: "Something went wrong",
  message: "The page could not load this information.",
  retryable: false,
} as const;

describe("ErrorState", () => {
  it.each([
    ["full-page", false, "alert"],
    ["compact", true, "status"],
  ] as const)("announces a %s error with the %s role", (_name, compact, role) => {
    render(<ErrorState compact={compact} error={internalError} />);
    expect(screen.getByRole(role).textContent).toContain("Something went wrong");
  });
});

describe("WebShell", () => {
  it("omits the environment label and footer when the host clears them", () => {
    render(<WebShell brand="Example" environmentLabel="" footer={null}><main>content</main></WebShell>);
    expect(screen.getByRole("link").textContent).toBe("Example");
    expect(screen.queryByRole("contentinfo")).toBeNull();
  });
});

describe("host theme", () => {
  it("uses the host's button tokens and responds to a theme change", () => {
    const theme = createTheme({ colors: { buttonSecondaryFill: "#123456", buttonSecondaryText: "#ffffff" } });
    const { rerender } = render(<ThemeProvider theme={theme}><WebShell><AutoRefresh automatic={false} refresh={() => undefined} /></WebShell></ThemeProvider>);
    expect(getComputedStyle(screen.getByRole("button", { name: "Refresh" })).backgroundColor).toBe("rgb(18, 52, 86)");
    rerender(<ThemeProvider theme={DarkTheme}><WebShell><AutoRefresh automatic={false} refresh={() => undefined} /></WebShell></ThemeProvider>);
    expect(screen.getByRole("button", { name: "Refresh" })).toHaveStyle({ backgroundColor: DarkTheme.colors.buttonSecondaryFill });
    rerender(<ThemeProvider theme={LightTheme}><WebShell><AutoRefresh automatic={false} refresh={() => undefined} /></WebShell></ThemeProvider>);
    expect(screen.getByRole("button", { name: "Refresh" })).toHaveStyle({ backgroundColor: LightTheme.colors.buttonSecondaryFill });
  });
});
