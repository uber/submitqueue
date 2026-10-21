import { render, screen } from "../../platform/test/rtl.js";
import { render as renderUnwrapped } from "@testing-library/react";
import { BaseProvider, DarkTheme, LightTheme } from "baseui";
import { Button } from "baseui/button";
import { renderToString } from "react-dom/server";
import { Server } from "styletron-engine-atomic";
import { Provider } from "styletron-react";
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

  it("follows the host theme when it changes", () => {
    const engine = new Server();
    function Page({ theme }: { theme: typeof LightTheme }) {
      return <Provider value={engine}><BaseProvider theme={theme}>
        <WebShell brand="Example"><main><Button>Refresh</Button></main></WebShell>
      </BaseProvider></Provider>;
    }
    const { rerender } = renderUnwrapped(<Page theme={LightTheme} />);
    rerender(<Page theme={DarkTheme} />);
    expect(engine.getCss()).toContain(DarkTheme.colors.backgroundPrimary);
    expect(engine.getCss()).toContain(DarkTheme.colors.buttonPrimaryFill);
    expect(screen.getByRole("link").textContent).toBe("Example");
  });

  it("extracts custom host theme styles without leaking between server renders", () => {
    function renderPage(backgroundPrimary: string) {
      const engine = new Server();
      const theme = { ...LightTheme, colors: { ...LightTheme.colors, backgroundPrimary } };
      const html = renderToString(<Provider value={engine}><BaseProvider theme={theme}>
        <WebShell brand="Example"><main>Queue activity</main></WebShell>
      </BaseProvider></Provider>);
      return { html, css: engine.getCss() };
    }
    const first = renderPage("#123456");
    const second = renderPage("#654321");
    expect(first.html).toContain("Queue activity");
    expect(first.css).toContain("#123456");
    expect(second.css).toContain("#654321");
    expect(second.css).not.toContain("#123456");
  });
});
