import { render } from "../test-render.js";
import { fireEvent, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import type { WebPageModel } from "../entity/index.js";
import { WebNavigationProvider, type WebNavigation } from "./navigation.js";
import { WebRefreshControl } from "./refresh-control.js";
import { useWebApp, WebApp, WebLink } from "./web-app.js";

function pageModel(title: string, refreshHref: string | null = null): WebPageModel {
  return { key: title, title, refresh: { terminal: false, transientFailureCount: 0, refreshHref } };
}

function Page({ model }: { model: WebPageModel }) {
  const app = useWebApp();
  return <main>
    <h1>{model.title}</h1>
    <button type="button" onClick={() => void app?.navigate("/next")}>Go</button>
    <WebRefreshControl refresh={model.refresh} />
  </main>;
}

function renderApp(model: WebPageModel, navigation: WebNavigation) {
  return render(<WebNavigationProvider value={navigation}><WebApp model={model} Page={Page} /></WebNavigationProvider>);
}

describe("WebApp", () => {
  it("refreshes through the host, moving a snapshot page to its refresh target", () => {
    const refresh = vi.fn(async () => undefined);
    renderApp(pageModel("Snapshot", "/latest"), { refresh });

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));

    expect(refresh).toHaveBeenCalledWith("/latest");
  });

  it("navigates through the host router", () => {
    const push = vi.fn();
    renderApp(pageModel("First"), { push });

    fireEvent.click(screen.getByRole("button", { name: "Go" }));

    expect(push).toHaveBeenCalledWith("/next");
  });

  it("renders whatever model the host passes next", () => {
    const navigation = {};
    const { rerender } = renderApp(pageModel("First"), navigation);

    rerender(<WebNavigationProvider value={navigation}><WebApp model={pageModel("Second")} Page={Page} /></WebNavigationProvider>);

    expect(screen.getByRole("heading").textContent).toBe("Second");
  });

  it("offers only manual refresh when the host cannot refresh in place", () => {
    renderApp(pageModel("First"), {});
    expect(screen.getByText("Manual refresh")).toBeDefined();
  });
});

describe("WebLink", () => {
  it("renders the host link component when one is supplied", () => {
    const Link = ({ href, children }: { href: string; children?: ReactNode }) => <a data-host-link href={href}>{children}</a>;
    render(<WebNavigationProvider value={{ Link }}><WebLink href="/next">Next</WebLink></WebNavigationProvider>);
    expect(screen.getByRole("link").hasAttribute("data-host-link")).toBe(true);
  });

  it("renders a plain anchor otherwise", () => {
    render(<WebLink href="/next">Next</WebLink>);
    expect(screen.getByRole("link").getAttribute("href")).toBe("/next");
  });
});

describe("WebRefreshControl", () => {
  it("renders nothing outside a WebApp", () => {
    const { container } = render(<WebRefreshControl refresh={pageModel("Alone").refresh} />);
    expect(container.textContent).toBe("");
    expect(container.querySelector("button")).toBeNull();
  });
});
