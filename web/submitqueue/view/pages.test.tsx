import { render } from "../test-render.js";
import { fireEvent, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { WebNavigationProvider } from "./navigation.js";
import { SubmitQueueApp } from "./app.js";
import { SubmitQueuePage, SubmitQueueShell, SubmitQueueStatePage } from "./pages.js";
import { createSubmitQueueWeb } from "../module.js";
import { newHmacCursorCodec } from "../extension/cursor/hmac/index.js";
import { createFakeGatewayReader, gatewayHistoryFixture, gatewayRequestFixture } from "../extension/gateway/mock/index.js";
import { WebPaths } from "../core/paths.js";
import { webErrorForGatewayCode } from "../controller/error.js";

const sha = "0123456789abcdef0123456789abcdef01234567";
const uri = `github://github.com/uber/submitqueue/pull/123/${sha}`;
const gateway = createFakeGatewayReader({
  requests: [gatewayRequestFixture({ sqid: "12", changeUris: [uri] })],
  history: [gatewayHistoryFixture()],
});
const web = createSubmitQueueWeb({
  gateway, cursors: newHmacCursorCodec("host-secret"), paths: new WebPaths({ basePath: "/sq" }),
});

describe("default reusable application views", () => {
  it("renders shared chrome with host-supplied branding, environment, and footer", () => {
    render(<SubmitQueueShell homeHref="/sq" brand="My queue host" environmentLabel="Staging" footer="Company footer">
      <main>Host content</main>
    </SubmitQueueShell>);
    expect(screen.getByRole("link", { name: "SubmitQueue queues" }).getAttribute("href")).toBe("/sq");
    expect(screen.getByText("My queue host")).toBeTruthy();
    expect(screen.getByText("Staging")).toBeTruthy();
    expect(screen.getByText("Company footer")).toBeTruthy();
    expect(screen.getByRole("main").textContent).toBe("Host content");
  });

  it.each([
    { path: "/sq", heading: "Queues" },
    { path: "/sq/demo-queue", heading: "demo-queue" },
    { path: "/sq/demo-queue/request/12", heading: "12" },
    { path: "/sq/demo-queue/change/github/github.com/uber/submitqueue/pull/123", heading: "PR #123" },
  ])("renders a complete page at $path without Next.js", async ({ path, heading }) => {
    const result = await web.handle({ path, search: {} });
    if (result.kind !== "render") {
      throw new Error("Expected a rendered page");
    }
    const refresh = vi.fn(async () => undefined);
    render(<WebNavigationProvider value={{ refresh }}><SubmitQueueShell homeHref="/sq">
      <SubmitQueueApp model={result.model} />
    </SubmitQueueShell></WebNavigationProvider>);
    expect(screen.getByRole("heading", { name: heading })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    expect(refresh).toHaveBeenCalledOnce();
    const queueLinks = screen.queryAllByRole("link", { name: "demo-queue" });
    queueLinks.forEach(link => expect(link.getAttribute("href")).toBe("/sq/demo-queue"));
  });

  it("renders a page without refresh controls outside an app", async () => {
    const result = await web.handle({ path: "/sq", search: {} });
    if (result.kind !== "render") {
      throw new Error("Expected a rendered page");
    }
    render(<SubmitQueuePage model={result.model} />);
    expect(screen.queryByRole("button", { name: "Refresh" })).toBeNull();
  });

  it("keeps the queue directory visible through a transient failure", async () => {
    const result = await web.handle({ path: "/sq", search: {} });
    if (result.kind !== "render" || result.model.kind !== "queues") {
      throw new Error("Expected the queue directory");
    }
    const { rerender } = render(<SubmitQueuePage model={result.model} />);
    rerender(<SubmitQueuePage model={{ ...result.model, result: { ok: false, error: webErrorForGatewayCode("unavailable") } }} />);
    expect(screen.getByText("Showing the last successful snapshot; updates are unavailable.")).toBeTruthy();
    expect(screen.getByRole("link", { name: "demo-queue" })).toBeTruthy();
  });

  it("delegates change-version navigation through the host adapter", async () => {
    const result = await web.handle({ path: "/sq/demo-queue/change/github/github.com/uber/submitqueue/pull/123", search: {} });
    if (result.kind !== "render") {
      throw new Error("Expected a change page");
    }
    const navigate = vi.fn();
    render(<WebNavigationProvider value={{ push: navigate }}><SubmitQueueApp model={result.model} /></WebNavigationProvider>);
    fireEvent.click(screen.getByRole("combobox"));
    fireEvent.click(screen.getByRole("option", { name: sha }));
    expect(navigate).toHaveBeenCalledWith(`/sq/demo-queue/change/github/github.com/uber/submitqueue/pull/123/${sha}`);
  });

  it.each(["not-found", "unauthorized"] as const)("renders the shared %s state", kind => {
    render(<SubmitQueueStatePage kind={kind} homeHref="/sq" />);
    expect(screen.getByRole("heading", { name: kind === "not-found" ? "Request not found" : "Authentication required" })).toBeTruthy();
  });
});
