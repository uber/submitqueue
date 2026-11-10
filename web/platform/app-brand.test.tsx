import type { WebLinkProps } from "./navigation.js";
import { WebNavigationProvider } from "./navigation.js";
import { AppBrand } from "./app-brand.js";
import { cleanup, render, screen } from "./test/rtl.js";
import { afterEach, expect, it } from "vitest";

afterEach(cleanup);

it("names the home link without reading the decorative monogram", () => {
  render(<AppBrand href="/queue" mark="SP">Stovepipe</AppBrand>);
  expect(screen.getByRole("link", { name: "Stovepipe" })).toHaveAttribute("href", "/queue");
  expect(screen.getByText("SP")).toHaveAttribute("aria-hidden", "true");
});

it("preserves host navigation and an explicit home label", () => {
  function HostLink(props: WebLinkProps) {
    return <a {...props} data-host-link="true" />;
  }
  render(<WebNavigationProvider value={{ Link: HostLink }}>
    <AppBrand href="/sq" homeLabel="SubmitQueue queues" mark="SQ">SubmitQueue</AppBrand>
  </WebNavigationProvider>);
  const link = screen.getByRole("link", { name: "SubmitQueue queues" });
  expect(link).toHaveAttribute("href", "/sq");
  expect(link).toHaveAttribute("data-host-link", "true");
});

it("omits an empty monogram while preserving custom branding", () => {
  render(<AppBrand href="/"><strong>My queue host</strong></AppBrand>);
  const link = screen.getByRole("link", { name: "My queue host" });
  expect(link.querySelector("[aria-hidden]")).toBeNull();
  expect(link.querySelector("strong")).toHaveTextContent("My queue host");
});

it("keeps a custom symbol decorative without adding a monogram badge", () => {
  render(<AppBrand href="/" mark={<svg viewBox="0 0 32 32" />}>Stovepipe</AppBrand>);
  const link = screen.getByRole("link", { name: "Stovepipe" });
  expect(link.querySelector("svg")?.parentElement).toHaveAttribute("aria-hidden", "true");
  expect(link).toHaveTextContent("Stovepipe");
});
