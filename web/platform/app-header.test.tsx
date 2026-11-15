import { afterEach, expect, it } from "vitest";
import { cleanup, render, screen, within } from "./test/rtl.js";
import { AppBrand } from "./app-brand.js";
import { AppHeader } from "./app-header.js";

afterEach(cleanup);

it("keeps branding and controls in a compact header that can grow when wrapped", () => {
  render(<AppHeader>
    <AppBrand href="/" mark="SQ">SubmitQueue</AppBrand>
    <button>Enable auto-refresh</button>
  </AppHeader>);
  const header = screen.getByRole("banner");
  expect(within(header).getByRole("link", { name: "SubmitQueue" })).toHaveAttribute("href", "/");
  expect(within(header).getByRole("button", { name: "Enable auto-refresh" })).toBeVisible();
  const style = getComputedStyle(header);
  expect(style.minHeight).toBe("64px");
  expect(style.height).not.toBe("64px");
  expect(style.boxSizing).toBe("border-box");
  expect(style.flexWrap).toBe("wrap");
});
