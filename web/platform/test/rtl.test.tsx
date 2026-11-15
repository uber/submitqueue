import { Block } from "baseui/block";
import { afterEach, expect, it } from "vitest";
import { cleanup, render, screen } from "./rtl.js";

afterEach(cleanup);

it("removes a test render's styles without affecting other mounted renders", () => {
  const initialSheets = document.styleSheets.length;
  const first = render(<Block display="none">First styled render</Block>);
  const second = render(<Block display="none">Second styled render</Block>);
  expect(screen.getByText("First styled render")).not.toBeVisible();
  expect(screen.getByText("Second styled render")).not.toBeVisible();
  const mountedSheets = document.styleSheets.length;
  expect(mountedSheets).toBeGreaterThan(initialSheets);

  first.unmount();
  expect(document.styleSheets.length).toBeLessThan(mountedSheets);
  expect(screen.getByText("Second styled render")).not.toBeVisible();

  second.unmount();
  expect(document.styleSheets.length).toBe(initialSheets);
});
