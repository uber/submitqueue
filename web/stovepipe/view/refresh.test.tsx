import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { WebNavigationProvider } from "@submitqueue/web-platform";
import { RefreshControls } from "./refresh.js";

beforeEach(() => {
  vi.useFakeTimers();
  Object.defineProperty(navigator, "onLine", {
    configurable: true,
    value: true,
  });
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: "visible",
  });
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

it("refreshes manually until polling is enabled, then stops when disabled", async () => {
  const refresh = vi.fn(async () => undefined);
  render(
    <WebNavigationProvider value={{ refresh }}>
      <RefreshControls />
    </WebNavigationProvider>,
  );
  await act(async () => vi.advanceTimersByTime(30_000));
  expect(refresh).not.toHaveBeenCalled();
  await act(async () =>
    fireEvent.click(screen.getByRole("button", { name: "Refresh" })),
  );
  expect(refresh).toHaveBeenCalledWith(null);

  fireEvent.click(screen.getByRole("checkbox"));
  await act(async () => vi.advanceTimersByTime(9_999));
  expect(refresh).toHaveBeenCalledTimes(1);
  await act(async () => vi.advanceTimersByTime(1));
  expect(refresh).toHaveBeenCalledTimes(2);
  fireEvent.click(screen.getByRole("checkbox"));
  await act(async () => vi.advanceTimersByTime(30_000));
  expect(refresh).toHaveBeenCalledTimes(2);
});

it("reports a failed refresh and recovers on the next ten-second poll", async () => {
  const refresh = vi
    .fn()
    .mockRejectedValueOnce(new Error("unavailable"))
    .mockResolvedValue(undefined);
  render(
    <WebNavigationProvider value={{ refresh }}>
      <RefreshControls />
    </WebNavigationProvider>,
  );
  fireEvent.click(screen.getByRole("checkbox"));
  await act(async () => vi.advanceTimersByTime(10_000));
  expect(screen.getByRole("alert")).toHaveTextContent("Refresh failed");
  expect(screen.getByRole("button", { name: "Refresh" })).toBeEnabled();
  await act(async () => vi.advanceTimersByTime(10_000));
  expect(refresh).toHaveBeenCalledTimes(2);
  expect(screen.queryByRole("alert")).toBeNull();
});

it("removes scheduled polls when the page unmounts", async () => {
  const refresh = vi.fn(async () => undefined);
  const { unmount } = render(
    <WebNavigationProvider value={{ refresh }}>
      <RefreshControls />
    </WebNavigationProvider>,
  );
  fireEvent.click(screen.getByRole("checkbox"));
  unmount();
  await act(async () => vi.advanceTimersByTime(30_000));
  expect(refresh).not.toHaveBeenCalled();
});
