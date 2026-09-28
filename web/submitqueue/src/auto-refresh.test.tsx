import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const routerRefresh = vi.fn();
vi.mock("next/navigation", () => ({
  useRouter: () => ({ refresh: routerRefresh }),
}));

import { AutoRefresh } from "./auto-refresh";

describe("AutoRefresh", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    routerRefresh.mockReset();
    Object.defineProperty(navigator, "onLine", { configurable: true, value: true });
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" });
  });

  afterEach(() => vi.useRealTimers());

  it("uses the interval plus jitter and continues polling", async () => {
    const refresh = vi.fn(async () => undefined);
    render(<AutoRefresh intervalMs={2_000} jitterMs={500} random={() => 0.5} refresh={refresh} />);

    await act(async () => vi.advanceTimersByTime(2_249));
    expect(refresh).not.toHaveBeenCalled();
    await act(async () => vi.advanceTimersByTime(1));
    expect(refresh).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTime(2_250));
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("keeps manual and timer refreshes single-flight", async () => {
    let finish: (() => void) | undefined;
    const refresh = vi.fn(() => new Promise<void>((resolve) => (finish = resolve)));
    render(<AutoRefresh intervalMs={100} jitterMs={0} refresh={refresh} />);

    await act(async () => vi.advanceTimersByTime(100));
    fireEvent.click(screen.getByRole("button", { name: "Refreshing…" }));
    expect(refresh).toHaveBeenCalledTimes(1);
    await act(async () => finish?.());
  });

  it("backs off transient failures up to the configured cap", async () => {
    const refresh = vi.fn(async () => undefined);
    render(
      <AutoRefresh
        intervalMs={2_000}
        jitterMs={0}
        maxBackoffMs={30_000}
        random={() => 0}
        refresh={refresh}
        transientFailureCount={4}
      />,
    );

    await act(async () => vi.advanceTimersByTime(29_999));
    expect(refresh).not.toHaveBeenCalled();
    await act(async () => vi.advanceTimersByTime(1));
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it("pauses while offline and resumes when online", async () => {
    Object.defineProperty(navigator, "onLine", { configurable: true, value: false });
    const refresh = vi.fn(async () => undefined);
    render(<AutoRefresh intervalMs={100} jitterMs={0} refresh={refresh} />);

    expect(screen.getByText("Automatic refresh paused")).toBeTruthy();
    await act(async () => vi.advanceTimersByTime(1_000));
    expect(refresh).not.toHaveBeenCalled();
    Object.defineProperty(navigator, "onLine", { configurable: true, value: true });
    fireEvent(window, new Event("online"));
    await act(async () => vi.advanceTimersByTime(100));
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it("stops automatic polling for terminal detail views", async () => {
    const refresh = vi.fn(async () => undefined);
    render(<AutoRefresh intervalMs={100} jitterMs={0} refresh={refresh} terminal />);

    await act(async () => vi.advanceTimersByTime(1_000));
    expect(refresh).not.toHaveBeenCalled();
    expect(screen.getByText("Automatic refresh stopped")).toBeTruthy();
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Refresh" })));
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});
