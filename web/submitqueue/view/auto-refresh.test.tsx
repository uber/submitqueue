import { render } from "../test-render.js";
import { act, fireEvent, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { AutoRefresh } from "./auto-refresh.js";

describe("AutoRefresh", () => {
  beforeEach(() => {
    vi.useFakeTimers();
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

  it("keeps manual and timer refreshes single-flight beyond multiple intervals", async () => {
    let finish: (() => void) | undefined;
    const refresh = vi.fn(() => new Promise<void>((resolve) => (finish = resolve)));
    render(<AutoRefresh intervalMs={100} jitterMs={0} refresh={refresh} />);

    await act(async () => vi.advanceTimersByTime(100));
    fireEvent.click(screen.getByRole("button", { name: "Refreshing…" }));
    await act(async () => vi.advanceTimersByTime(500));
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

  it("progressively backs off consecutive server failures", async () => {
    const refresh = vi.fn(async () => undefined);
    render(
      <AutoRefresh
        intervalMs={100}
        jitterMs={0}
        maxBackoffMs={1_000}
        random={() => 0}
        refresh={refresh}
        transientFailureCount={1}
      />,
    );

    await act(async () => vi.advanceTimersByTime(200));
    expect(refresh).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTime(399));
    expect(refresh).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTime(1));
    expect(refresh).toHaveBeenCalledTimes(2);
    await act(async () => vi.advanceTimersByTime(799));
    expect(refresh).toHaveBeenCalledTimes(2);
    await act(async () => vi.advanceTimersByTime(1));
    expect(refresh).toHaveBeenCalledTimes(3);
  });

  it("backs off after a rejected refresh and recovers once a refresh succeeds", async () => {
    const refresh = vi.fn<() => Promise<void>>().mockRejectedValueOnce(new Error("network")).mockResolvedValue(undefined);
    render(<AutoRefresh intervalMs={100} jitterMs={0} random={() => 0} refresh={refresh} />);

    await act(async () => vi.advanceTimersByTime(100));
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(screen.getByText("Updates unavailable · retrying")).toBeTruthy();
    await act(async () => vi.advanceTimersByTime(199));
    expect(refresh).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTime(1));
    expect(refresh).toHaveBeenCalledTimes(2);
    expect(screen.getByText("Live")).toBeTruthy();
    await act(async () => vi.advanceTimersByTime(100));
    expect(refresh).toHaveBeenCalledTimes(3);
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

  it("pauses while hidden and resumes when visible", async () => {
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "hidden" });
    const refresh = vi.fn(async () => undefined);
    render(<AutoRefresh intervalMs={100} jitterMs={0} refresh={refresh} />);

    await act(async () => vi.advanceTimersByTime(1_000));
    expect(refresh).not.toHaveBeenCalled();
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" });
    fireEvent(document, new Event("visibilitychange"));
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

  it("does not poll a terminal list failure", async () => {
    const refresh = vi.fn(async () => undefined);
    render(
      <AutoRefresh
        intervalMs={100}
        jitterMs={0}
        refresh={refresh}
        terminal
        transientFailureCount={1}
      />,
    );

    await act(async () => vi.advanceTimersByTime(1_000));
    expect(refresh).not.toHaveBeenCalled();
  });
});
