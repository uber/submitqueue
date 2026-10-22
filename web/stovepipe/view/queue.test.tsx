import { act, cleanup, fireEvent, render, screen, waitFor, within } from "../../platform/test/rtl.js";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import type { QueueRequestsModel } from "../entity/index.js";
import { QueueRequests, StovepipeQueueProvider } from "./queue.js";

const nextHref = "/repo%2Fmain?page=opaque%2B%2F%3D";
function page(ids: string[], next: string | null = nextHref): QueueRequestsModel {
  return { requests: ids.map(id => ({
    id, href: `/repo%2Fmain/request/${id}`, changeUri: `git://repo/${id}`, baseUri: "", state: "accepted",
    acceptedAtMs: "1000", updatedAtMs: "1000", outcomeReason: "",
  })), nextHref: next };
}
function displayedIds() {
  return within(screen.getByRole("table", { name: "Requests" })).getAllByRole("link").map(link => link.textContent);
}
let intersect: () => void;
beforeEach(() => {
  vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
  vi.stubGlobal("IntersectionObserver", class {
    constructor(callback: IntersectionObserverCallback) {
      intersect = () => callback([{ isIntersecting: true } as IntersectionObserverEntry], this as unknown as IntersectionObserver);
    }
    observe() {}
    disconnect() {}
  });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("loads on intersection once, appends unique requests, and stops at the end", async () => {
  let resolve!: (result: QueueRequestsModel) => void;
  const loadPage = vi.fn(() => new Promise<QueueRequestsModel>(complete => {
    resolve = complete;
  }));
  render(<StovepipeQueueProvider loadPage={loadPage}><QueueRequests routeKey="queue" initial={page(["3", "2"])} /></StovepipeQueueProvider>);
  await act(async () => {
    intersect();
    intersect();
  });
  expect(loadPage).toHaveBeenCalledTimes(1);
  expect(loadPage).toHaveBeenCalledWith(nextHref, expect.any(AbortSignal));
  expect(displayedIds()).toEqual(["3", "2"]);
  const loadMore = screen.getByRole("button", { name: "Load more requests" });
  expect(loadMore).toHaveAttribute("aria-disabled", "true");
  expect(screen.getByRole("status")).toHaveTextContent("Loading older requests…");
  expect(screen.getByRole("progressbar", { name: "Loading older requests" })).toBeVisible();
  fireEvent.click(loadMore);
  expect(loadPage).toHaveBeenCalledTimes(1);
  await act(async () => resolve(page(["2", "1", "1"], null)));
  expect(displayedIds()).toEqual(["3", "2", "1"]);
  expect(screen.getByRole("status")).toHaveTextContent("All available requests loaded");
  expect(screen.queryByRole("progressbar")).toBeNull();
  expect(screen.queryByRole("button", { name: /Load more/ })).toBeNull();
});

it("retains rows after a failed load and retries the same continuation by button", async () => {
  const loadPage = vi.fn().mockRejectedValueOnce(new Error("unavailable")).mockResolvedValueOnce(page(["1"], null));
  render(<StovepipeQueueProvider loadPage={loadPage}><QueueRequests routeKey="queue" initial={page(["2"])} /></StovepipeQueueProvider>);
  await act(async () => intersect());
  expect(displayedIds()).toEqual(["2"]);
  expect(screen.getByRole("alert")).toHaveTextContent("Could not load older requests");
  expect(screen.queryByRole("progressbar")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Retry loading older requests" }));
  await waitFor(() => expect(displayedIds()).toEqual(["2", "1"]));
  expect(loadPage.mock.calls.map(call => call[0])).toEqual([nextHref, nextHref]);
  expect(screen.queryByRole("alert")).toBeNull();
});

it("aborts a departing queue, restores its loaded rows, and keeps new arrivals behind an explicit action", async () => {
  const initial = page(["3", "2"]);
  const loadPage = vi.fn().mockResolvedValueOnce(page(["1"], "/repo%2Fmain?page=last"));
  function App({ visible = true, latest = initial }: { visible?: boolean; latest?: QueueRequestsModel }) {
    return <StovepipeQueueProvider loadPage={loadPage}>{visible && <QueueRequests routeKey="queue" initial={latest} />}</StovepipeQueueProvider>;
  }
  const { rerender } = render(<App />);
  await act(async () => intersect());
  expect(displayedIds()).toEqual(["3", "2", "1"]);
  let signal: AbortSignal | undefined;
  let finish!: (result: QueueRequestsModel) => void;
  loadPage.mockImplementationOnce((_href: string, abort: AbortSignal) => {
    signal = abort;
    return new Promise<QueueRequestsModel>(resolve => {
      finish = resolve;
    });
  });
  await act(async () => intersect());
  rerender(<App visible={false} />);
  expect(signal?.aborted).toBe(true);
  await act(async () => finish(page(["0"], null)));
  const latest = page(["4", "3"]);
  latest.requests[1]!.state = "succeeded";
  rerender(<App latest={latest} />);
  expect(displayedIds()).toEqual(["3", "2", "1"]);
  expect(screen.getByText("succeeded")).toBeVisible();
  expect(screen.queryByRole("link", { name: "0" })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: /Show latest requests/ }));
  expect(displayedIds()).toEqual(["4", "3"]);
  expect(window.scrollTo).toHaveBeenCalledWith(0, 0);
});
