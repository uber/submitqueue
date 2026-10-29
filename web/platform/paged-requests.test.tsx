import { act, cleanup, fireEvent, render, screen, waitFor, within } from "./test/rtl.js";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { PagedRequests, RequestPagesProvider, type RequestPage, type RequestPageLoader } from "./paged-requests.js";

interface Row {
  id: string;
  state: string;
}

const nextHref = "/repo%2Fmain?page=opaque%2B%2F%3D";
function page(ids: string[], next: string | null = nextHref): RequestPage<Row> {
  return { requests: ids.map(id => ({ id, state: "accepted" })), nextHref: next };
}
function RowList({ initial, routeKey }: { initial: RequestPage<Row>; routeKey: string }) {
  return <PagedRequests initial={initial} routeKey={routeKey} requestKey={request => request.id}>
    {requests => <ul aria-label="Requests">{requests.map(request =>
      <li key={request.id}><a href={`/repo%2Fmain/request/${request.id}`}>{request.id}</a><span>{request.state}</span></li>)}</ul>}
  </PagedRequests>;
}
function displayedIds() {
  return within(screen.getByRole("list", { name: "Requests" })).getAllByRole("link").map(link => link.textContent);
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
  let resolve!: (result: RequestPage<Row>) => void;
  const loadPage = vi.fn(() => new Promise<RequestPage<Row>>(complete => {
    resolve = complete;
  }));
  render(<RequestPagesProvider loadPage={loadPage}><RowList routeKey="queue" initial={page(["3", "2"])} /></RequestPagesProvider>);
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
  render(<RequestPagesProvider loadPage={loadPage}><RowList routeKey="queue" initial={page(["2"])} /></RequestPagesProvider>);
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
  function App({ visible = true, latest = initial }: { visible?: boolean; latest?: RequestPage<Row> }) {
    return <RequestPagesProvider loadPage={loadPage}>{visible && <RowList routeKey="queue" initial={latest} />}</RequestPagesProvider>;
  }
  const { rerender } = render(<App />);
  await act(async () => intersect());
  expect(displayedIds()).toEqual(["3", "2", "1"]);
  let signal: AbortSignal | undefined;
  let finish!: (result: RequestPage<Row>) => void;
  loadPage.mockImplementationOnce((_href: string, abort: AbortSignal) => {
    signal = abort;
    return new Promise<RequestPage<Row>>(resolve => {
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

it("keeps loading older requests while a refresh updates the latest rows", async () => {
  let finish!: (result: RequestPage<Row>) => void;
  const loadPage = vi.fn<RequestPageLoader>(() => new Promise<RequestPage<Row>>(resolve => { finish = resolve; }));
  const { rerender } = render(<RequestPagesProvider loadPage={loadPage}>
    <RowList routeKey="queue" initial={page(["3"])} />
  </RequestPagesProvider>);
  await act(async () => intersect());
  const signal = loadPage.mock.calls[0]![1];
  const latest = page(["4", "3"]);
  latest.requests[1]!.state = "succeeded";
  rerender(<RequestPagesProvider loadPage={loadPage}>
    <RowList routeKey="queue" initial={latest} />
  </RequestPagesProvider>);
  expect(signal.aborted).toBe(false);
  expect(screen.getByText("succeeded")).toBeVisible();
  await act(async () => finish(page(["2"], null)));
  expect(displayedIds()).toEqual(["3", "2"]);
  fireEvent.click(screen.getByRole("button", { name: /Show latest requests/ }));
  expect(displayedIds()).toEqual(["4", "3"]);
});

it("rejects an earlier continuation and retries without losing displayed requests", async () => {
  const loadPage = vi.fn()
    .mockResolvedValueOnce(page(["2"], "/last"))
    .mockResolvedValueOnce(page(["1"], nextHref))
    .mockResolvedValueOnce(page(["1"], null));
  render(<RequestPagesProvider loadPage={loadPage}>
    <RowList routeKey="queue" initial={page(["3"])} />
  </RequestPagesProvider>);
  await act(async () => intersect());
  fireEvent.click(screen.getByRole("button", { name: "Load more requests" }));
  await waitFor(() => expect(screen.getByRole("alert")).toBeVisible());
  expect(displayedIds()).toEqual(["3", "2"]);
  fireEvent.click(screen.getByRole("button", { name: "Retry loading older requests" }));
  await waitFor(() => expect(displayedIds()).toEqual(["3", "2", "1"]));
  expect(loadPage.mock.calls.map(call => call[0])).toEqual([nextHref, "/last", "/last"]);
});
