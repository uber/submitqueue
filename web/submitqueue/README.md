# @submitqueue/web-submitqueue

The complete read-only SubmitQueue UI as a mountable module. Folders follow the Go domain layout, plus `view/` because Go has no UI layer.

| Folder | Holds |
|---|---|
| [`entity/`](entity/) | Serializable view models (requests, history, queues, changes, pages) and load results |
| [`core/`](core/) | Paths and `parseRoute`, change references, status table, receipt windows |
| [`extension/gateway/`](extension/gateway/) | The structural gateway contract, the gRPC-status classifier, and test fakes in `mock/` |
| [`controller/`](controller/) | One loader per page: directory, queue, request, change |
| [`view/`](view/) | `SubmitQueueApp`, pages, the shell and small components, navigation, polling, and the stylesheet |
| [`module.ts`](module.ts) | `createSubmitQueueWeb`, which serves one page request end to end |

Entry points: the root is client-safe (`SubmitQueueApp`, page frames, `WebNavigationProvider`, model types); `./server` has `createSubmitQueueWeb`, the gateway contract, and `WebPaths`; `./extension/gateway/mock` has test fakes; `./styles.css` is the full stylesheet. Controllers and helpers stay internal.

Process the UI entry with the host's bundler for both SSR and the browser. The `/server` entry runs directly in Node and does not load BaseUI. See the shared [package contract](../README.md#layers).

**Hosting it.** Give `createSubmitQueueWeb` a gateway client (any client matching the structural contract, so the module ships no generated code) and optionally a mount path, authorization check, error classifier, or cached queue catalog. Route every page path to `handle`, render `SubmitQueueApp` with each page model (it includes the shared header, time zone preference and refresh controls), and wrap it in a `WebNavigationProvider` for client-side navigation. Mount `RequestPagesProvider` above route changes with a `loadPage(href, signal)` callback that returns the queue page's `QueueRequestsModel` through the host's authenticated transport; queue lists then load older rows automatically near the bottom, with a Load more button and explicit retry after failure, and keep loaded rows and scroll position across request visits. Without it, continuation links navigate normally. Load `./styles.css` for long-identifier wrapping and supply BaseUI's `BaseProvider` and Styletron's `Provider`. The host owns the theme and styling engine, including SSR stylesheet extraction and hydration; BaseUI, React, React DOM, and Styletron React are peers. `WebShell` follows the provider's theme rather than taking a separate theme prop.

Pass `onLog` on each `handle` request to deliver severity, message, and structured metadata to the host's request-scoped logger. Errors retain their original value under `error`, and logs include the correlation `requestId` and page `path`. The callback must be synchronous; omitting it is silent, and callback exceptions do not change page results. The module has no logging-library dependency.

Instrumentation belongs in the host: wrap `handle` to observe page results and instrument the concrete gateway client for RPC traces and timings. A `render` result can contain a failed load, so check `model.result.ok` before reporting success. The reference host uses native OpenTelemetry APIs; other hosts can use their own telemetry without an adapter. The reusable module has no telemetry dependency or instrumentation API.

**Behavior worth knowing.** Pages live under `/queue/<queue>` (`/request/<sqid>` and `/change/…` below it), so queue names never collide with host routes. `queueRequests` returns the queue page's requests as data for a host's JSON route, with the same authorization and filters. A queue list starts at its newest request however old it is, with no time window in the URL. Repeated `?status=` parameters restrict it to those current statuses; the gateway applies the filter, so a filtered page can be short while older matches load as the viewer scrolls. Queue refreshes update the status of displayed requests in place; new arrivals appear behind a Show latest requests action. Older pages put the gateway's `List` continuation token in the URL as `?page=` unchanged; the token carries its own window, so continuing needs only the queue and token, and hosts hold no pagination secret. Expected outcomes (missing resources, page tokens the gateway rejects) are results, never exceptions. Raw gateway errors are logged, never rendered. Views keep the last good data on transient failures. Request IDs stay opaque in URLs; dot-only segments get a visible `~` escape.

The header offers optional ten-second auto-refresh, off by default. Browser reload remains available for manual updates. Polling pauses while the page is hidden or offline and stops for terminal views.

Timestamps default to the browser's time zone. The Browser/UTC segmented control in the shared header applies to the entire page. The browser remembers the selection across navigation and reloads when local storage is available. Server-rendered timestamps use UTC until the browser resolves its time zone and preference.

```bash
./tool/bazel test //web/submitqueue:all
```
