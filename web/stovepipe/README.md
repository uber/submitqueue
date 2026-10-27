# Stovepipe web UI

A framework-neutral React UI with a queue request list and a request page containing the current lifecycle summary, retained history, and recorded project results. The host supplies queue configuration, a structural `StovepipeService` client, authentication, transport and optional logging.

Logging uses a synchronous `onLog` callback passed on each `handle` request. Bind it to the host's request-scoped logger; it receives severity, message, and structured metadata containing the original `error`, correlation `requestId`, and page `path`. Omitting it is silent, and callback exceptions do not change page results. The library has no logging-library dependency.

## Integration

Process the UI entry with the host's bundler for both SSR and the browser. The `/server` entry runs directly in Node and does not load BaseUI. See the shared [package contract](../README.md#layers).

Instrumentation belongs in the host: wrap `handle` to observe page results and instrument the concrete service client for RPC traces. A `render` result can contain a failed section, so check `requests.ok`, or both `history.ok` and `projectStatus.ok`, before reporting success. The reference host uses native OpenTelemetry spans, a page-load counter and a duration metric; other hosts can use their own telemetry without an adapter. The reusable module has no telemetry dependency or instrumentation API.

Use the server entry's `createStovepipeWeb` to load a page and render its model with `StovepipeApp`. Mount beneath `WebNavigationProvider` when the host supplies client navigation and refresh. Without it, links and refresh use ordinary document navigation. Import `@submitqueue/web-stovepipe/styles.css` for long-identifier wrapping, and supply BaseUI's `BaseProvider` and Styletron's `Provider`. The host owns the theme and styling engine, including SSR stylesheet extraction and hydration; the library creates neither. BaseUI, React, React DOM, and Styletron React are peer dependencies.

Mount `StovepipeQueueProvider` above route changes with a `loadPage(href, signal)` callback that returns a `QueueRequestsModel` through the host's authenticated transport. Queue lists then load older rows automatically near the bottom, with a keyboard-accessible Load more button and explicit retry after failure. The provider retains loaded rows and scroll positions across detail visits. Without JavaScript or a loader, continuation links navigate normally. The reference host supplies an authenticated, uncached JSON route.

## Routes and data

Request pages use `/<queue>/request/<request-id>` and load both the current summary and events with `GetRequestHistoryByID`. The summary is displayed above history and is not reconstructed from retained events. Queue pagination uses the backend's opaque continuations, and the latest URL carries no time window.

Project results use `GetProjectStatusByID` for that exact request, with an empty
project selection to read all recorded projects. Each page contains up to 50
projects; `?projects=<opaque-token>` carries its continuation and can be shared.
First-page navigation removes the cursor. Pages are live rather than a snapshot;
return to the first page to see newly recorded projects.

The request page shows a compact summary with expandable change and base URIs. Project results are the default view; History is a separate tab at `?view=history`. Switching tabs preserves the project cursor. Project pagination controls appear above and below the table, show the number of projects on the page, and use the host router without a document reload. Browser Back and Forward restore the selected tab and cursor; links also work without JavaScript. The repository result appears in the summary above both tabs; the project results table contains only projects. Recorded results show Green at zero, Partial Breakage between zero and one, and Breakage at one, without percentages.

Repository breakage, per-project breakage, and recording completion are separate.
Zero breakage is green; absent results are never treated as green. Completion
describes result production, not whether all pages have been displayed. Project
and history reads fail independently, and backend diagnostics stay in host logs.
All result mapping, rendering, pagination, refresh and stale-result handling live
in this package; hosts only adapt their transport representation.

## Refresh and presentation

The header offers optional ten-second auto-refresh, off by default. Use browser reload for manual updates. Polling pauses while the page is hidden or offline. Failed refreshes retain successful results on the same route; request-detail results are scoped to their route. Timestamps default to the browser's time zone, with a Browser/UTC segmented control in the header. The browser remembers the selection across navigation and reloads when local storage is available. Server-rendered timestamps use UTC until the browser resolves its time zone and preference. Lifecycle labels and outcome reasons come from the backend and are displayed verbatim.

Queue refreshes update the status of matching displayed requests while preserving their positions and loaded older pages. New arrivals appear behind a Show latest requests action that starts a fresh list. A full browser reload also starts a fresh list; older-page URLs retain their explicit cursor.
