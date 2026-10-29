# Stovepipe web UI

A framework-neutral React UI with queue-level Requests and Open issues views and a request page containing the current lifecycle summary, retained history, and recorded project results. The host supplies queue configuration, a structural `StovepipeService` client, authentication, transport and optional logging.

Logging uses a synchronous `onLog` callback passed on each `handle` request. Bind it to the host's request-scoped logger; it receives severity, message, and structured metadata containing the original `error`, correlation `requestId`, and page `path`. Omitting it is silent, and callback exceptions do not change page results. The library has no logging-library dependency.

## Integration

Process the UI entry with the host's bundler for both SSR and the browser. The `/server` entry runs directly in Node and does not load BaseUI. See the shared [package contract](../README.md#layers).

Instrumentation belongs in the host: wrap `handle` to observe page results and instrument the concrete service client for RPC traces. A `render` result can contain a failed section, so check `requests.ok`, `issues.ok`, or both `history.ok` and `projectStatus.ok`, according to the page kind, before reporting success. The reference host uses native OpenTelemetry spans, a page-load counter and a duration metric; other hosts can use their own telemetry without an adapter. The reusable module has no telemetry dependency or instrumentation API.

Use the server entry's `createStovepipeWeb` to load a page and render its model with `StovepipeApp`. Mount beneath `WebNavigationProvider` when the host supplies client navigation and refresh. Without it, links and refresh use ordinary document navigation. Import `@submitqueue/web-stovepipe/styles.css` for long-identifier wrapping, and supply BaseUI's `BaseProvider` and Styletron's `Provider`. The host owns the theme and styling engine, including SSR stylesheet extraction and hydration; the library creates neither. BaseUI, React, React DOM, and Styletron React are peer dependencies.

Mount `RequestPagesProvider` above route changes with a `loadPage(href, signal)` callback that returns a `QueueRequestsModel` through the host's authenticated transport. Queue lists then load older rows automatically near the bottom, with a keyboard-accessible Load more button and explicit retry after failure. The provider retains loaded rows and scroll positions across detail visits. Without JavaScript or a loader, continuation links navigate normally. The reference host supplies an authenticated, uncached JSON route.

Mount `OpenIssuePagesProvider` with an authenticated `loadPage(href, signal)` callback returning an `OpenIssuesModel` to append issue pages with Load more. The reference host supplies its own uncached, authenticated continuation route. Without JavaScript or a loader, continuation links navigate normally.

## Routes and data

Request pages use `/<queue>/request/<request-id>` and load both the current summary and events with `GetRequestHistoryByID`. The summary is displayed above history and is not reconstructed from retained events. Queue pagination uses the backend's opaque continuations, and the latest URL carries no time window.

The queue's `?view=issues` tab calls `ListOpenIssues` and groups rows by task in backend task/unit order. An empty unit is labeled Task-wide failure and appears first within its task; loading another page extends the existing group. Each row links to its opening request and shows the opening timestamp. The displayed count is loaded issues, not a queue-wide total. Empty batches can carry a continuation, so only an exhausted empty listing says No tracked open issues. This is not a claim of build success or complete coverage. Project attribution, issue history, and repair controls are not part of this view.

Project results use `GetProjectStatusByID` for that exact request, with an empty
project selection to read all recorded projects. Pages default to 50
projects; the Projects per page dropdown offers 25, 50, 100, or 200.
`?projects=<opaque-token>` carries the continuation and `projectPageSize` carries
a non-default page size, so both can be shared. Changing the size starts at the
first page. Unsupported sizes fall back to 50.
First-page navigation removes the cursor. Pages are live rather than a snapshot;
return to the first page to see newly recorded projects.

The request page shows a compact summary with change and base URIs always visible. Project results are the default view; History is a separate tab at `?view=history`. Build and Test Issues (`?view=issues`) and Fix Attempts (`?view=fix-attempts`) show Coming soon. Switching tabs preserves the project cursor. Project results and history tables fill the remaining page height and scroll within the active tab. On small screens, a minimum table height keeps rows usable while the page can scroll to reach the controls. Project pagination controls appear above and below the table, show the number of projects on the page, and use the host router without a document reload or resetting the document scroll position. A new project page starts at the top of the table, while the surrounding page and pagination controls stay in place, including on a short final page. Host router links should honor `WebLinkProps.scroll` to support this behavior. Browser Back and Forward restore the selected tab and cursor; links also work without JavaScript. The repository result appears in the summary above all tabs; the project results table contains only projects. Recorded results show Green at zero, Partial Breakage between zero and one, and Breakage at one, without percentages.

Queue rows and request summaries show recorded broken projects out of the total when the total is positive, including zero broken projects. A zero total displays a dash. Counts come from the backend summary, not the current page of project results, so pagination does not change the ratio.

Repository breakage, per-project breakage, and recording completion are separate.
Zero breakage is green; absent results are never treated as green. Completion
describes result production, not whether all pages have been displayed. Project
and history reads fail independently, and backend diagnostics stay in host logs.
All result mapping, rendering, pagination, refresh and stale-result handling live
in this package; hosts only adapt their transport representation.

## Refresh and presentation

The header offers optional ten-second auto-refresh, off by default. Use browser reload for manual updates. Polling pauses while the page is hidden or offline. Failed refreshes retain successful results on the same route; request-detail results are scoped to their route. Timestamps default to the browser's time zone, with a Browser/UTC segmented control in the header. The browser remembers the selection across navigation and reloads when local storage is available. Server-rendered timestamps use UTC until the browser resolves its time zone and preference. Lifecycle labels and outcome reasons come from the backend and are displayed verbatim.

Queue refreshes update the status of matching displayed requests while preserving their positions and loaded older pages. New arrivals appear behind a Show latest requests action that starts a fresh list. A full browser reload also starts a fresh list; older-page URLs retain their explicit cursor.

Open-issue refreshes read the first page. Before any continuation is loaded, a successful refresh replaces the displayed issues. After loading more, polling preserves the displayed traversal and offers Show latest issues to replace it with the latest successful first-page result. The issues page and retained data are scoped to the queue and view, not the cursor, so refreshing a continuation URL preserves both the display on failure and the auto-refresh setting. Failed continuations preserve loaded issues and allow retrying the same cursor. Loading more is not a snapshot; issues can change between reads.
