# Stovepipe web UI

A framework-neutral React UI with a queue request list and a request page containing the current lifecycle summary and retained history. The host supplies queue configuration, a structural `StovepipeService` client, authentication, transport, logging and OpenTelemetry providers.

Logging uses a synchronous `onLog` callback passed on each `handle` request. Bind it to the host's request-scoped logger; it receives severity, message, and structured metadata containing the original `error`, correlation `requestId`, and page `path`. Omitting it is silent, and callback exceptions do not change page results. The library has no logging-library dependency.

## Integration

Use the server entry's `createStovepipeWeb` to load a page and render its model with `StovepipeApp`. Mount beneath `WebNavigationProvider` when the host supplies client navigation and refresh. Without it, links and refresh use ordinary document navigation. Import `@submitqueue/web-stovepipe/styles.css`, which includes the common native styles from `@submitqueue/web-platform`.

## Routes and data

Request pages use `/<queue>/request/<request-id>` and load both the current summary and events with `GetRequestHistoryByID`. The summary is displayed above history and is not reconstructed from retained events. Queue pagination uses the backend's opaque continuations, and the latest URL carries no time window.

## Refresh and presentation

Refresh is manual by default, with optional ten-second polling. Failed refreshes retain successful results on the same route; changing routes clears them. Timestamps use UTC. Lifecycle labels and outcome reasons come from the backend and are displayed verbatim.
