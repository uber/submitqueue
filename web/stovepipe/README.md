# Stovepipe web UI

A framework-neutral React UI for queue requests, open issues, and request details. The host supplies authentication, backend transport, routing adapters, and styling.

## Host integration

- Create the page loader with `createStovepipeWeb` from the `/server` entry, supplying queues and a `StovepipeService` client. Render page models with `StovepipeApp` from the UI entry; use `StovepipePaths` for a custom mount path.
- Import `@submitqueue/web-stovepipe/styles.css` and supply BaseUI and Styletron providers. The host owns the theme, SSR stylesheet extraction, and hydration.
- For client-side routing, use `WebNavigationProvider` and honor `WebLinkProps.scroll`.
- For incremental lists, keep paging providers mounted across route changes and supply authenticated continuation loaders. See the [reference host's provider setup](../service/stovepipe/src/next/queue-provider.tsx).

See the [shared package contract](../README.md#layers) for bundling requirements and the [reference host](../service/stovepipe/README.md) for complete wiring and local startup. Shared [build and test commands](../README.md#build-test-run) apply to this package.

## Data semantics

- Missing results are unknown, not green. Recording completion does not imply validation success.
- Paginated listings are live, not snapshots.
