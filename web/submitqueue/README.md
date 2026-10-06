# SubmitQueue web library

`@submitqueue/web-submitqueue` is the reusable, read-only presentation library. It owns serializable request models, gateway-to-view-model loaders, safe timestamp conversion, public error classification, queue-scoped route helpers, React components, automatic refresh behavior, and deterministic gateway fakes.

The package has three deliberate entry points:

- `@submitqueue/web-submitqueue` contains client-safe models, components, status presentation, paths, and refresh controls.
- `@submitqueue/web-submitqueue/server` contains server-only gateway loaders and diagnostics.
- `@submitqueue/web-submitqueue/testing` contains reusable test fakes.

The library does not own routes, credentials, queue selection, transport creation, or deployment. Those decisions belong to a host such as `web/service/submitqueue/`. Queue-directory, request-table, Summary/History, and change-submission components consume serializable models. Change identity parsing and lookup selection remain host responsibilities; the components receive display fields and safe internal links.

The package has no Next.js dependency. Server loaders do not call framework lifecycle or cache APIs; a host must make gateway reads dynamic and uncached. `AutoRefresh` accepts a required refresh callback whose promise settles only after the host's refresh is complete. The demo's `NextRefresh` adapter owns `router.refresh()` and React transition completion. React, Connect, and Protobuf-ES remain peer contracts.

Request list/detail result views retain the last successful snapshot on transient failures and mark it stale. They clear it on permanent failures; hosts must key the view by its resource/window so snapshots are never carried to a different request or queue. Search applies only to the displayed request page, history filters preserve event order, and metadata remains uninterpreted text.

```bash
./tool/bazel test //web/submitqueue:test //web/submitqueue:typecheck_typecheck_test
./tool/bazel build //web/submitqueue:pkg
```

The package-consumer checks typecheck without Next.js and reject Next.js dependencies or imports in the emitted library.

`loadQueueDirectory` maps the gateway's `ListQueues` response into serializable queue models and sanitized errors. It uses a separate structural queue-reader interface so request-only adapters remain valid; the host supplies the client and decides where to render or validate the discovered catalog.

Normal queue/resource paths remain readable. Exceptional dot-only path segments use a visible `~` escape so browsers cannot normalize them away; `decodePathSegment` reverses that transport escape without interpreting an ID. Change-version navigation is an optional host callback over already supplied URLs, keeping router integration out of the library.
