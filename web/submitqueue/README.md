# @submitqueue/web-submitqueue

The complete read-only SubmitQueue UI as a mountable module. Folders follow the Go domain layout, plus `view/` because Go has no UI layer.

| Folder | Holds |
|---|---|
| [`entity/`](entity/) | Serializable view models (requests, history, queues, changes, pages) and load results |
| [`core/`](core/) | Paths and `parseRoute`, change references, status table, receipt windows, observability helpers |
| [`extension/gateway/`](extension/gateway/) | The structural gateway contract, the gRPC-status classifier, and test fakes in `mock/` |
| [`extension/cursor/`](extension/cursor/) | The page-cursor contract and its HMAC implementation in `hmac/` |
| [`controller/`](controller/) | One loader per page: directory, queue, request, change |
| [`view/`](view/) | `SubmitQueueApp`, pages, the shell and small components, navigation, polling, and the stylesheet |
| [`module.ts`](module.ts) | `createSubmitQueueWeb`, which serves one page request end to end |

Entry points: the root is client-safe (`SubmitQueueApp`, page frames, `WebNavigationProvider`, model types); `./server` has `createSubmitQueueWeb`, the gateway and cursor contracts, and `WebPaths`; `./extension/cursor/hmac` signs cursors; `./extension/gateway/mock` has test fakes; `./styles.css` is the full stylesheet. Controllers and helpers stay internal.

**Hosting it.** Give `createSubmitQueueWeb` a gateway client (any client matching the structural contract, so the module ships no generated code), a `CursorCodec`, and optionally a mount path, authorization check, logger, meter, tracer, error classifier, or cached queue catalog. Route every page path to `handle`, render `SubmitQueueApp` with each page model, and wrap it in a `WebNavigationProvider` for client-side navigation. Hosts must load `./styles.css`, either by importing it or by serving it and linking to it; theme with the `--sq-*` properties.

**Behavior worth knowing.** Expected outcomes (missing resources, stale cursors) are results, never exceptions. Raw gateway errors are logged, never rendered. Views keep the last good data on transient failures. Request IDs stay opaque in URLs; dot-only segments get a visible `~` escape.

```bash
./tool/bazel test //web/submitqueue:all
```
