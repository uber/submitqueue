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

Entry points: the root is client-safe (`SubmitQueueApp`, page frames, `WebNavigationProvider`, model types); `./server` has `createSubmitQueueWeb`, the gateway and cursor contracts, and `WebPaths`; `./extension/cursor/hmac` signs cursors; `./extension/gateway/mock` has test fakes; `./styles.css` holds scoped page layouts. Controllers and helpers stay internal.

**Hosting it.** Give `createSubmitQueueWeb` a gateway client (any client matching the structural contract, so the module ships no generated code), a `CursorCodec`, and optionally a mount path, authorization check, logger, meter, tracer, error classifier, or cached queue catalog. Route every page path to `handle`, render `SubmitQueueApp` with each page model, and wrap it in a `WebNavigationProvider` for client-side navigation. Hosts must load `./styles.css`, either by importing it or by serving it and linking to it.

**Base Web integration.** Render beneath the host's existing `baseui` `BaseProvider` and `styletron-react` `Provider`; the module creates neither and inherits the host's theme, layer management, and Styletron engine. Buttons, inputs, filters, status tags, table primitives, and headings use public Base Web components. The remaining stylesheet scopes page layouts to `.sq-app`, and its private color and font variables come from the active theme. Hosts choose light, dark, or a custom theme through `BaseProvider`; they must use the same theme when rendering and hydrating. The optional `SubmitQueueShell` adds standalone branding; an existing application can render `SubmitQueueApp` directly inside its own chrome.

The compiled package uses CommonJS with typed exports; ES module hosts can import it normally. Standard Base Web component paths resolve in Node and in source workspace bundlers.

React 18 and 19 are supported. Keep one shared instance of React, Base Web, and Styletron React in the host, satisfying the declared peer versions, including the supported source workspace version. Server-rendered hosts supply their normal Styletron style collection and hydration; the reference host in [`src/next/styles.tsx`](../service/submitqueue/src/next/styles.tsx) shows the Next.js integration. Gateway transport, auth, routing, and deployment remain host decisions.

**Behavior worth knowing.** Expected outcomes (missing resources, stale cursors) are results, never exceptions. Raw gateway errors are logged, never rendered. Views keep the last good data on transient failures. Request IDs stay opaque in URLs; dot-only segments get a visible `~` escape.

```bash
./tool/bazel test //web/submitqueue:all
```
