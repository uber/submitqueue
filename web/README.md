# SubmitQueue web

A read-only browser UI for SubmitQueue, shipped as a **module** any server can mount. The module owns the whole UX (routing, data loading, pages, styles, navigation, polling); the **host** that mounts it owns infrastructure (identity, gateway transport, logging, metrics, framework) and the Base Web theme and Styletron engine. Design decisions are in [the RFC](../doc/rfc/web-ui.md).

## Layers

```
service/submitqueue   the host: Next.js app + wiring (like a Go service's main.go)
        │ mounts
submitqueue           the SubmitQueue module: routes, controllers, pages, styles
```

Dependencies only point down. The module never imports generated protos, a transport, or a web framework; only the host chooses implementations. A package test checks this against every file in the built package.

## How a page is served

1. Next.js routes every path to one catch-all page in the host.
2. The host authenticates the viewer and calls the module's `handle(path)`.
3. The module strips its mount path, parses the rest into a route, applies the host's optional authorization, and loads the route through its controllers and the gateway client the host injected.
4. The result is a redirect, not-found, forbidden, or a serializable page model; the host renders the model with `SubmitQueueApp`.
5. In the browser, `SubmitQueueApp` handles links, refresh, and polling through the host's navigation adapter.

## Layout

| Path | What it is |
|---|---|
| [`submitqueue/`](submitqueue/) | `@submitqueue/web-submitqueue`: the module, laid out like a Go domain (`entity`, `core`, `extension`, `controller`) plus `view` |
| [`service/submitqueue/`](service/submitqueue/) | The reference host: `src/server` (wiring), `src/next` (Next.js adapter), `src/app` (routes), and its OCI image |
| [`api/`](api/) | Private generated gateway bindings, used by the host and the end-to-end test |
| [`test/`](test/) | End-to-end browser test and package checks |
| [`tool/`](tool/) | Bazel rule for protobuf generation |

## Build, test, run

Bazel is the build (it pins Node 24.8.0, matching Base Web's Node 24 requirement); the pnpm workspace exists only for editors (`make web-install`).

```bash
make web-check                 # unit, type, lint, drift, and package checks
make web-e2e-test              # real stack in Docker + Playwright + axe
make local-submitqueue-start   # full local stack; prints the web URL (user test, password test)
make web-proto                 # after changing gateway .proto files
```
