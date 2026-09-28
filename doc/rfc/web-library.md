# Web as a Library

## Decision

SubmitQueue publishes its browser UX as a library of components, gateway helpers, and presentation models. A deployer-owned Next application supplies routes, authentication, telemetry, configuration, and gateway connectivity.

The package resembles `submitqueue/client/`, while the host resembles service wiring. The library consumes the published gateway contract and maps it to presentation. Domain extensions stay behind the gateway.

Phase one is read-only: request summary, history, and queue receipt list ([status/list](submitqueue/status-list-api.md), [history](submitqueue/history-api.md)). Mutations require a separate authorization and audit design.

## Host/library boundary

| Library | Host |
|---|---|
| Gateway-to-presentation mapping, components, status and error display | Next application root, routes, and which subset to expose |
| Internal and external link helpers | Queue names, per-queue gateway routing, credentials, and deadlines |
| OpenTelemetry API instrumentation and a structural `Logger` | Session authorization, telemetry backends, configuration, and process lifecycle |

The host configures the queues it serves and selects each generated client with `(queue) => client`. It builds those clients with `@connectrpc/connect-node` `createGrpcTransport` over HTTP/2: TLS by default, plaintext h2c only as an explicit local option. It installs the library's tracing interceptor while constructing the transport. The gateway stays reachable only by trusted hosts.

Components render serializable props and do not fetch. Sessions, gateway clients, and protobuf messages stay on the server. Server helpers call `connection()` before gateway I/O, host routes that call them are dynamically rendered, and neither side caches gateway results.

`WebPaths` selects internal links. Each substituted value is unpadded base64url, so a slash inside an sqid (`<queue>/<counter>`) remains one path segment after a proxy decodes `%2F`. External links require a host mapping keyed by change-URI scheme and authority; other values render as text. Build URLs stay on history occurrences, because one request can have concurrent sibling builds.

`List` requires a queue and a half-open receipt window. A first visit uses a trailing 24-hour window, which the host may override. Both bounds live in the route's search parameters and stay fixed across refresh and paging, because the page token is valid only for that queue and those bounds. Changing the window drops the token.

A client component calls `router.refresh()` to rerun the server loaders. One refresh runs at a time. The wait is the terminal client's poll interval plus jitter, grows after a transport failure, and pauses while the document is hidden or the browser is offline. A request view stops on `landed`, `error`, or `cancelled`. A queue list keeps polling for the life of its fixed window.

## Package shape

The pnpm workspace lives under `web/`, with a nested `web/go.mod` so the Go build graph does not index it:

```
web/
├── api/                    # @submitqueue/api, one package for base and gateway stubs
├── submitqueue/            # @submitqueue/web-submitqueue
└── service/submitqueue/    # reference host
```

Generated TypeScript is committed beside the Go stubs. Gateway stubs and the base protos they import share one package, so the imports stay valid after publish. The library ships ESM at the root, `./server`, and `./testing`. The root preserves `'use client'` and reaches no `server-only` or Node-only code. React, Next, Connect, and Protobuf-ES are peers. Shared UI waits until a second domain needs it.

## Prerequisites

- Typed gRPC details for known errors. Helpers classify `InvalidArgument`, `NotFound`, and `ResourceExhausted` as user errors, `Unavailable` and `DeadlineExceeded` as transient infrastructure errors, and every other code as an infrastructure error. Raw gateway messages are not rendered.
- A typed build URL on history occurrences.
- Status values stay strings. The library has a display table and an unknown fallback, checked against a fixture in `api/submitqueue/gateway/testdata/`.
- Protobuf `int64` timestamps become numeric milliseconds in presentation models after a safe-range check.

## Acceptance

- Go and Vitest tests cover proto drift, the status fixture, timestamp conversion, stable list bounds, base64url paths, trusted links, and error classification.
- Component tests cover rendering and polling controls: single-flight, jitter and backoff, hidden and offline pause, and terminal stop.
- Playwright and axe-core cover browser accessibility, responsive and visual states, navigation, and stable focus and scroll.
- Every host route rejects a missing or denied session without relying on `proxy.ts`.
- A Node-owned Compose check, outside Bazel and in required checks, runs the reference host against the real grpc-go gateway as `e2e-submitqueue-web` ([testing guide](../howto/TESTING.md#container-naming)). It covers TLS and explicit h2c, and a slash-containing sqid fetched through the deployment proxy.
- `pnpm pack` tarballs install into an external Next app, match the root, `./server`, and `./testing` export map, keep `server-only` out of the root, and build at `next >=16.3 <17` with both supported React 19 bounds.
- Browser fakes are a build-time alias. The production artifact contains none.

## Rejected

- **A repository-owned application or container.** Another deployer would have to fork routes, authentication, telemetry, and transport.
- **Static export or embedding in a Go binary.** Request routes are dynamic, and the gateway stays private to a server that can authorize the caller.
- **A generic runtime, DI container, or web-extension layer.** Next's filesystem and the generated gateway client are the composition boundaries.
