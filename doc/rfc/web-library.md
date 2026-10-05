# Web as a Library

## Decision

SubmitQueue publishes its browser UX as a library of components, gateway helpers, and presentation models. A deployer-owned Next application supplies routes, authentication, telemetry, configuration, and gateway connectivity.

The package resembles `submitqueue/client/`, while the host resembles service wiring. The library consumes the published gateway contract and maps it to presentation. Domain extensions stay behind the gateway.

Phase one is read-only: request summary, history, and queue receipt list ([status/list](submitqueue/status-list-api.md), [history](submitqueue/history-api.md)). Mutations require a separate authorization and audit design.

## Host/library boundary

| Library | Host |
|---|---|
| Gateway-to-presentation mapping, components, status and error display | Next application root, routes, and which subset to expose |
| Internal link helpers and a structural gateway-diagnostic hook | Queue names, per-queue gateway routing, credentials, deadlines, and the diagnostic backend |
| Serializable view models and polling controls | Session authorization, configuration, and process lifecycle |

The host configures the queues it serves and selects each generated client with `(queue) => client`. It builds those clients with `@connectrpc/connect-node` `createGrpcTransport` over HTTP/2: TLS by default, plaintext h2c only as an explicit local option. It installs the library's tracing interceptor while constructing the transport. The gateway stays reachable only by trusted hosts.

Components render serializable props and do not fetch. Sessions, gateway clients, and protobuf messages stay on the server. Server helpers call `connection()` before gateway I/O, host routes that call them are dynamically rendered, and neither side caches gateway results.

`WebPaths` selects queue-scoped internal links. A queue landing page and request list live at `/<queue>` and a detail page at `/<queue>/request/<full-request-id>`. The `request` segment leaves room for other queue-scoped resources while the full request ID remains human-readable and opaque to the UI; the host's catch-all route only reassembles its URL path segments before passing it unchanged to the gateway. Phase one renders change URIs and build metadata as text; trusted external-link mapping and typed build URLs are deferred.

`List` requires a queue and a half-open receipt window. A first visit uses a trailing 24-hour window, which the host may override. Both bounds live in the route's search parameters and stay fixed across refresh and paging, because the page token is valid only for that queue and those bounds. Changing or resetting the window drops the token; the reference host links back to the queue's list route to establish a fresh trailing window.

A client component starts `router.refresh()` inside a React transition and does not schedule the next refresh until that transition finishes. The wait is the terminal client's poll interval plus jitter, grows across consecutive transport failures, and pauses while the document is hidden or the browser is offline. A request view stops only after the summary is terminal and successfully loaded history contains the same terminal status. A queue list keeps polling for the life of its fixed window.

## Package shape

The pnpm workspace lives under `web/`, with a nested `web/go.mod` so the Go build graph does not index it:

```
web/
├── api/                    # @submitqueue/api, one package for base and gateway stubs
├── submitqueue/            # @submitqueue/web-submitqueue
└── service/submitqueue/    # reference host
```

Generated TypeScript is committed beside the Go stubs. Gateway stubs and the base protos they import share one package, so the imports stay valid after publish. The library ships ESM at the root, `./server`, and `./testing`. The root preserves `'use client'` and reaches no `server-only` or Node-only code. React, Next, Connect, and Protobuf-ES are peers. Shared UI waits until a second domain needs it.

## Shipped compatibility contracts

- Helpers classify `InvalidArgument`, `NotFound`, and `ResourceExhausted` as user errors, `Unavailable` and `DeadlineExceeded` as transient infrastructure errors, and every other code as an infrastructure error. Raw gateway messages are not rendered. A host-supplied diagnostic hook receives the operation, request identity, Connect code, and original cause on the server.
- Status values stay strings. The library has a display table and an unknown fallback.
- Protobuf `int64` timestamps become numeric milliseconds in presentation models after a safe-range check.

## Acceptance

- Vitest tests cover proto drift, timestamp conversion, stable list bounds, queue-scoped readable paths, error classification, host-controlled deadlines, structured diagnostics, readiness configuration, and polling controls including transition-aware single-flight, progressive backoff, hidden/offline pause, and history-aware terminal stop.
- Every protected host layout and route repeats the session check rather than relying only on `proxy.ts`.
- A Node-owned Compose check, outside Bazel and in required checks, runs the reference host against the real grpc-go gateway as `e2e-submitqueue-web` ([testing guide](../howto/TESTING.md#container-naming)). It covers the Basic-auth challenge, explicit h2c, newest-first list navigation, a slash-containing sqid, lifecycle/build history, and axe-core checks on list and detail pages.
- `pnpm pack` tarballs install into an external TypeScript consumer and typecheck the root, `./server`, and `./testing` export map. The repository's Next 16 reference host production build separately verifies the framework integration.
- Browser fakes are a build-time alias. The production artifact contains none.

## Deferred

- Trusted change links and typed build-history links wait for a host-owned allowlist and a gateway field that distinguishes URLs from display metadata.
- OpenTelemetry interceptors and span APIs wait for a deployment that can establish naming and propagation conventions; phase one exposes only the structural diagnostic hook.
- A shared gateway status fixture waits until the wire contract owns status values rather than opaque strings.
- TLS transport E2E, responsive screenshot matrices, focus/scroll restoration, and the full supported React/Next version matrix are follow-up coverage. Phase one CI exercises h2c, axe-core, the pinned dependency set, and the declared package export surface.

## Rejected

- **A repository-owned application or container.** Another deployer would have to fork routes, authentication, telemetry, and transport.
- **Static export or embedding in a Go binary.** Request routes are dynamic, and the gateway stays private to a server that can authorize the caller.
- **A generic runtime, DI container, or web-extension layer.** Next's filesystem and the generated gateway client are the composition boundaries.
