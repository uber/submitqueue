# Web as a Library

SubmitQueue should publish its web UX as a library of components, gateway helpers, and presentation models. A deployer-owned Next application supplies authentication, telemetry, configuration, routes, and gateway connectivity.

## Problem

There is no browser UX today. The gateway already exposes the required read APIs: `GetRequestSummaryByID`, `GetRequestSummaryByChangeURI`, `List`, `GetRequestHistoryByID`, and `GetRequestHistoryByChangeURI` ([status/list](submitqueue/status-list-api.md), [history](submitqueue/history-api.md)). It also exposes `Cancel`, which needs an actor/audit contract before the web host uses it. `submitqueue/client/` already renders the read data in a terminal.

A repository-owned Next application would couple the reusable UX to one deployment's environment variables, authentication, telemetry, and gateway routing. Unlike a Go deployer, who can import the library and replace `service/.../main.go`, a Next deployer would have to fork the application root (`app/`, `proxy.ts`, and `next.config.ts`).

Hosts supply these deployment-specific inputs:

| Concern | The host supplies | Wired with |
|---|---|---|
| Authentication and authorization | the caller session and allowed actions | host routes, server actions, and an optional optimistic `proxy.ts` redirect |
| Logging | a `pino.Logger`, or any structurally compatible object | `pino` plus `pino-pretty`, `pino-opentelemetry-transport`, or another host-selected transport |
| Metrics | a registered global OpenTelemetry `MeterProvider`, or none | `@opentelemetry/sdk-metrics` plus a Prometheus or OTLP exporter |
| Gateway transport | a generated gateway client for each queue | Connect transport configured for the host's gateway address, credentials, and edge |
| Configuration | branding, external-host mappings, and internal path templates | a serializable `WebConfig` populated by the host |

Keeping these inputs outside the library allows the same UX to run in multiple hosts. This also matches the gateway's existing security boundary: `submitqueue/client/conn.go` can send a bearer token, but this repository does not validate it. Authentication belongs to the proxy, sidecar, ingress, or host in front of the gateway.

Change and build links are concrete helpers over gateway data. Change URIs use `scheme://{host[:port]}/{path}` with a mandatory host and must be interpretable without deployment wiring ([change URIs](change-uri.md)). Build runners already write the provider's web URL into request metadata. Every rendered external link is validated against an HTTP/HTTPS allowlist; `git://` and deployments with different browser hosts use explicit `WebConfig` mappings.

## Runtime boundary

Unlike a Go service, a Next application runs in three environments:

```
build    route discovery, bundling, and static generation; no host process
server   Node server components, route handlers, server actions, and proxy
browser  client components on another machine; serializable data and opaque
         server-action references only
```

Next owns the request loop and bundler, so the library cannot own "no server" in the literal Go sense. Instead, it owns no process and no application root. The host owns the Next app root and resolves server dependencies once per process. Only serializable data, capabilities, and opaque server-action references cross into client components.

## Design principles

- **Use the gateway as the domain boundary.** Web code reads and mutates SubmitQueue only through the published gateway API. Missing domain data should extend that API.
- **Keep deployment policy in the host.** The library does not read environment variables, open listeners, import auth SDKs, choose telemetry exporters, hardcode external URLs, or exit the process.
- **Keep server dependencies on the server.** Host objects are never serialized into client components.
- **Compose with data and callbacks.** Host routes pass serializable data, capabilities, and authorized server actions to library components.
- **Keep gateway routing private to the host.** Per-queue gateway selection is a plain host function, not a library interface.
- **Let the host choose framework versions.** `react`, `react-dom`, and `next` are peer dependencies.

## Go analogy and its limits

The reusable web package most closely matches `submitqueue/client/`, not `pipeline.Construct`:

| Web | Go analogue |
|---|---|
| Gateway query helpers, paging, presentation models | `submitqueue/client/query.go`, `view.go`, and `watch.go` |
| Host route, auth checks, transport, and telemetry wiring | `service/.../main.go` |
| `(queue) => generated client` | host-private `Factory.For(Config{QueueName})` routing |

The analogy stops at the wire boundary. Go controllers take domain entities and keep protobuf mapping in the host. The web library intentionally consumes the published gateway contract, so gateway-to-presentation mapping and gRPC-status-to-display-error mapping belong in the library. All deeper domain extensions remain behind the gateway.

## Design

### Package boundary

Start with one domain package. Promote shared code only after another domain needs it:

```
web/                                  # pnpm workspace root
├── go.mod                            # exclude Node dependencies from root Go ./...
├── .nvmrc
├── package.json
├── pnpm-lock.yaml
├── pnpm-workspace.yaml
├── api/                              # committed protobuf-es/Connect output
│   ├── base/change/
│   ├── base/mergestrategy/
│   └── submitqueue/gateway/          # @submitqueue/api-submitqueue-gateway
├── submitqueue/                      # @submitqueue/web-submitqueue
│   └── src/
│       ├── gateway/                  #   server-only RPC helpers
│       ├── component/                #   server and client UI components
│       ├── model/                    #   presentation models and capabilities
│       ├── params.ts                 #   route/search parameter parsing
│       ├── href.ts                   #   external and internal link helpers
│       └── testing/                  #   fixtures and fake generated clients
└── service/
    └── submitqueue/                  # reference host and Next application root
```

`web/` contains the complete Node workspace, including committed Protobuf-ES/Connect output and the reference host; proto sources and Go output stay in the root `api/` tree. `web/go.mod` and repository exclusions keep Node dependencies outside Go and Gazelle traversal. The web library depends on the generated API package, not Go domain code.

The package ships built ESM with default component exports plus `./server` and `./testing`. React, Next, Connect, Protobuf-ES, and optional pino types are peers so host and library share one runtime. `web/service/submitqueue/` demonstrates composition; deployers own their hosts.

Phase one contains SubmitQueue only. Stovepipe can later add `web/stovepipe/` against its published RPCs; Runway needs a read API beyond `Ping` before it can expose UX. Shared UI is promoted only after that second domain exists.

### Gateway boundary

The generated gateway client is the library's only domain dependency. Concrete helpers map gateway responses into presentation models; there is no web-specific gateway interface.

Gateway helpers take `Client<typeof SubmitQueueGateway>`, typed queries, and an optional `pino.Logger`; expected outcomes use result unions. The library obtains its meter through `metrics.getMeter("@submitqueue/web-submitqueue", version)` and never accepts an SDK or exporter.

The host constructs Node gRPC clients, supplies credentials and deadlines, and routes queues with a plain `(queue) => generated client` function. Cross-queue helpers take that function. The library supplies presentation/error mapping, tracing instrumentation, and a fake generated client for tests.

If a view needs missing domain data, the gateway contract grows instead of adding a web-only backend. Phase one adds `ListQueues`, backed by `queueconfig.Store.List`; a multi-gateway host fans it out across configured endpoints and rejects its configuration at startup if two gateways report the same queue name.

Phase one refreshes views with a small client component that calls `router.refresh()` on an interval, re-running the server loaders, matching `submitqueue/client/watch.go`. It presents the bounded receipt-history `List` without status filtering. Streaming or filtered gateway APIs require separate contract changes.

Link helpers derive change and build URLs from gateway data and internal links from serializable `WebPaths` templates such as `/q/{queue}/r/{sqid}`. A missing template suppresses the link. `build_url` is added to `HistoryEvent`, where each build occurrence belongs, and may be mirrored on `RequestSummary` for the current build.

The library depends only on `@opentelemetry/api` and obtains its own meter. SDKs, exporters, collector addresses, and logger construction remain host concerns. A Connect interceptor nests gateway spans under the active Next request trace.

### Host composition

Next's filesystem is the route topology; there is no runtime engine or view registry. Each host route:

1. Resolve and authorize the host session.
2. Select and call the generated gateway client through library helpers.
3. Derive serializable capabilities and data.
4. Render a library component.
5. Provide host-owned server actions for enabled mutations.

Route params are untrusted and parsed before lookup. Phase one is read-only. Cancel follows only after `CancelRequest` records an actor; its action re-authenticates, authorizes the same queue/request, and derives identity from the trusted session rather than browser input. Land remains out of scope until change-level authorization is defined.

Hiding a button is not authorization. The library receives resource-bound capabilities for presentation and callbacks for actions, not sessions or an authorization interface. List rows carry their own queue/request capabilities.

Library components are synchronous and render only their props; they never fetch. Data loading lives in gateway helpers called by host routes, which keeps the boundary explicit and lets components be tested without a server.

Routes remain hand-written composition roots, and hosts may expose subsets. `WebPaths` controls internal links so unmounted routes are not advertised. Gateway loaders are `server-only` and uncached. Only serializable models, resource-bound capabilities, and opaque server-action references cross to client components; sessions, proto messages, and gateway clients do not.

The reference host targets `next >=16.3 <17` and compatible React 19 releases. `proxy.ts` may optimize anonymous redirects, but routes and actions perform authoritative checks.

### Wire compatibility

Gateway calls stay on the server, and the gateway remains reachable only by trusted hosts; otherwise host authorization can be bypassed.

`make proto` generates and commits TypeScript stubs under `web/api/`, mirroring `protopb/`. Protobuf-ES maps `int64` timestamps to `bigint`; gateway helpers validate safe range and convert them to `number` milliseconds in presentation models. Proto messages never cross into client components.

Gateway statuses are strings today, not an enum. The library owns a display table for color, ordering, and terminal state plus an explicit unknown-status fallback. A committed fixture under `api/submitqueue/gateway/testdata/` is checked by both Go and TypeScript tests so new `entity.RequestStatus*` values cannot silently miss web presentation; keeping it outside `web/` lets hermetic Go tests read it.

Before implementation, additive gateway changes provide:

- `ListQueues`
- typed gRPC status details for known errors
- typed `build_url` fields instead of an implicit metadata key
- an actor field for audited mutations before web Cancel is enabled

### Error boundary

Gateway helpers classify from gRPC codes and typed details, never from server messages:

| Codes | Display classification |
|---|---|
| `InvalidArgument`, `NotFound`, `ResourceExhausted` | actionable user error |
| `Unavailable`, `DeadlineExceeded` | transient infrastructure error |
| `Internal`, `Unknown`, unmapped | infrastructure error |

Unknown errors default to infrastructure errors, and raw gateway messages are never rendered. Expected outcomes use result unions. Unexpected failures reach `error.tsx`, which shows only Next's digest; `onRequestError` logs that digest with trace and request context.

### Ownership invariants

- Host: routes, authorization, gateway construction/routing, telemetry SDKs, configuration, branding, process lifecycle, and deployment.
- Library: gateway mapping, presentation models, components, capabilities, status display, safe links, and display errors.
- `./server` imports `server-only`; library code never reads environment variables or chooses backends.
- External links pass an HTTP/HTTPS allowlist; status fixtures prevent Go/TypeScript drift.
- Committed Go and TypeScript proto outputs are regenerated in a clean-tree drift check.
- Authorization is enforced and tested in host routes/actions. Capabilities only control presentation.

The supported deployment is a long-running Node host with private gateway access. `web/` stays outside Bazel/Gazelle while remaining inside required checks.

## UX testing strategy

| Layer | Tool | What it verifies |
|---|---|---|
| Contract/model | Go tests + Vitest | Proto round trips and drift, status compatibility, `bigint` conversion, paging, href safety, error mapping, and client props containing no proto markers or `bigint` |
| Component behavior | Vitest + React Testing Library | Rendering states, interactions, resource capabilities, and keyboard/focus behavior |
| Browser UX | Playwright + axe-core | Responsive and visual regression, real-browser accessibility/contrast, non-color-only statuses, navigation, loading/empty/error states, and stable focus/scroll during polling |
| Host security | Route matrix generated from `app/` + action lint | Every discovered route has an authorization case and rejects missing/denied sessions without `proxy.ts`; a lint rule requires every `'use server'` export to call the authorization helper, and actions reject cross-resource arguments when called directly |
| Host integration | Playwright + Docker Compose | Real gateway list/history flows, polling, transport failures, and later mutation authorization |

Browser UX tests use a separate build-time entry point that aliases the reference host's gateway and session providers to fakes; production builds contain neither fake, and a build check verifies the production artifact. Fixtures cover scripted status changes, unknown statuses, long and numerous URIs, long errors, full pages, omitted path templates, and per-row capabilities. Screenshots are deterministic, and the lowest and latest supported Next/React versions run the component and browser suites.

End-to-end tests use the real gateway in the domain-qualified `e2e-submitqueue-web` context ([testing guide](../howto/TESTING.md#container-naming)). Storybook is not required initially because the fixture mode already renders deterministic browser states.

## Rejected

- **Container image instead of a library.** Hosts could not replace authentication, telemetry, or transport without a permanent source fork.
- **One catch-all route.** `app/[[...slug]]/page.tsx` would replace Next routing and lose per-segment layouts, metadata, streaming, Suspense boundaries, and caching.
- **Static export or embedding in a Go binary.** Dynamic request routes cannot be enumerated, server actions and authoritative route checks disappear, and the grpc-go gateway needs a browser-reachable proxy with its own resource authorization. Revisit only with a separate design.
- **Web extensions, DI, or a generic platform engine.** The gateway and Next filesystem already provide domain and route boundaries. Extra interfaces or runtime assembly duplicate them; shared code is promoted only after another domain demonstrates reuse.
- **Library-owned process infrastructure.** Auth SDKs, logger/metrics backends, environment defaults, token sources, clients, and registries belong to the host. OpenTelemetry and optional pino types already provide the needed contracts.
- **GraphQL or REST BFF.** Server components already provide the BFF and the generated gateway client is the domain boundary; another wire contract adds no capability.
- **Shared Go/TypeScript view model through WASM or code generation.** A cross-language status fixture catches drift; the remaining presentation code is cheaper than a cross-language runtime.
- **Non-Next SPA.** Server components keep gateway access and authorization on the server; an SPA moves both into the browser.
