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
- **Keep routing private to the host.** Per-queue gateway selection is a plain host function, not a library interface.
- **Let the host choose framework versions.** `react`, `react-dom`, and `next` are peer dependencies.

```
┌───────────────────────────── HOST ──────────────────────────────┐
│ app routes · proxy · auth · telemetry · config · transport      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ generated client, data, actions
                               ▼
┌──────────────────────────── LIBRARY ────────────────────────────┐
│ gateway helpers · param parsers · components · models · hrefs   │
│ NO env reads · NO listener · NO auth SDK · NO backend choices   │
└─────────────────────────────────────────────────────────────────┘
```

## Go analogy and its limits

The reusable web package most closely matches `submitqueue/client/`, not `pipeline.Construct`:

| Web | Go analogue |
|---|---|
| Gateway query helpers, paging, presentation models | `submitqueue/client/query.go`, `view.go`, and `watch.go` |
| Host route, auth checks, transport, and telemetry wiring | `service/.../main.go` |
| `(queue) => generated client` | host-private `Factory.For(Config{QueueName})` routing |

The analogy stops at the wire boundary. Go controllers take domain entities and keep protobuf mapping in the host. The web library intentionally consumes the published gateway contract, so gateway-to-presentation mapping and gRPC-status-to-display-error mapping belong in the library. All deeper domain extensions remain behind the gateway.

## Proposal

### Step 1 · Package layout

Start with one domain package and promote shared code only after another domain needs it:

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
│       ├── gateway/                  #   ./server: server-only RPC helpers
│       ├── component/                #   server and client UI components
│       ├── model/                    #   presentation models and capabilities
│       ├── params.ts                 #   route/search parameter parsing
│       ├── href.ts                   #   external and internal link helpers
│       └── testing/                  #   ./testing: fixtures and fake generated clients
├── service/
│   └── submitqueue/                  # reference host and Next application root

api/                                 # existing proto source + committed Go output
```

`web/` is a root-level artifact tree, like `api/`, `service/`, and `test/`. It contains the complete Node workspace, including the reference host, so dependency installation has one workspace boundary. pnpm may create package-local links, but all stay below `web/`. The Go domain and service trees remain Go-only.

TypeScript stubs are generated into the pnpm workspace so their runtime dependencies resolve there. Protobuf-ES and Connect-ES v2 use pinned `@bufbuild/buf` and `@bufbuild/protoc-gen-es` development dependencies; no separate Connect generator is needed. After the existing Bazel Go generation, `make proto` runs `pnpm --dir web exec buf generate` for TypeScript. A new `check-proto` target regenerates both Go and TypeScript output and fails on a dirty tree. The API package, `@bufbuild/protobuf`, and `@connectrpc/connect` use one compatible v2 line across the workspace.

The web library depends on `web/api/submitqueue/gateway`, not the Go code under `submitqueue/`. The cost is that one domain spans `submitqueue/`, `api/submitqueue/`, and `web/submitqueue/`; ownership and the Go/web hierarchies must remain aligned by convention.

`web/service/submitqueue/` is the reference composition root used by Docker Compose. Deployers reuse the package and maintain their own host.

### Step 2 · Gateway client and presentation helpers

The generated gateway client is the web library's only domain dependency. The library adds concrete helpers for presentation mapping, error classification, and telemetry; it does not redefine the gateway as an interface.

```ts
// web/submitqueue/src/gateway/request.ts
import type { Client } from '@connectrpc/connect'
import type { SubmitQueueGateway } from '@submitqueue/api-submitqueue-gateway'

export async function loadRequest(
  gateway: Client<typeof SubmitQueueGateway>,
  query: ByID,
  logger?: Logger,
): Promise<RequestPageResult>

export async function cancelRequest(
  gateway: Client<typeof SubmitQueueGateway>,
  command: CancelCommand,
  logger?: Logger,
): Promise<CancelResult>
```

The library obtains its meter through `metrics.getMeter("@submitqueue/web-submitqueue", version)` and never accepts an SDK or exporter. A logger is optional and typed as `pino.Logger`; `pino` is an optional peer because the emitted declarations reference its type.

The host constructs Node clients with `@connectrpc/connect-node` `createGrpcTransport`, which speaks gRPC over HTTP/2 to the existing grpc-go server. The transport is cached on `globalThis` so route and instrumentation bundles plus development hot reload share one instance. It is configured with TLS and bearer/deadline interceptors. The library exports an API-only Connect tracing interceptor so gateway calls nest under the active Next span. Host startup validates every configured gateway. Per-queue routing is a plain function:

```ts
type GatewayForQueue = (queue: string) => Client<typeof SubmitQueueGateway>
```

Cross-queue helpers take this function. Library tests use a fake generated client.

If a view needs domain data that the gateway does not expose, the gateway contract should grow. Phase one adds `ListQueues`, backed by the gateway's existing `queueconfig.Store.List`. A host spanning several gateways fans out over its configured gateway endpoints and associates each returned queue with that client.

Links are also concrete helpers:

```ts
export function changeHref(uri: string, config: WebConfig): Href | null
export function buildHref(buildURL: string): Href | null
export function requestHref(id: ByID, paths: WebPaths): Href | null
```

`WebPaths` contains serializable templates such as `/q/{queue}/r/{sqid}`. A missing template means the host did not expose that route, so the component renders no link. The gateway contract adds `build_url` to `HistoryEvent`, where each build occurrence belongs, and may mirror the current build URL on `RequestSummary`.

#### Telemetry

`@opentelemetry/api` is stable, works in Node and browsers, and is a no-op until the host registers a provider. Library packages depend only on that API. SDKs, exporters, registries, transports, and collector addresses remain host dependencies. Connect interceptors create client spans so gateway calls nest under Next request traces.

An incompatible OpenTelemetry API major silently becomes a no-op, so host startup asserts compatibility. Ephemeral/serverless hosts may flush through `after(...)`; the recommended long-running Node host does not flush per request.

### Step 3 · Host route composition

Next's filesystem is the route topology, so the library does not add a runtime engine or view registry. Each host route performs the composition work that Go service mains perform:

1. Resolve and authorize the host session.
2. Select and call the generated gateway client through library helpers.
3. Derive serializable capabilities and data.
4. Render a library component.
5. Provide host-owned server actions for enabled mutations.

```tsx
// web/service/submitqueue/app/request/[queue]/[sqid]/page.tsx
export default async function RequestRoute({ params }: RouteProps) {
  const parsed = parseRequestParams(await params) // params is a Promise in Next 16
  if (!parsed.ok) {
    return <InvalidRoute error={parsed.error} />
  }
  const route = parsed.value
  const session = await requireSession()
  await requireAllowed(session, Action.ViewRequest, route)

  const result = await loadRequest(gatewayForQueue(route.queue), route, logger)
  if (!result.ok) {
    return <RequestError error={result.error} />
  }

  return (
    <RequestPage
      data={result.value}
      capabilities={{
        request: {
          queue: route.queue,
          sqid: route.sqid,
          canCancel: false, // phase one is read-only
        },
      }}
    />
  )
}
```

Phase one is read-only. Cancel is enabled only after `CancelRequest` carries an additive actor field and the gateway records it in request history. A later host-owned action re-authenticates, authorizes the same queue/request resource, constructs identity fields from the trusted session rather than browser input, and returns a result union instead of throwing an expected error. Land remains out of scope until change-level authorization is defined.

Hiding a button is not authorization. The library receives resource-bound capabilities for presentation and callbacks for actions, not sessions or an authorization interface. List rows carry their own queue/request capabilities.

Routes remain hand-written host composition roots. A host may expose only a subset; missing `WebPaths` templates suppress links to unmounted routes. Separate route files preserve per-segment layouts, metadata, streaming, Suspense boundaries, and caching.

The library ships built ESM that preserves `"use client"` directives, with package exports for the default component surface, `./server`, and `./testing`. The reference host targets `next >=16.3 <17`—above the Server Actions CSRF security floor—and compatible React 19 releases. Gateway loaders are `server-only`, never cached, and convert proto messages before data reaches client components.

`proxy.ts` may perform an optimistic anonymous-user redirect, but every server component, route handler, and server action performs the real authorization check. Multi-instance hosts configure a shared `NEXT_SERVER_ACTIONS_ENCRYPTION_KEY` and `experimental.serverActions.allowedOrigins` when a proxy rewrites `Host`.

### Step 4 · Browser boundary

The host passes only serializable configuration, page data, and capabilities to client components. Sessions remain in the host:

```tsx
// web/service/submitqueue/app/layout.tsx (host-owned)
export default async function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html>
      <body>
        <WebProvider config={publicConfig()}>
          {children}
        </WebProvider>
      </body>
    </html>
  )
}
```

Phase one uses client `error.tsx` boundaries that show `error.digest`, plus `onRequestError`, which logs that digest with the active trace ID. Browser telemetry (`useReportWebVitals` or a dedicated endpoint) is added only when a concrete metric requires it; the library does not prescribe a second collector or browser credential path.

### Step 5 · Data and wire contracts

Gateway calls run only in server components, route handlers, or server actions. The browser holds a session cookie, never gateway credentials. The gateway is a private dependency reachable only by trusted hosts; otherwise host authorization can be bypassed.

`make proto` generates and commits TypeScript stubs under `web/api/`, mirroring `protopb/`. Protobuf-ES maps `int64` timestamps to `bigint`; gateway helpers validate safe range and convert them to `number` milliseconds in presentation models. Proto messages never cross into client components.

Gateway statuses are strings today, not an enum. The library owns a display table for color, ordering, and terminal state plus an explicit unknown-status fallback. A committed fixture under `api/submitqueue/gateway/testdata/` is checked by both Go and TypeScript tests so new `entity.RequestStatus*` values cannot silently miss web presentation; keeping it outside `web/` lets hermetic Go tests read it.

Before implementation, additive gateway changes provide:

- `ListQueues`
- typed gRPC status details for known errors
- typed `build_url` fields instead of an implicit metadata key
- an actor field for audited mutations before web Cancel is enabled

### Step 6 · Errors

Gateway helpers classify from gRPC codes and typed details, never from server messages:

| Codes | Display classification |
|---|---|
| `InvalidArgument`, `NotFound`, `ResourceExhausted` | actionable user error |
| `Unavailable`, `DeadlineExceeded` | transient infrastructure error |
| `Internal`, `Unknown`, unmapped | infrastructure error |

Unknown errors default to infrastructure errors, and raw gateway messages are never rendered. Gateway follow-up work attaches typed details; the current server only sends `err.Error()` and passes some unclassified errors through.

Helpers and server actions return result unions for expected outcomes rather than throwing them. Unexpected infrastructure failures reach a client `error.tsx` boundary, which shows only Next's `error.digest`. On the server, `onRequestError` logs the same digest with the active trace ID, route, and request context. Stack traces and reporting destinations remain hidden and host-owned.

### Step 7 · Host surface

| File | Content |
|---|---|
| `instrumentation.ts` | Node-guarded OpenTelemetry registration and `onRequestError` |
| `app/layout.tsx` | `<WebProvider>` + branding |
| `app/**/page.tsx` | authenticate, load through gateway helpers, and render library components |
| `app/**/action.ts` | authorize mutations and call gateway helpers |
| `proxy.ts` | optimistic anonymous-user redirect only |
| `server/gateway.ts` | `globalThis`-cached transports, clients, and `(queue) => client` routing |
| `server/auth.ts` | authoritative session and resource checks |
| `server/telemetry.ts` | `globalThis`-cached logger plus OpenTelemetry SDK registration |
| `server/config.ts` | validated host config and serializable display config |
| `next.config.ts` | standalone output, tracing root, and Server Action origins |
| `Dockerfile` | the host's image |

These files contain no presentation logic, gateway request mapping, or status definitions. Those remain in the library.

## What the library enforces

| Concern | Enforced by |
|---|---|
| No environment reads in the library | Lint rule banning `process.env` under `web/submitqueue`; CI gate |
| No gateway client in browser bundles | `import 'server-only'` in the `./server` export |
| No unsafe external URLs | Every rendered external href passes an HTTP/HTTPS scheme allowlist |
| Status vocabulary drift | Shared fixture tests the string display table in Go and TypeScript |
| Host controls framework versions | `react` / `next` are `peerDependencies`, never dependencies |
| One protobuf runtime | API package, Connect, and Protobuf-ES versions are aligned in the workspace |
| No telemetry backend in the library | `pino` is an optional type peer and `@opentelemetry/api` is a no-op peer; SDKs and exporters are forbidden in library packages |

Authorization is host-owned and therefore not enforceable by the library. Host route tests must verify that reads and mutations reject unauthorized sessions; hiding a component based on capabilities is never treated as enforcement.

## Host telemetry

`instrumentation.ts` is a separate bundle entry, so shared process objects are cached on `globalThis` rather than relying on module-instance singletons:

```ts
export async function register() {
  if (process.env.NEXT_RUNTIME !== 'nodejs') return
  const { registerTelemetry } = await import('./server/telemetry')
  await registerTelemetry()
}

export const onRequestError: Instrumentation.onRequestError = async (
  error,
  request,
  context,
) => {
  if (process.env.NEXT_RUNTIME !== 'nodejs') return
  const { reportRequestError } = await import('./server/telemetry')
  const digest =
    typeof error === 'object' && error !== null && 'digest' in error
      ? String(error.digest)
      : undefined
  reportRequestError({
    error,
    digest,
    route: context.routePath,
    request,
  })
}
```

`server/telemetry.ts` owns pino and SDK construction through `globalThis` caches. The OpenTelemetry provider survives through the API global, and the library obtains its meter from that API. `reportRequestError` logs the supplied digest with the active trace ID.

## Testing

```ts
import pino from 'pino'

const data = await loadRequest(
  fakeGateway({ 'demo/1': summary({ status: 'landing' }) }),
  { queue: 'demo', sqid: 'demo/1' },
  pino({ level: 'silent' }),
)

if (!data.ok) throw new Error('test fixture must load')
render(<RequestPage data={data.value} capabilities={requestCapabilities('demo', 'demo/1')} />)
```

The fake implements the generated client shape. Proto fixtures include `bigint` timestamps, but presentation fixtures contain only serializable values. Playwright end-to-end tests use the domain-qualified compose context `e2e-submitqueue-web` ([testing guide](../howto/TESTING.md#container-naming)).

## Deployment model

The supported deployment is a long-running Node host with server-side gateway access. `next.config.ts` uses `output: "standalone"` and `outputFileTracingRoot` points at `web/`; the Docker build context is `web/`. Hosts behind a rewriting proxy set `experimental.serverActions.allowedOrigins`.

## Build and CI

The `web/` pnpm workspace adds these Makefile targets to the existing CI gates:

```makefile
check-proto: ## Check committed Go and TypeScript protobuf files
web-build: ## Build all web packages
web-e2e-test: ## Run Playwright web end-to-end tests
web-lint: ## Lint and typecheck web packages
web-test: ## Run web unit tests
```

Targets remain alphabetically sorted with `## Description`; `make lint` and `make test` include their web equivalents.

Phase one does not use Bazel for TypeScript or Playwright. Bazel 8's root `REPO.bazel` and the root Gazelle directive exclude all of `web/`; web tests run through Make/pnpm and feed the same required-checks gate. `web/go.mod` keeps the root Go module from traversing Node dependencies. This choice can be revisited if hermetic Bazel web tests become necessary.

`web/package.json` pins pnpm through `packageManager`, and `.nvmrc` pins Node. The library publishes built ESM and declares `next >=16.3 <17`, compatible React 19, the generated API package, Connect v2, Protobuf-ES v2, and optional pino types as peers.

## Rejected

- **Container image instead of a library.** Hosts could not replace authentication, telemetry, or transport without a permanent source fork.
- **One catch-all route.** `app/[[...slug]]/page.tsx` would replace Next routing and lose per-segment layouts, metadata, streaming, Suspense boundaries, and caching.
- **Static export or embedding in a Go binary.** Dynamic request routes cannot be enumerated, server actions and authoritative route checks disappear, and the grpc-go gateway needs a browser-reachable proxy with its own resource authorization. Revisit only with a separate design.
- **Web extension interfaces.** The gateway is already the domain contract. Extra `Gateway`, `GatewayFactory`, `Authorizer`, and generic `Deps` interfaces duplicate that contract or turn host authentication into library policy.
- **A generic `web/platform` engine.** Next's filesystem already defines route topology, and only one web domain exists. Add shared platform code only after a second domain demonstrates the common behavior.
- **Library-owned auth, logger, or metrics backends.** Token sources, clients, timeouts, transports, and registries are process infrastructure. Shipping them also adds unused dependencies to every host.
- **Bespoke telemetry interfaces.** OpenTelemetry already provides a stable, no-op metrics API. A type-only `pino.Logger` is structurally compatible with other loggers without an adapter or runtime dependency.
- **Environment-based library defaults.** `NEXT_PUBLIC_GATEWAY_ADDR` or similar defaults create a hidden configuration surface.
- **GraphQL or REST BFF.** Server components already provide the BFF and the generated gateway client is the domain boundary; another wire contract adds no capability.
- **DI framework.** Host route modules are explicit composition roots and do not need a runtime graph.
- **Shared Go/TypeScript view model through WASM or code generation.** A cross-language status fixture catches drift; the remaining presentation code is cheaper than a cross-language runtime.
- **Non-Next SPA.** Server components keep gateway access and authorization on the server; an SPA moves both into the browser.

## Migration path

The first phase makes additive gateway changes without changing existing behavior:

1. Add gateway contracts required by the UI (`ListQueues`, attached typed error details, typed `build_url`) and generate committed TypeScript packages under `web/api/`; add Go/TypeScript round-trip and status-fixture checks.
2. Add the read-only `web/submitqueue` package with gateway helpers, models, components, parameter parsers, href helpers, and fakes; add unit and screenshot tests.
3. Add the read-only `web/service/submitqueue` reference host and a `web-ui` service to `service/submitqueue/docker-compose.yml`, depending on `gateway-service`; add `e2e-submitqueue-web`.
4. Add an actor to `CancelRequest`, persist it in request history, and then add the host-authorized Cancel action. Land remains deferred until change-level authorization is designed.
5. Build another host from the published package. Promote shared code only when that host or another domain demonstrates a reusable need.

## Open questions

- **Live updates.** Phase one uses polling with server revalidation, matching `client/watch.go`. Server-sent events could follow; a gateway `Watch` RPC needs a separate RFC.
- **`List` filtering.** The [status/list RFC](submitqueue/status-list-api.md) deferred status filters. The first queue view must either filter a receipt-time window or extend the gateway.
- **Authorization capabilities.** Finalize resource-bound capability types for queue pages and request rows. Phase one exposes read capabilities; Cancel follows its actor contract, and Land follows change-level authorization.
- **Initial scope.** Decide whether the first app contains only SubmitQueue or also Stovepipe history. Each domain uses its published RPC contract; Runway currently exposes only `Ping`, so a Runway view requires a new API. A second domain may justify shared UI helpers.
- **OTLP compatibility.** Determine whether intended host metrics backends accept OTLP. Otherwise those hosts must provide a `MetricReader`, making their wiring less similar to the reference host.
- **npm publishing.** The proposed `@submitqueue/*` packages require an npm organization and release cadence.
