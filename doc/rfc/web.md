# Web/UI Design

## Problem

Operators have no browser UI. The CLI polls gateway history. Gateway already exposes request summary, history, and receipt-time `List` ([status-list-api.md](submitqueue/status-list-api.md), [history-api.md](submitqueue/history-api.md)); Stovepipe exposes a parallel history API ([stovepipe/request-history-api.md](stovepipe/request-history-api.md)). Those APIs are gRPC, which a browser cannot call directly.

The first UI is read-only: choose a queue, list requests, open summary and history, search by change URI, and poll while a request is in flight. The design should also leave room for richer operator workflows without replacing the frontend stack.

A **Backend for Frontend (BFF)** is an HTTP server dedicated to one UI. The browser sends HTTP/JSON to the BFF; the BFF calls the domain's gRPC API. It is not a public API or a browser gRPC client.

## Decision

Each domain that needs a UI owns a Node.js/React application built as a Next.js App Router BFF. SubmitQueue starts at `submitqueue/web`; Stovepipe may add `stovepipe/web` when needed. There is no repository-root web application.

This stack is chosen over Go templates because JavaScript and TypeScript provide a broader UI ecosystem: accessible component libraries, client-side caching, polling, virtualization, forms, charts, and established browser testing. Those capabilities matter as the operator surface grows beyond its first read-only views. Cadence Web v4 is also a deployed example of the same Next.js, React, gRPC, and overlay model.

The cost is a Node runtime and frontend toolchain alongside Go. Production also needs a thin overlay because Uber's service-specific identity, authorization, deployment, and observability wiring does not belong in this OSS repository. The overlay changes platform seams, not product views.

## Architecture

```
Browser
   |  HTTP/JSON
   v
Next.js BFF ({domain}/web)        platform/web
   |  React views                  auth shape, Pino, OTEL,
   |  route handlers               gRPC helpers
   |  @grpc/grpc-js
   v
Domain gRPC API
   |  gateway or Stovepipe
   |  authorizes the caller identity before any data
   v
Controllers and stores


This repository                       Production overlay
---------------                       ------------------
{domain}/web       product app <----  pinned OSS version
platform/web       shared hooks <---- replaced platform adapters
service/{domain}/web example host     identity, policy, exporters, deploy
```

The browser never calls gRPC or YARPC. Next route handlers run in the Node runtime and call the domain's published service with `@grpc/grpc-js` and `@grpc/proto-loader`.

The OSS example gateway uses `google.golang.org/grpc`. Uber's SubmitQueue gateway is hosted with YARPC and exposes a gRPC inbound. The Node BFF dials that gRPC inbound; it does not use a YARPC Node client. Generated `*.pb.yarpc.go` remains Go-host code.

The BFF calls only the owning domain's API. SubmitQueue uses `SubmitQueueGateway`; a Stovepipe UI uses `Stovepipe`. Orchestrator and Runway are not UI backends.

## Repository layout

```
platform/web/                 shared Node hooks; no React or domain views
submitqueue/web/              SubmitQueue Next.js application
stovepipe/web/                Stovepipe application when needed
service/submitqueue/web/      example Dockerfile and environment
service/stovepipe/web/        example host when needed
```

`{domain}/web` is the executable application (`next start`). It owns:

- `src/app`: pages and API routes;
- `src/route-handlers`: HTTP-to-gRPC behavior;
- `src/views`: React views;
- generated TypeScript proto-loader types for that domain.

Views call HTTP route handlers through TanStack Query; they do not call gRPC. Route handlers are explicitly Node runtime handlers because `@grpc/grpc-js` cannot run on the Next Edge runtime.

`platform/web` contains reusable Node plumbing: auth strategy and user-info contracts, route-gate helpers, Pino setup, OpenTelemetry registration, and gRPC/proto-loader helpers. It imports no domain application and has no React dependency.

`service/{domain}/web` is the runnable OSS example: Dockerfile, compose configuration, example peer address, ports, and auth setting. It runs the application from `{domain}/web`; it does not contain another copy of pages or handlers.

Web packages have their own npm lockfiles, lint, typecheck, and Jest jobs. Gazelle and Go tests do not traverse npm trees. This repository's `.proto` files remain the wire-contract source of truth; generated TypeScript types are derived from them.

## Read model

The first SubmitQueue UI mounts BFF routes for queue listing, request `List`, request summary, and history. `Land`, `Cancel`, and other writes are not mounted. Queue listing, `List`, summary, and history are data RPCs.

Queue selection depends on a gateway `ListQueues` RPC over `queueconfig.Store.List` ([QueueConfig](../../submitqueue/entity/queue_config.go)). This RPC must land before the UI. The BFF does not maintain queue names in environment variables or checked-in configuration.

All request reads remain queue-scoped. Cross-queue request listing is outside the gateway contract. Live status uses polling, matching `submitqueue/client` watch; there is no streaming status RPC. Stovepipe request IDs remain distinct from SubmitQueue sqids.

Adding writes requires a follow-up RFC covering operation authorization, audit behavior, and CSRF protection for cookie-authenticated browser requests.

## Technology

- TypeScript in strict mode.
- Node 24.x, matching the current Cadence Web compatibility band.
- Next.js App Router 14.x and React 18.
- Base Web with Styletron for components and styling.
- TanStack Query v5 for server-state caching, polling, and pagination.
- Zod for HTTP input validation.
- `@grpc/grpc-js` and `@grpc/proto-loader` for BFF-to-service calls.
- Pino for server logs.
- OpenTelemetry Node SDK for metrics and traces.
- Jest, Testing Library, and JSDOM/Node test environments.
- npm with a committed lockfile per package.

Major upgrades to Next, React, or Node are compatibility changes for production overlays and should be coordinated with the overlay.

## Authentication and authorization

The domain service is the authoritative authorization boundary. Every data RPC from the BFF carries the end-user identity; the gateway (or Stovepipe) authenticates that identity and authorizes the call before returning any data. A BFF service credential is not a substitute for the caller: the service may use it to prove that the BFF is a trusted client, but authorization is always for the forwarded user.

Missing, invalid, or unauthorized identity fails the RPC. The BFF must not filter, redact, or invent results to paper over a denied call. Optional UI hiding of queues or actions is presentation only.

`platform/web` defines replaceable browser-identity and credential-forwarding hooks:

1. An auth strategy resolves at process start. The example host uses `disabled` so local compose can call the example gateway without a platform identity. A production overlay supplies its platform strategy and must forward the authenticated user on every data RPC.
2. User-info middleware maps the HTTP request to that identity. The OSS JWT strategy transports a cookie and gRPC metadata but does not claim to verify platform identity.
3. A route gate can reject browser requests without identity before dialing gRPC. This is defense in depth, not the service ACL.
4. The gRPC client attaches the end-user identity (and any required BFF client credential) on every data RPC. Health and metrics are not data RPCs; they follow deployment-platform access policy rather than browser-route policy.

## Logging

The BFF uses Pino for structured server logs. Request logs include route, gRPC outcome, queue, and identity when available. Stdout is the deployment contract, allowing an overlay to connect the process to its log pipeline without changing views.

There is no built-in browser `/api/log` ingest. A deployer that needs browser error reporting may add an authenticated route in its overlay.

## Metrics and tracing

`platform/web` registers the OpenTelemetry Node SDK before route handlers load. The registration hook accepts instrumentations, a metric reader, span processors, and resource attributes.

HTTP, gRPC, runtime, and Pino instrumentations provide request metrics, client spans, runtime signals, and log/trace correlation. Local compose can disable the SDK with `OTEL_SDK_DISABLED`.

The production overlay maps these hooks to Uber infrastructure in the same shape as Cadence Web:

- Pino remains on stdout and receives trace correlation from OTEL instrumentation.
- An OTEL Prometheus metric reader exposes scrape text on a platform-restricted route; the platform scrape feeds M3, which uMonitor dashboards and alerts query.
- Resource attributes include the service and environment tags expected by the metrics platform.
- An OTLP span processor sends traces to the local collector sidecar.

The metrics route is exempt from browser identity middleware only when deployment policy restricts it to the platform scraper. It is not a public browser endpoint.

Go services retain zap and tally. The web process has its own Node observability lifecycle.

## Production overlay

The production host is a Node overlay repository that pins an OSS SubmitQueue version and combines `{domain}/web` with `platform/web`. It contains identity, forwarding of the authenticated user onto every data RPC, policy, OpenTelemetry exporters, website deployment configuration, and platform port mapping. Product pages and route handlers remain in OSS. The overlay does not authorize queue or request access; the domain service does.

The overlay is not an application in Uber's web monorepo:

- product applications there use Fusion, whose plugins do not mount onto `next start`;
- its Next.js applications are documentation/static sites rather than gRPC BFFs;
- rewriting the UI in Fusion would create a second product instead of deploying the OSS application.

The Go service monorepo continues to host gateway and Stovepipe. The Node overlay calls those services; it does not run inside their Go processes.

## Alternatives considered

Three architecture families were evaluated:

1. **Next.js BFF with React and an overlay (chosen).** One frontend runtime owns views and the HTTP-to-gRPC boundary. It has the richest UI ecosystem and follows the deployed Cadence Web model.
2. **Go `templ`/HTMX server rendering.** It fits Go library/host wiring and avoids Node in production, but gives up the React component ecosystem and client-state tooling. HTMX also makes server-rendered fragments part of the browser interaction contract, which becomes harder to evolve as interactions grow.
3. **React SPA with a Go static/API host.** It keeps production HTTP wiring in Go, but still needs a Node build and introduces a separate Go JSON BFF contract. It combines two stacks while losing Next's integrated server runtime and the proven overlay model.

Direct grpc-web was not selected because it moves browser CORS, cookies, and authentication into the gateway. A Fusion-only implementation was not selected because OSS and Uber would no longer share the product UI.

## Testing

- `platform/web`: auth resolution, user-info, route gate, credential metadata, Pino setup, and OTEL registration.
- Route handlers: mocked gRPC client; success, validation, not found, pagination, service failure, missing identity, and unauthorized identity. A denied or missing-identity RPC is returned as an error, not an empty list or a redacted page.
- React views: mocked fetch/query client; empty, error (including authorization denial), pagination, and in-flight/terminal states with fake timers.
- Build checks: lint and TypeScript typecheck for each npm package.
- Example host: compose health plus one list or summary request against the example gateway.
- Production overlay: identity, user-identity forwarding on data RPCs, denied and missing identity failing at the domain service, metrics scrape access, exporters, and deployment wiring.

## Rejected

- **Go templates as the product UI.** A Go host can render HTML and use HTMX for fragment updates, keeping the executable and deployment wiring in Go. That is a reasonable choice for a small, mostly static tool. It makes HTML fragments the interaction contract, however, and leaves tables, filters, polling, loading/error states, accessibility primitives, and future client state to application-specific code. It also does not use the React component and testing ecosystem or the Cadence Web deployment model. The lower runtime cost does not outweigh those constraints for the operator UI.
- **React SPA plus a Go static/API host.** A Go service could serve a compiled React bundle and expose a JSON API that calls gateway RPCs. This avoids a Node runtime in production, but still requires Node to build the bundle. It creates two separately versioned surfaces—the browser bundle and the Go JSON API—and another contract to authenticate, validate, document, and test. Next's route handlers keep the HTTP-to-gRPC boundary beside the React application, while the overlay gives the production host an established place for platform-specific wiring.
- **Fusion-only implementation or a rewrite in Uber's web monorepo.** This would use the internal website stack and its plugins, but it would be a separate implementation from the OSS UI. Product behavior, views, and tests would drift between the two repositories. The overlay instead deploys the OSS application and limits Uber changes to identity, authorization, exporters, and deployment wiring.
- **Direct grpc-web from the browser.** This removes the BFF but requires the gateway to own browser-facing CORS, cookies, session/auth semantics, and exposure policy. The gateway is a domain RPC service, not a browser session host. A BFF contains those concerns and keeps the published gRPC contract unchanged.

## Relationship to Cadence Web v4

[cadence-workflow/cadence-web](https://github.com/cadence-workflow/cadence-web) is the closest deployed analog: Next.js App Router, React, Base Web, `@grpc/grpc-js`, Pino, OpenTelemetry, example Docker, and a production overlay. This RFC adopts that runtime and deployment split while keeping one application per SubmitQueue domain.
