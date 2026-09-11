# Web/UI Design

## Problem

Operators have no browser UI. The CLI polls gateway history. Gateway already has summary, history, and receipt-time `List` ([status-list-api.md](submitqueue/status-list-api.md), [history-api.md](submitqueue/history-api.md)); Stovepipe has a parallel history API ([stovepipe/request-history-api.md](stovepipe/request-history-api.md)). Those RPCs are gRPC. A browser cannot call them the way the CLI does.

The first UI is a **read** operator console: pick a queue, list requests, open summary and history, search by change URI, poll while a request is in flight. That is the same surface as `submitqueue/client` watch, not a workflow editor or a Cadence-scale SPA.

Two ways to serve that HTML:

1. **Go templates** — an importable Go library plus a Go HTTP host (OSS example under `service/`, production in go-code). Same library/host split as gateway.
2. **Next.js BFF** — a Node process in OSS that the browser talks HTTP/JSON to; that process calls gRPC. Production is a source overlay (Cadence Web v4).

SubmitQueue picks (1). The rest of this RFC is that design. Next.js stays the documented alternative if the UX outgrows pages, forms, fragments, and polling.

## Choice

Lightweight operator reads fit server-rendered pages. The product and Uber wiring are already Go. A Node BFF would add npm, a second observability stack, and an overlay repo that cannot import `{domain}/web` the way go-code imports the gateway.

Cadence Web v4 is still the closest **rich** analog (Next.js, React, Base Web). This UX is pages, forms, fragments, and polling: Go + [`templ`](https://templ.guide/) HTML, HTMX for updates, server as source of truth.

## Picture

```
Browser
   |  HTTP: pages, forms, HTMX fragments
   v
Go web host
   |  imports submitqueue/web
   |  owns HTTP, auth, zap, tally, tracing, gateway client
   v
SubmitQueue gateway RPC
   |  authenticates and authorizes every operation
   v
Gateway controllers and stores


This repository                         Uber go-code
---------------                         ------------
submitqueue/web       UI library <----  imported by web host
service/submitqueue/web example host    HTTP, YARPC client, zap, tally, deploy
```

The web host is a **separate process** from the gateway. UI reads go through the gateway’s public RPC and inbound auth. Mounting templates on the gateway and calling controllers in-process would couple deploys and skip that boundary.

Wire is still protobuf RPC. The OSS example adapter uses `google.golang.org/grpc`. Uber `submitqueue-gateway` is YARPC (`yarpcfx`, grpc inbound on `UBER_PORT_GRPC`); the go-code web host uses the generated **YARPC client**, not `@grpc/grpc-js`.

Go **library** vs **host**: [modular-queue-wiring.md](submitqueue/modular-queue-wiring.md). The UI library is a client of the hosted gateway API.

## What we lose by choosing templ over Next

| | **templ + HTMX (chosen)** | **Next.js BFF (not chosen)** |
|---|---|---|
| Executable | Go host imports a library | Node `next start` is the app |
| Uber production | go-code service, module pin | Overlay repo (Cadence Web v4) |
| Auth to gateway | Native YARPC client + authfx | Node must attach tokens on gRPC metadata |
| Logs / metrics | zap + tally → M3 / uMonitor | Pino + OTEL scrape → M3 |
| UI kit | Semantic HTML + CSS | Base Web, Styletron, React 18 |
| Interactivity | Full-page + HTMX fragments; poll | TanStack Query, client cache, richer local state |
| JS required | No for core nav; HTMX for live poll/filter | Yes |
| Cadence-shaped patches | No | Overlay file-replace of views/handlers |
| Frontend hiring / ecosystem | Go + templ | TypeScript / React |

Concrete losses:

- **No Cadence-like operator chrome.** Tables, tabs, and theme overlays that Base Web gives for free are hand-built CSS. Restyling at Uber is host CSS, not a Fusion or Base Web theme.
- **No client application.** Complex local state, optimistic UI, drag-and-drop, and dense interactive graphs are a poor fit. HTMX swaps HTML; it is not a store. Growing that far means a follow-up that adopts Next (or a bounded widget), not React-inside-templ.
- **No React islands on templ.** That reintroduces a JS build without the Next BFF. If the UX needs a client app, use Next.
- **No shared Node overlay story.** Deployers who already run Cadence Web cannot drop this UI into that overlay. They import a Go module or run the example binary.
- **Fragment UX instead of JSON SPA.** Pagination and status poll are HTML fragments or full reloads. There is no public JSON BFF for a separate frontend.
- **templ codegen.** `.templ` files generate committed `*_templ.go`. That is extra toolchain next to `make proto`, not a second language runtime.

What we keep: queue-scoped list/summary/history, poll, OSS/Uber sharing of product HTML, gateway as ACL, ordinary Go CI (`go test`, gazelle).

## App

One UI package per domain that needs a read model: `submitqueue/web` now; `stovepipe/web` later. Runway has none. No repo-root `web/` app. No `platform/web` until a second domain UI shows a shared contract.

```
submitqueue/web/
├── handler.go             routes and HTTP behavior
├── gateway.go             UI-facing gateway interface
├── viewmodel/             presentation values
├── template/              .templ sources and generated Go
└── static/                embedded CSS, HTMX, small local JS

service/submitqueue/web/
├── server/                example main, gRPC adapter, lifecycle
└── Dockerfile
```

`submitqueue/web` is a Go library: `http.Handler`, templates, assets. It does not listen, read env, authenticate, or construct an RPC client.

The host supplies a gateway adapter:

- OSS example: generated Go gRPC client.
- go-code: generated YARPC client.
- Tests: in-memory mock.

The interface is read-only: ping, `ListQueues`, summary, `List`, history. `ListQueues` is a prerequisite over `queueconfig.Store.List` ([QueueConfig](../../submitqueue/entity/queue_config.go)). Other reads are queue-scoped, same as the CLI. Cross-queue request listing is not in the gateway contract. Live updates are poll (`submitqueue/client` watch). Stovepipe IDs stay `request/<queue>/<counter>`, not `sqid`.

Writes (`Land`, `Cancel`, `Ingest`) need a later RFC (auth + CSRF). They still go through the gateway.

`service/submitqueue/web` is the example host, analogous to `service/submitqueue/gateway/server`. Compose starts it next to the example gateway.

## User experience

Normal URLs, links, GET forms: queue picker, queue-scoped list (receipt-time range, pagination), summary and history by sqid, exact change-URI search, poll while in flight.

The server renders full pages for navigation and HTML fragments for updates. HTMX hits the same handlers for pagination, filtering, and status poll. A terminal status omits the next poll trigger. Without JS, nav still works; poll needs refresh.

No client store, no hydration, no parallel JSON API.

## Technology

| Layer | Choice |
|---|---|
| Language / runtime | Go, same version as this module |
| HTTP | `net/http` |
| Components | [`github.com/a-h/templ`](https://templ.guide/) |
| Progressive updates | HTMX, pinned and `go:embed`’d |
| Styling | Semantic HTML and application CSS |
| Assets | `go:embed`; no CDN |
| Gateway client | Host adapter (gRPC example, YARPC Uber) |
| Logs / metrics | Host zap and tally |
| Tracing | Host HTTP and RPC middleware |
| Tests | Go, `httptest`, testify |

`templ` over raw `html/template` for typed components. Generation is part of repo tooling; Gazelle runs after. Inline script is avoided so hosts can set a tight CSP.

## Auth

The **gateway** is the ACL for every RPC. The web host authenticates the browser, then attaches credentials the gateway accepts. Missing or invalid RPC auth fails at the gateway.

OSS example may run with auth disabled against the example gateway. Local only.

At Uber:

1. The website edge injects employee identity (`x-auth-params-email`). The host rejects protected UI routes when it is absent.
2. The gateway adapter sends a supported on-behalf-of credential on the YARPC call.
3. `submitqueue-gateway` authfx (uSSO / breeze) verifies that RPC and applies queue / operation policy.
4. Hiding queues in HTML is presentation only.

Health and metrics do not use the browser auth path just because they share a process.

## Logging, metrics, tracing

Same Go stack as gateway: zap (route, RPC outcome, queue, actor), tally (request, render, RPC, latency), HTTP and YARPC tracing. In go-code, tally → M3 → uMonitor; zap stdout → log pipeline.

No Pino, Node OTEL, or Prometheus text route on the UI process. The OSS library has no Uber exporters; hosts inject zap/tally.

## Uber host

A Go service in go-code, not an overlay and not Uber’s web monorepo (Fusion). It imports `github.com/uber/submitqueue/submitqueue/web` and supplies HTTP lifecycle, edge identity gate, YARPC gateway client, zap, tally, tracing, config, deploy metadata.

This is ordinary Go service wiring: the same dependency-injection, transport, and observability modules the gateway host already uses, plus a `net/http` handler for the browser. Other Go-rendered internal tools follow the same pattern.

## Testing

- Handlers: mock gateway, `httptest` (success, bad query, not found, RPC error).
- Components: render view models; assert semantics, not golden HTML files.
- Fragments: HTMX vs full page; terminal status stops poll.
- Assets: content type and cache headers.
- Example compose: health, list, summary (`svc-submitqueue-web`).
- go-code: identity, outbound credentials, auth rejection, M3, wiring.

## Rejected

- **Next.js as the first SQ UI.** Extra Node process and overlay for list, summary, history, and poll. Tradeoffs: [What we lose](#what-we-lose-by-choosing-templ-over-next).
- **Fusion, or a rewrite in Uber’s web monorepo.** Operators and OSS would not share an app.
- **UI mounted in the gateway process.** Couples deploys and skips gateway RPC auth.

## Relationship to Cadence Web v4

[cadence-workflow/cadence-web](https://github.com/cadence-workflow/cadence-web) is the rich Node BFF (Next, React, Base Web, overlay). This RFC copies Cadence’s **split of product vs deployer policy**, not its **runtime**.
