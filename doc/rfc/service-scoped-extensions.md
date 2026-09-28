# Service-Scoped Extensions

Deciding which service of a multi-service domain an extension belongs to, and moving the parts that are that service's alone out of `{domain}/extension/` and into `{domain}/{service}/extension/`. SubmitQueue's storage extension is the one this layout was applied to; the same rule is meant to apply to every extension after it.

## Status

Implemented for SubmitQueue storage and for the core packages that served one service. `submitqueue/gateway/extension/storage` and `submitqueue/orchestrator/extension/storage` each declare a `Factory`, a `Storage` aggregate, and a MySQL schema. The thirteen store contracts, their mocks, and the error vocabulary remain at `submitqueue/extension/storage`. There is no `submitqueue/extension/storage/mysql` package. `submitqueue/extension/storage/BUILD.bazel` filegroups the two service schemas for deployments that colocate them.

`submitqueue/core/batch` and `submitqueue/core/request` are not on disk. Batch helpers live in `submitqueue/orchestrator/core/batch`. Request-log publish and terminate live in `submitqueue/orchestrator/core/request`. The gateway materializer lives in `submitqueue/gateway/core/request`. `submitqueue/core` holds `changeset`, `messagequeue`, and `topickey`. `changeset` still declares `Stores` and `Resolve`, because `buildrunner`, `conflict`, and `speculation` are still domain-level packages that depend on it. Stovepipe keeps `stovepipe/extension/storage/mysql` because that domain is the service; that package is not a leftover SubmitQueue backend.

## Problem

`submitqueue/extension/storage/` held thirteen stores behind one `Storage` aggregate and one `Factory`, and both SubmitQueue services depended on the whole thing. They do not use the whole thing. The gateway reaches for four stores and the orchestrator for the other nine, with no overlap in either direction.

The split is the documented design. [Gateway Status and List APIs](submitqueue/status-list-api.md) states it outright:

> The gateway owns the append-only request log and three new logical read models. The orchestrator's request and change stores are pipeline working state with different retention semantics, so neither API reads them.

The aggregates enforce it. A gateway `Storage` has no `GetBatchStore`, so a gateway controller cannot obtain an orchestrator store.

Separating the services onto their own databases already worked — the orchestrator publishes lifecycle events on the log topic and the gateway persists them, so the four gateway tables are not a store two services share. Each service's schema directory now contains the tables that service reads. The union filegroup at `submitqueue/extension/storage` is what a colocated deployment takes.

## Decisions

1. An extension whose **aggregate** is used by exactly one service of a multi-service domain puts that aggregate, its implementations, its mocks and its schema at `{domain}/{service}/extension/{ext}/`. The store contracts themselves stay at `{domain}/extension/{ext}/`: the aggregate is what decides reachability, so moving the contracts adds no enforcement while forcing domain-level callers to import a service package.
2. An extension shared between a domain's services stays wholly at `{domain}/extension/{ext}/` — including one used by a single service today but expected in both. `submitqueue/extension/queueconfig/` is gateway-only now and stays where it is on that basis. A single-service domain is unaffected: its domain root is its service root, so nothing moves.
3. Storage is the extension this split applied to. `changeprovider`, `validator`, `conflict`, `buildrunner` and `speculation` are orchestrator-only and would qualify under Decision 1, but none has a schema and none was the reason for this change; they stay at `{domain}/extension/` until a later RFC takes them. The trigger for moving storage was a schema that a separate-database deployment must not over-provision.
4. SubmitQueue's storage aggregate splits in two: `submitqueue/gateway/extension/storage/` declares a `Factory` and a four-accessor `Storage`, `submitqueue/orchestrator/extension/storage/` a `Factory` and a nine-accessor `Storage`. Neither can resolve the other's stores.
5. `submitqueue/extension/storage/` keeps the thirteen store contracts, their mocks, the error vocabulary, `Config`, and the package README, which documents the contracts. Its import path does not change, so callers that name only a contract or an error are untouched.
6. The contract package is not promoted to `platform/`. It is not cross-domain — `stovepipe/extension/storage/storage.go` already declares its own verbatim copy of the same symbols, so a per-domain error vocabulary is the existing pattern rather than something this RFC introduces.
7. Each half owns its own MySQL schema directory, so the tables a service creates are the tables it reads. A union target over both stays available for deployments that colocate the services — the end-to-end tests and the local compose stack take it and name no service directory. Split schemas are the opt-in, not the default.

## The split

Every store, verified against actual usage rather than intent.

| Gateway | Table |
|---|---|
| `RequestLogStore` | `request_log` |
| `RequestSummaryStore` | `request_summary` |
| `RequestQueueSummaryStore` | `request_summary_by_queue` |
| `RequestURIStore` | `change_uri_request_mapping` |

| Orchestrator | Table |
|---|---|
| `RequestStore` | `request` |
| `RequestBatchStore` | `request_batch` |
| `ChangeStore` | `change` |
| `BatchStore` | `batch` |
| `BatchDependentStore` | `batch_dependent` |
| `QueueBatchStateStore` | `queue_batch_state` |
| `BuildStore` | `build` |
| `SpeculationPathSetStore` | `speculation_path_set` |
| `PathBuildStore` | `path_build` |

`counter` is not in either list. It is its own shared extension and each service keeps its own counter table.

## Layout

```
submitqueue/
├── extension/storage/                  # contracts, shared
│   ├── {13 stores}_store.go            # BatchStore, RequestLogStore, …
│   ├── storage.go                      # ErrNotFound, ErrAlreadyExists, ErrVersionMismatch,
│   │                                   # IsNotFound, WrapNotFound, Config
│   ├── README.md
│   └── mock/                           # the 13 store mocks
├── gateway/extension/storage/
│   ├── storage.go                      # Factory + Storage, 4 accessors
│   ├── mock/                           # the aggregate's mock
│   └── mysql/
│       └── schema/                     # 4 tables
└── orchestrator/extension/storage/
    ├── storage.go                      # Factory + Storage, 9 accessors
    ├── mock/                           # the aggregate's mock
    └── mysql/
        └── schema/                     # 9 tables
```

Naming a contract stays legal from anywhere; obtaining one does not. A gateway file may write `basestorage.BatchStore`, but its `Storage` has no `GetBatchStore`, so there is no way to hold one. That method set is the boundary.

## What this means for `submitqueue/core/`

`core/` is infra shared between the domain's services, so it must not depend on one. Splitting the aggregate made that a live question: a file there that resolves stores has to name some service's `Storage` or `Factory`.

Keeping the contracts at domain level settles it for files that name only a store type. It does not settle it for files that take an aggregate, because aggregates are service-scoped by construction.

Those files were resolved two ways.

**Relocate the package when it serves one service.** This is done. `submitqueue/orchestrator/core/batch` holds the batch helpers. `submitqueue/orchestrator/core/request` holds log publish and terminate. `submitqueue/gateway/core/request` holds the materializer. They keep taking an aggregate; a service naming its own extension is not an inversion.

**Declare the shape when it cannot.** `submitqueue/core/changeset` stays, because domain-level extension packages depend on it — `buildrunner` and its implementations, `conflict/pathoverlap`, and the `speculation/scorer` implementations — and those remain at domain level under Decision 3. Moving it now would trade one inversion for another. It still declares the slice of an aggregate it needs, and the wiring layer supplies the binding:

```go
type Stores interface {
	GetRequestStore() storage.RequestStore
	GetChangeStore() storage.ChangeStore
}

type Resolve func(queue string) (Stores, error)
```

The orchestrator aggregate exposes both accessors, so it satisfies `Stores` structurally and `changeset` names no service package. The gateway aggregate does not: it holds the request log and the public projections, and it has neither `GetRequestStore` nor `GetChangeStore`. Once those extensions relocate under the orchestrator, `changeset`'s consumers are all orchestrator-side and it should relocate with them — at which point it can drop `Stores`/`Resolve` and take `orchstorage.Factory` directly, matching the other relocated packages rather than staying the one exception.

`submitqueue/core` now holds `changeset`, `messagequeue` and `topickey`, and nothing in it imports a service package.

A third option exists and is not used here: narrowing a signature to take the individual contracts rather than the aggregate, for example `FindByRequestID(ctx, batches storage.BatchStore, links storage.RequestBatchStore, …)`. It suits a function whose store set is fixed, and is worth reaching for before relocating a package that genuinely is shared.

## Alternatives

**Subdivide the schema filegroup only.** What [#737](https://github.com/uber/submitqueue/pull/737) did. One small diff, and it gives a deployment the per-service table sets it needs. It leaves the coupling in place: both services still depend on all thirteen stores, nothing stops a gateway controller resolving `BatchStore`, and it puts a service-shaped axis inside an extension that is supposed to know nothing about service topology. Rejected.

**Move the store contracts down with their implementations.** Cohesive — a table's contract, implementation, schema and mock in one place — but it buys no enforcement, because the aggregate already decides reachability, and it costs `core/` a service import for every file naming a store type. Rejected.

**Keep one `Storage` aggregate and have each service wire the subset it uses.** Smaller than this RFC, since nothing moves. But the orchestrator's stores stay reachable from the gateway, so the boundary is a convention rather than a fact, and the schema stays undivided. Rejected.

**Promote the contract package to `platform/`.** Rejected under Decision 6.
