# Namespacing the Shared Layer

Proposal to group the repo's shared, cross-domain building blocks under a single `base/` namespace. Decision and rationale only; the rename lands after this RFC is reviewed.

## Problem

Shared cross-domain code and domain-specific code use the **same bare names at the same nesting level**. Three top-level packages — `core/`, `entity/`, `extension/` — each have a same-named sibling inside every domain (`submitqueue/core/`, `submitqueue/entity/`, `submitqueue/extension/`, and the Stovepipe equivalents).

The domain side is already namespaced by its domain folder, so it reads unambiguously: `submitqueue/core` is "SubmitQueue's core." The shared side is the unqualified one, and that is where the ambiguity lives. When someone says "core" or "entity" in a review, a commit message, or a search, it is unclear whether they mean the cross-domain package or a domain's own. The import path disambiguates at compile time, but the human-facing name does not.

This is not a problem with the word "core" or "entity" specifically — it is structural. Renaming one package in isolation (say `core/` -> `base/`) does not generalize: there is no clean single-word rename for a shared `entity/` or `extension/`, and doing it per-package would leave three inconsistent, ad-hoc names while two of the three collisions remain.

## Proposal

Give the shared layer its own namespace: move all three shared packages under one umbrella directory, `base/`.

```
BEFORE  (shared dirs collide with domain dirs)    AFTER  (one base/ umbrella, domains unchanged)
----------------------------------------------    ----------------------------------------------
submitqueue/                                      submitqueue/
|-- core/         <shared>                        |-- base/         <shared umbrella, new>
|   |-- errs/                                     |   |-- core/
|   |-- httpclient/                               |   |   |-- errs/
|   `-- metrics/                                  |   |   |-- httpclient/
|-- entity/       <shared>                        |   |   `-- metrics/
|   `-- messagequeue/                             |   |-- entity/
|-- extension/    <shared>                        |   |   `-- messagequeue/
|   |-- counter/                                  |   `-- extension/
|   `-- messagequeue/                             |       |-- counter/
|-- submitqueue/                                  |       `-- messagequeue/
|   |-- core/                                     |-- submitqueue/   (unchanged)
|   |-- entity/                                   |   |-- core/
|   |-- extension/                                |   |-- entity/
|   |-- gateway/                                  |   |-- extension/
|   `-- orchestrator/                             |   |-- gateway/
`-- stovepipe/                                    |   `-- orchestrator/
    |-- core/                                     `-- stovepipe/     (unchanged)
    |-- entity/                                       |-- core/
    |-- extension/                                    |-- entity/
    |-- gateway/                                      |-- extension/
    `-- orchestrator/                                 |-- gateway/
                                                      `-- orchestrator/
```

The only structural difference is on the shared side: the three root packages move under `base/` (gaining one nesting level), while both domains are byte-for-byte unchanged. After this, every bare `core` / `entity` / `extension` belongs to a domain, and anything shared across domains lives under `base/`. The umbrella applies uniformly to all shared building blocks — present and future — rather than relying on a clever rename per package.

Import paths gain one segment on the shared side only, e.g. `github.com/uber/submitqueue/core/errs` becomes `github.com/uber/submitqueue/base/core/errs`. Domain import paths are untouched.

## Why `base`

`base` reads as the foundational layer the rest of the repo builds on, it is short, and it works equally well in front of `core`, `entity`, and `extension`. The word alternatives considered:

- **`shared`** — the most literal ("shared across domains"), very hard to misread. Slightly more verbose in every import path.
- **`common`** — conventional for cross-cutting code, but vaguer; "common" says less about the layer's role than "base."
- **`internal`** — rejected. Go's `internal/` visibility rule would forbid `example/` and `test/` from importing these packages, and both currently do.
- **Per-package rename, no umbrella** — rejected. Only `core` has a natural standalone rename; it leaves `entity` and `extension` colliding and the scheme inconsistent.

`base` and `shared` are both defensible; this RFC recommends `base` and treats the final word as the main open question for review.

## Alternatives considered

### One shared tree per layer, with domains as subdirectories

Instead of a `base/` umbrella, keep a single top-level `core/`, `entity/`, and `extension/` that each hold *both* the shared packages (at the root) and every domain's packages (as subdirectories) — e.g. `entity/messagequeue` (shared) sitting alongside `entity/submitqueue/...` and `entity/stovepipe/...`. Services (`gateway`, `orchestrator`) stay under each domain.

```
core/                 entity/               extension/
|-- errs/             |-- messagequeue/     |-- counter/
|-- httpclient/       |-- submitqueue/      |-- messagequeue/
|-- metrics/          `-- stovepipe/        |-- submitqueue/
|-- submitqueue/                            `-- stovepipe/
`-- stovepipe/
```

This *also* removes the bare-name collision — there is only ever one `entity/` tree, scoped by subdirectory — and its appeal is symmetry and a single home for "all entities."

Rejected because it breaks **domain cohesion unevenly**. A domain's library code (entity/extension/core) would live under the layer trees while its services live under the domain tree, so SubmitQueue ends up split across `core/submitqueue`, `entity/submitqueue`, `extension/submitqueue`, *and* `submitqueue/` services — four-plus top-level locations, and "where is SubmitQueue's X?" depends on whether X is a library layer or a service. It also makes the first domain implicit (the bare `gateway`/`orchestrator`) while later domains are explicit, which is asymmetric and worsens as domains are added. Crucially, it offers **no churn advantage**: promoting a package from domain-specific to shared is still an import-path change (`entity/submitqueue/foo` -> `entity/foo`), the same cost as under `base/`. `base/` removes the identical collision while keeping every domain in exactly one place.

### Full layer-first (group by type, domain underneath)

A stronger version of the above that also relocates services — top-level `entity/`, `extension/`, `controller/`, `service/`, each split by domain. Rejected for the same cohesion reason, amplified: it is the "package by layer" arrangement Go style guidance specifically warns against, it scatters every domain across the whole repo, and it conflates two distinct axes — *service role* (`gateway`, `orchestrator`) and *domain* (`submitqueue`, `stovepipe`) — at one level, even though each domain already has its own gateway and orchestrator. The repo is a 3-axis matrix (domain x role x layer); domain-first nests those axes cleanly and addresses every cell, e.g. `stovepipe/gateway/controller`.

## Placement rule — keeping things in the right tier

Most churn at the shared<->domain seam comes from putting a generic helper inside a domain because that domain happened to need it first, then moving it to shared later. The tree shape does not prevent that; placing by *nature* at creation does. The test:

- If a package speaks a **domain vocabulary** — a SubmitQueue `Request`, a batch, "land", "speculate" — it is domain-bound. Put it in the domain; it is unlikely to ever become shared.
- If it is **pure plumbing with no domain concept** — `errs`, `metrics`, `httpclient`, a generic message queue — it is shared from birth. Put it in `base/`; it will not move down either.

Everything shared today (`errs`, `metrics`, `httpclient`, `messagequeue`) is plumbing with zero domain vocabulary, which is exactly why it has stayed put. Applying the test up front makes genuine promotions rare, which is the real lever on churn — not the choice of tree.

## Scope

This is a mechanical move: package directories relocate under `base/`, import paths and `BUILD.bazel` files update accordingly, and the package docs and READMEs that describe the "top-level vs domain" split are reworded to "base vs domain." No package contents, types, or behavior change. The work is a single rename PR (regenerated BUILD files via gazelle), reviewable as a no-op refactor.
