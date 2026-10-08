---
title: Home
template: home.html
hide:
  - navigation
  - toc
---

<div class="sq-github-only" markdown>

# SubmitQueue

A high-performance speculative submission queue that keeps your trunk consistently green at scale.

</div>

SubmitQueue does not validate changes one at a time. It speculatively rebases and validates many changes in parallel against predicted future states of HEAD. Changes whose validations pass land automatically. When a validation fails, SubmitQueue isolates the offending change and retries the rest, with no human involved. It is designed for large monorepos and fast-moving teams, where concurrent changes can introduce subtle conflicts and destabilize builds.

## Why SubmitQueue

<div class="grid cards sq-features" markdown>

-   :material-source-branch-check:{ .lg .middle } **Speculative validation**

    ---

    SubmitQueue builds a tree of possible future HEADs and validates the paths most likely to land, in parallel.

    [:octicons-arrow-right-24: Speculation](rfc/submitqueue/speculation.md)

-   :material-shield-check-outline:{ .lg .middle } **Isolates failures**

    ---

    A failing change is isolated and rejected while the rest of its batch carries on to land.

    [:octicons-arrow-right-24: Orchestrator workflow](rfc/submitqueue/workflow.md)

-   :material-puzzle-outline:{ .lg .middle } **Pluggable extensions**

    ---

    Build runners, change providers, storage, queues and scorers are vendor-agnostic interfaces with swappable implementations.

    [:octicons-arrow-right-24: Extension contract](rfc/submitqueue/extension-contract.md)

-   :material-tray-full:{ .lg .middle } **Durable, queue-driven pipeline**

    ---

    Every stage is an idempotent consumer on an at-least-once message queue, with optimistic locking and no distributed transactions.

    [:octicons-arrow-right-24: SQL-based queue](rfc/sql-queue-rfc.md)

</div>

## Components

<div class="grid cards" markdown>

-   :material-call-merge:{ .lg .middle } **SubmitQueue**

    ---

    The gateway and orchestrator that accept, batch, speculate on, build and land changes.

    [:octicons-arrow-right-24: Workflow](rfc/submitqueue/workflow.md)

-   :material-airplane-landing:{ .lg .middle } **Runway**

    ---

    The landing service. It owns VCS operations, conflict checks and merges, on SubmitQueue's behalf.

    [:octicons-arrow-right-24: Workflow](rfc/runway/workflow.md)

-   :material-check-decagram-outline:{ .lg .middle } **Stovepipe**

    ---

    The post-land pipeline. It validates landed commits and tracks the last green revision.

    [:octicons-arrow-right-24: Workflow](rfc/stovepipe/workflow.md)

</div>

## Try it in a minute

You need only Docker. No repository, account or token is required.

```bash
make local-submitqueue-start   # Gateway + Orchestrator + Runway + MySQL
make demo-requests             # create changes, enqueue them, watch them land
make local-submitqueue-stop
```

The [Quickstart](howto/QUICKSTART.md) goes from a fake provider to a local git repository to real GitHub pull requests. Questions? Join the [Slack community](https://join.slack.com/t/submitqueue/shared_invite/zt-46gkqj682-7zcQphxm2pYqkjDo9lbmYA).
