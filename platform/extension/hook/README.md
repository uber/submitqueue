# Hook

Vendor-agnostic interface for fire-and-forget side effects run in response to pipeline lifecycle events: warehouse exports, code-host comments, notifications, audit trails. See [the hooks framework RFC](../../../doc/rfc/hook-framework.md) for the design and [`api/base/hook`](../../../api/base/hook) for the event contract.

## Interface

### Hook

Handles one lifecycle event. `Name` identifies it in logs, metrics, and failure attribution.

Four obligations, all of them consequences of running behind an at-least-once queue:

- **Idempotent on the event id.** The same event may arrive more than once, including after a successful `Handle`. The id is derived from the transition, so a redelivery carries the id the first delivery did.
- **Return nil to ignore an event.** There is no filter or subscription API. A hook that does not care about a type returns nil and costs nothing; routing can become a wiring decorator if it ever pays for itself.
- **Return plain errors.** Classification is the consumer's job. An error must mean the side effect did not happen — reporting failure for work that succeeded turns at-least-once delivery into repeated duplicate effects.
- **Never write pipeline state.** A hook's outcome is invisible to the pipeline, which is exactly what makes it unable to affect the transition that triggered it.

## Wiring

A hook is wired **once per host**, not resolved per queue, so this package has no `Config` and no `Factory`. What an integration does is a property of the deployment rather than of the queue an event came from; a hook that genuinely needs per-queue behavior resolves the queue from the event payload.

The host constructs its hook and hands it to the dispatcher in [`platform/hook`](../../hook), which owns the consumer side: decode, validate, invoke.

## Implementations

- **`noop/`** — accepts every event and does nothing. The default before a host has any integration, so the seam behaves identically whether or not hooks are configured.
- **`composite/`** — fans an event out to several children, runs all of them even after one fails, and joins the failures with the name of each failing child. Read its package doc before wiring more than one child: they share a single retry budget, so one chronically failing integration eventually dead-letters events the others handled fine.

A sink that serves several domains is one implementation wired into each domain's host, not one implementation per domain.

## Implementing a Hook

1. Create `platform/extension/hook/{name}/` for a hook reusable across domains, or `{domain}/extension/hook/{name}/` for one that is domain-specific.
2. Implement `Handle` and `Name`, keying any deduplication on `event.GetId()`.
3. Decide per event `type` what to do, and return nil for the types you ignore.
4. Wire it into the host's dispatcher — inside a `composite` if the host has more than one.
