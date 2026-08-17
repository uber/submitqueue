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

### Hooks

Resolves the hooks that run for an event. The controller in [`platform/hook`](../../hook) asks it once per delivery and runs everything it returns; returning none is ordinary and means nothing this host wired cares about the event.

`For` takes the event rather than a queue name because the envelope carries no queue. Which scope selects hooks differs per domain — queue, source, event type — and only the host that publishes the payload can read a queue out of it, so the choice belongs to the resolver. Resolution runs on every delivery and cannot fail: an integration that cannot be reached is a `Handle` error, not an absent hook.

## Wiring

There is no `Config` and no `Factory` here. Selection is the resolver's job, and the resolver is built in the wiring layer — the only place that knows the full set of queues and the integrations wired for each. The host constructs its `Hooks` and hands it to the controller in [`platform/hook`](../../hook), which owns the consumer side: decode, validate, resolve, invoke.

Two queues in one host can point at different providers and want different integrations, which is why hooks are resolved per event rather than fixed per deployment.

## Implementations

- **`noop/`** — accepts every event and does nothing. A placeholder for a host that wants the stage registered before it has any integration; a resolver that returns no hooks does the same thing.

A sink that serves several domains is one implementation wired into each domain's host, not one implementation per domain.

## Implementing a Hook

1. Create `platform/extension/hook/{name}/` for a hook reusable across domains, or `{domain}/extension/hook/{name}/` for one that is domain-specific.
2. Implement `Handle` and `Name`, keying any deduplication on `event.GetId()`.
3. Decide per event `type` what to do, and return nil for the types you ignore.
4. Return it from the host's `Hooks` resolver for the events it should run on.

Every hook the resolver returns for an event shares one consumer and therefore one retry budget: one chronically failing integration eventually dead-letters events the others handled fine. See [`platform/hook`](../../hook) before wiring several.
