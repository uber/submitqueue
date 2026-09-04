# Hook event contract

The published, language-neutral contract for hook events: fire-and-forget lifecycle notifications that let integrations react to a pipeline transition without being able to stall or fail the pipeline. See [the hooks framework RFC](../../../doc/rfc/hook-framework.md) for the design and [the message queue contract RFC](../../../doc/rfc/messagequeue-contract.md) for the conventions it follows.

It lives under `api/base/` rather than `api/{domain}/` because no domain owns it. Every domain publishes this same shape to its own hook topic, so a sink consuming several domains reads one schema rather than one per producer.

Payloads are defined as proto3 messages in [`proto/hook.proto`](proto/hook.proto) and generated into [`protopb/`](protopb); the proto is the authority and a non-Go client compiles against it directly. On the wire, payloads are serialized as protobuf JSON (`protojson`), so the queue keeps storing self-describing JSON. The Go helpers here are generic `protojson` glue — `Marshal(m)` and `Unmarshal[T](b, m)` — plus the two rules that must be identical across producers: how an id is minted and what makes an event well-formed. Field names stay snake_case (`UseProtoNames`) and `int64` fields serialize as JSON strings.

## The envelope

`HookEvent` carries `id`, `source`, `type`, `timestamp_ms`, `version`, and `payload`. The envelope holds only what every consumer keys on uniformly; everything specific to what happened lives in the payload.

`source` and `type` are open strings rather than enums, and `payload` is a `google.protobuf.Struct` rather than a `oneof`. That is the central trade: a producer adds a new event type by publishing it, instead of by changing the wire contract and redeploying every consumer. protojson rejects unknown *enum* values, so an enum here would break existing consumers on every addition.

Subject, queue, and error are deliberately **not** on the envelope. They are facts about a particular occurrence, so they belong in the payload — no major event platform carries a top-level error either.

## Identity and idempotency

`id` is derived from the transition, not random: `source`, `type`, the subject's id, and the subject's post-transition `version`, joined. `NewEventID` mints it. Replaying the delivery that caused the transition therefore mints the *same* id, which is what lets the queue dedupe the redelivery and lets a hook stay idempotent by keying on it. That derivation is why the framework needs no transactional outbox: the publish rides inside the delivery that performed the state write, and a crash before the ack replays both halves safely.

When a transition is not a versioned write there is no version to distinguish occurrences, so the id of the message that caused it stands in, plus an ordinal when one cause publishes several same-typed events. `NewUnversionedEventID` mints that form.

Consumers never parse an id. It is a dedupe and idempotency key, not a structured field.

## Staleness

`version` is the subject's optimistic-locking version immediately after the transition, and `0` when the transition was not a versioned write. Delivery is at-least-once, so a hook can receive an event describing a transition that has since been superseded; comparing this version against the subject's current version in the store is how it tells the two apart. Timestamps cannot answer that, because the clocks belong to different machines.

A domain with no versioned entities (Runway holds no durable state of its own) publishes `0` throughout. That is the normal mode for such a producer, not a degenerate case.

## Payload

Shaped per `type` by the domain that publishes it, add-only, and documented by that domain. It must carry the subject's id, and it must carry any fact recorded nowhere else — land step outcomes, build failure detail — because for those the event is the only durable record.

It must **not** be an entity snapshot. A snapshot is stale the moment it is redelivered, it competes with the store as a source of truth, and it drags a domain's schema into a contract shared by every domain. Hooks resolve entities from their stores.

## Topic keys

The binding between a topic key and its payload lives in the message's `topic_keys` option (defined in `api/base/messagequeue`); `TopicKeys` reads it back by reflection. A topic key is a stable logical name, not a concrete wire topic — each implementer maps the key to whatever topic name its broker/queue requires, via `consumer.TopicRegistry` in our Go wiring.

| Message | Direction | Topic key |
|---|---|---|
| `HookEvent` | producing domain → hook stage | `hook` |

The key is per-host: each domain runs its own hook topic and its own hook controller, so two domains sharing one queue backend must map `hook` to distinct topic names.

## Evolution

Contract changes are additive-only: add new fields; never remove, rename, repurpose, or retype an existing field, and never reuse a field number. protojson ignores unknown fields on read and omits zero-valued fields on write, so a new optional field is backward-compatible in both directions. New event types and new payload keys are not contract changes at all — that is the point of the open envelope.
