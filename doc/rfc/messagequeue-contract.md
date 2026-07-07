# Message Queue Contract

How queue payloads are defined, located, and bound to topics across domains.

## Problem

Queue payloads are Go structs serialized with `encoding/json` (`submitqueue/entity`, `runway/entity`), so the wire shape is defined only by Go source. Three gaps:

- **No language-neutral contract.** Some payloads cross a domain boundary — a client written in another language has nothing to compile or validate against.
- **No topic-to-payload binding.** `consumer.TopicRegistry` maps a `TopicKey` to a backend, topic name, and subscription — but not to the payload schema. That knowledge lives implicitly in whichever controller (de)serializes.
- **No audience distinction.** Nothing separates private wiring between our own services from a published cross-domain contract.

## Decisions

### Contract language: Protobuf

Payloads are defined as **proto3 messages**. The `.proto` is the language-neutral authority, and the Go binding is generated from it. This is the same mechanism the RPC contracts in `api/` already use, so queue payloads and RPC payloads share one toolchain, one set of shared field types, and one mental model. Generation runs through the repo's existing hermetic `protoc` Bazel rule (`tool/proto`); message-only contracts (no service) skip the gRPC/YARPC plugins.

The decisive property is that the binding is *generated*, not hand-authored. There is no separate Go struct that can drift from the contract, and therefore no drift test to keep them in sync — the only binding is the generated one.

### Wire format: protobuf JSON

Payloads stay JSON on the wire; messages are serialized with **protobuf JSON (`protojson`)**, not binary proto. The MySQL-backed queue keeps storing self-describing JSON, exactly as before — only the source of the (de)serialization changes from a hand-written `encoding/json` struct to a generated message.

protojson has its own conventions, which the contract adopts deliberately:

- **Field names are the proto names (snake_case).** Serialized with `UseProtoNames`, so `queue_name` stays `queue_name` rather than protojson's default `queueName`. The wire matches the declared field names.
- **Enums serialize as their value name in UPPER_SNAKE** (`REBASE`, `SQUASH_REBASE`, `MERGE`) — the proto-conventional spelling.
- **64-bit integers serialize as strings.** A protojson rule for cross-language safety; relevant for any millisecond timestamp or count a future payload carries.
- **Unknown fields are ignored on read** and zero-valued fields are omitted on write, which gives additive evolution for free: a consumer skips fields it does not yet know.

### Location: audience decides

A contract is **external** when something outside its owning domain depends on it — another domain's service, or a client written in another language. It is **internal** when only the owning domain's own services use it. The test is concrete: *does anything outside this domain compile or deserialize against it?*

- **External** → `api/{domain}/messagequeue/`. The `api/` prefix is the published surface; outside code is expected to depend on it.
- **Internal** → `{domain}/core/messagequeue/`.

The `.proto`, its generated `protopb`, and any Go helpers co-locate in each home; only the home differs.

### Visibility

Bazel [`visibility`](https://bazel.build/concepts/visibility) enforces the split: `{domain}/core/messagequeue/` targets are scoped to the owning domain, so depending on one from outside is a build error; `api/` targets are public. No metadata keyword — the directory carries the distinction.

### Topic-key binding: the `topic_keys` option

Each payload message declares a **`topic_keys`** option: the stable logical topic keys that carry it (a message may list several — one payload can serve a queue pair). It is the single source of truth; the reverse index (key → message) is derivable, not authored. A topic key is **not** a concrete wire name — each implementer maps the key to whatever topic name its broker/queue requires (subject to that backend's naming constraints). On our Go side the keys are the `consumer.TopicKey` values, mapped to concrete names through the `TopicRegistry`.

`topic_keys` is a custom proto option — an extension of `google.protobuf.MessageOptions` defined once in `api/base/messagequeue` — so the binding travels with the message in its compiled descriptor, readable by any proto consumer in any language rather than living in out-of-band Go wiring.

**How it's consumed.** The option is read off the message descriptor by reflection; it is *not* on the publish/consume hot path. Publishing and consuming still resolve a concrete topic name from a `consumer.TopicKey` through the `TopicRegistry`, unchanged — the option does not replace that wiring. Instead, two readers consume it:

- A **contract test** reads the option (via a small `TopicKeys(msg)` helper) and asserts that every `consumer.TopicKey` the domain registers is carried by exactly one message, and that no option names an unknown key. This is the guard that keeps the language-neutral binding and the Go wiring from silently drifting apart.
- A **non-Go client** reads the same option straight from the compiled descriptor to discover which key carries which payload, then maps each key to a concrete topic name per its own backend, with no access to our Go types.

So the option earns its place as the single, language-neutral source of truth for the topic-key↔payload binding and as the anchor the drift test checks the Go wiring against — not as a runtime lookup.

### Go binding: the generated `protopb`

The generated message types in `protopb` are the Go binding, sitting beside `proto/` exactly as for the RPC contracts. The contract package adds only thin helpers — `protojson` (de)serialization and the `topic_keys` reflection lookup. Shared field types (`change.Change`, `mergestrategy.MergeStrategy`) are themselves shared protos under `api/base/{change,mergestrategy}/proto`, imported by every contract that needs them.

## Example

Two illustrative payloads. `ExampleRequest` is carried on a single topic;
`ExampleResult` shows the list form — one payload that serves a queue pair, so
it repeats the `topic_keys` option once per topic key:

```proto
syntax = "proto3";

package uber.example.messagequeue;

import "api/base/messagequeue/proto/messagequeue.proto";

message ExampleRequest {
    option (uber.base.messagequeue.topic_keys) = "example-request";

    string id = 1;    // Client-owned correlation id.
    string mode = 2;  // "fast" or "thorough".
    repeated string items = 3;
}

message ExampleResult {
    // One shape, two queues: the same result is published under the check-result
    // key for a dry run and the merge-result key for a committing run.
    option (uber.base.messagequeue.topic_keys) = "example-check-result";
    option (uber.base.messagequeue.topic_keys) = "example-merge-result";

    string id = 1;       // Echoes the request's correlation id.
    bool success = 2;
}
```

A conforming `ExampleRequest` wire value:

```json
{ "id": "req-42", "mode": "fast", "items": ["a", "b"] }
```

## Rejected

- **JSON Schema for payloads.** A hand-authored schema duplicates the message definition and needs a drift test to stay in sync with a hand-authored Go struct. Proto generates the Go binding from the one definition, so the duplication — and the test guarding it — disappears; the contract also shares the toolchain and shared types with the RPC surface.
- **Binary proto / Avro on the wire.** Binary loses the self-describing JSON the MySQL-backed queue relies on, and Avro's value is a schema registry for decoding binary, which we do not have. protojson keeps the wire as JSON while still generating the binding.
- **One unified `api/` tree with audience as metadata.** Fine for inert schemas, but co-locating the generated binding pulls internal types into the published surface; a directory boundary matching audience is more honest.
