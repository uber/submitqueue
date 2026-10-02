# Scoped Sequential Resource IDs

## Decision

A generated resource ID is the canonical decimal string for a positive value returned by a durable counter scoped to `(queue, resource type)` within an application's storage.

SubmitQueue and Stovepipe use separate storage backends. Resource type is `request` or `batch`; there is no additional application-domain key or schema change.

| Resource | Current ID | Proposed ID | Identity within the application |
|---|---:|---:|---|
| SubmitQueue request | `demo-queue/42` | `"42"` | `(demo-queue, request, "42")` |
| SubmitQueue batch | `demo-queue/batch/7` | `"7"` | `(demo-queue, batch, "7")` |
| Stovepipe request | `request/monorepo/main/42` | `"42"` | `(monorepo/main, request, "42")` |

The decimal ID is unique only within its scope. The same value may appear in another queue, resource type, or application. APIs and messages therefore carry the queue separately; their typed field or message type supplies the resource type.

Do not embed scope into the ID. Forms such as `demo-queue/42`, `demo-queue/batch/7`, `request.42`, and ARN-like resource names are not stored or accepted as IDs.

## Counter

The counter backend persists one high-water mark per `(queue, resource type)`. MySQL keeps its existing `(queue, domain)` primary key; `domain` stores the resource type. For example:

```text
(demo-queue, request) -> 42
(demo-queue, batch)   -> 7
```

Controllers allocate an ID before creating the resource; stores accept the caller-supplied ID and never generate one.

- The first ID is `1`; `0` is the unset value.
- Allocation is atomic across replicas and durable across restarts.
- Allocated values are never reused. Failed writes may leave gaps.
- Overflow fails instead of wrapping.
- Numeric order is allocation order only within the same scope.

The counter contract requires an atomic durable increment, not MySQL specifically. MySQL remains the initial implementation.

## Storage and contracts

Resource tables keep IDs as strings. Queue remains the leading key:

```text
request(queue, id VARCHAR(...), ...) PRIMARY KEY (queue, id)
batch(queue, id VARCHAR(...), ...)   PRIMARY KEY (queue, id)
```

Reference columns use the same string type. Domain entities may use distinct named string types such as `RequestID` and `BatchID`; protobuf resource fields remain `string`. The counter backend may store its high-water marks as integers, and controllers convert allocated values to canonical decimal strings before creating resources.

This proposal applies only to counter-generated resources. Provider build IDs, message and hook IDs, change URIs, and content hashes keep their existing contracts.

## URLs and display

| Before — base64url or percent-encoded | After — readable |
|---|---|
| `/queues/demo-queue/requests/ZGVtby1xdWV1ZS80Mg`<br>or `/queues/demo-queue/requests/demo-queue%2F42` | `/queues/demo-queue/requests/42` |
| `/queues/demo-queue/batches/ZGVtby1xdWV1ZS9iYXRjaC83`<br>or `/queues/demo-queue/batches/demo-queue%2Fbatch%2F7` | `/queues/demo-queue/batches/7` |
| `/queues/demo-queue/changes/Z2l0aHViOi8v…`<br>or `/queues/demo-queue/changes/github%3A%2F%2Fgithub.com%2Fuber%2Frepo%2Fpull%2F123%2F{sha}` | `/queues/demo-queue/changes/github/github.com/uber/repo/pull/123` |
| `/queues/demo-queue/changes/cGhhYjovL3BoYWIuZXhhbXBsZS5jb20vRDEyMzQ1LzY3ODkw`<br>or `/queues/demo-queue/changes/phab%3A%2F%2Fphab.example.com%2FD12345%2F67890` | `/queues/demo-queue/changes/phab/phab.example.com/D12345` |

Both columns use the same queue prefix for comparison. The before batch/change routes and percent-encoded alternatives are illustrative, not implemented pages; GitHub base64url is abbreviated, and `{sha}` stands for a full lowercase commit SHA.

## Rejected alternatives

- **Queue or resource-type prefixes:** duplicate explicit context, lengthen keys, require parsing, and introduce URL separators.
- **ARN-like names:** solve global lookup, which current APIs neither provide nor require.
- **UUIDs or a global counter:** provide global uniqueness at the cost of unnecessary encoding or coordination.
- **Integer resource fields:** couple the persisted and wire contracts to the current counter representation without adding identity semantics.
- **SQL auto-increment or `MAX(id) + 1`:** move allocation into one storage implementation or fail under concurrency.
- **Process-local counters:** reuse IDs after restart and collide across replicas.
