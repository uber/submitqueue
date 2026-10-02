# Scoped Sequential Resource IDs

## Status

Proposed.

## Decision

A generated resource ID is the canonical decimal string for a positive value returned by a durable counter scoped to `(queue, domain)` within an application's storage.

The counter domain names the sequence (`request` or `batch`), not the application. SubmitQueue and Stovepipe use separate storage backends; there is no additional application-domain key or schema change.

| Resource | Current ID | Proposed ID | Identity within the application |
|---|---:|---:|---|
| SubmitQueue request | `demo-queue/42` | `"42"` | `(demo-queue, request, "42")` |
| SubmitQueue batch | `demo-queue/batch/7` | `"7"` | `(demo-queue, batch, "7")` |
| Stovepipe request | `request/monorepo/main/42` | `"42"` | `(monorepo/main, request, "42")` |

The decimal ID is unique only within its scope. The same value may appear in another queue, counter domain, or application. APIs and messages therefore carry the queue separately; their typed field or message type supplies the counter domain.

Do not embed scope into the ID. Forms such as `demo-queue/42`, `demo-queue/batch/7`, `request.42`, and ARN-like resource names are not stored or accepted as IDs.

## Counter

The counter backend persists one high-water mark per `(queue, domain)`. MySQL keeps its existing schema and primary key. For example:

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

The decimal ID is used directly as one path segment:

```text
/requests/42
/batches/7
```

The route supplies the counter domain; the request context supplies the queue. Queue URL design is separate.

A UI may display `request.42`, `batch.7`, or `#42`, but those are derived labels, not identities.

## Rejected alternatives

- **Queue or domain prefixes:** duplicate explicit context, lengthen keys, require parsing, and introduce URL separators.
- **ARN-like names:** solve global lookup, which current APIs neither provide nor require.
- **UUIDs or a global counter:** provide global uniqueness at the cost of unnecessary encoding or coordination.
- **Integer resource fields:** couple the persisted and wire contracts to the current counter representation without adding identity semantics.
- **SQL auto-increment or `MAX(id) + 1`:** move allocation into one storage implementation or fail under concurrency.
- **Process-local counters:** reuse IDs after restart and collide across replicas.
