# Scoped Sequential Resource IDs

## Status

Proposed.

## Decision

A generated resource ID is the positive `int64` returned by a durable counter scoped to `(owner domain, queue, resource kind)`.

| Resource | Current ID | Proposed ID | Complete identity |
|---|---:|---:|---|
| SubmitQueue request | `demo-queue/42` | `42` | `(submitqueue, demo-queue, request, 42)` |
| SubmitQueue batch | `demo-queue/batch/7` | `7` | `(submitqueue, demo-queue, batch, 7)` |
| Stovepipe request | `request/monorepo/main/42` | `42` | `(stovepipe, monorepo/main, request, 42)` |

The numeric ID is unique only within its scope. The same value may appear in another queue, resource kind, or domain. APIs and messages therefore carry the queue separately; their typed field or message type supplies the resource kind.

Do not embed scope into the ID. Forms such as `demo-queue/42`, `demo-queue/batch/7`, `request.42`, and ARN-like resource names are not stored or accepted as IDs.

## Counter

The counter backend persists one high-water mark per `(owner domain, queue, resource kind)`. For example:

```text
(submitqueue, demo-queue, request) -> 42
(submitqueue, demo-queue, batch)   -> 7
(stovepipe, demo-queue, request)   -> 11
```

Controllers allocate an ID before creating the resource; stores accept the caller-supplied ID and never generate one.

- The first ID is `1`; `0` is the unset value.
- Allocation is atomic across replicas and durable across restarts.
- Allocated values are never reused. Failed writes may leave gaps.
- Overflow fails instead of wrapping.
- Numeric order is allocation order only within the same scope.

The counter contract requires an atomic durable increment, not MySQL specifically. MySQL remains the initial implementation.

## Storage and contracts

Resource tables store the numeric value directly. Queue remains the leading key:

```text
request(queue, id BIGINT, ...) PRIMARY KEY (queue, id)
batch(queue, id BIGINT, ...)   PRIMARY KEY (queue, id)
```

Reference columns use the same numeric type. Domain entities use distinct named types such as `RequestID` and `BatchID`, and protobuf resource fields use `int64`.

This proposal applies only to counter-generated resources. Provider build IDs, message and hook IDs, change URIs, and content hashes keep their existing contracts.

## URLs and display

The decimal ID is used directly as one path segment:

```text
/requests/42
/batches/7
```

The route supplies the resource kind; the request context supplies the queue. Queue URL design is separate.

A UI may display `request.42`, `batch.7`, or `#42`, but those are derived labels, not identities.

## Rejected alternatives

- **Queue or kind prefixes:** duplicate explicit context, lengthen keys, require parsing, and introduce URL separators.
- **ARN-like names:** solve global lookup, which current APIs neither provide nor require.
- **UUIDs or a global counter:** provide global uniqueness at the cost of unnecessary encoding or coordination.
- **SQL auto-increment or `MAX(id) + 1`:** move allocation into one storage implementation or fail under concurrency.
- **Process-local counters:** reuse IDs after restart and collide across replicas.
