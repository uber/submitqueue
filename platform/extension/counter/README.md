# Counter

Vendor-agnostic interface for atomic sequential number generation, scoped by owner domain, queue, and resource kind.

## Interface

### Factory

Resolves the Counter bound to one queue. The host wiring decides which backend serves which queue and injects the owner domain; a resolved instance can only advance that scope's sequences.

### Counter

Generates unique, sequential values scoped to a resource kind within the bound owner domain and queue.

- **resource kind**: A string key naming a sequence within the bound scope (max 255 characters). Each `(owner domain, queue, resource kind)` tuple maintains its own independent sequence.
- **Next**: Atomically increments and returns the next value. The first call for a new resource kind returns 1. Safe for concurrent use; values are unique but ordering is not guaranteed.

The resource kind is a sequence name, not an ID prefix. Callers pass `"request"` or `"batch"`; the returned number is formatted as a decimal string without embedding any scope.

## Usage

```go
cnt, err := factory.For(counter.Config{QueueName: "my-queue"})

val, err := cnt.Next(ctx, "request") // returns 1
val, err = cnt.Next(ctx, "request")  // returns 2
val, err = cnt.Next(ctx, "batch")    // returns 1, an independent sequence

other, err := factory.For(counter.Config{QueueName: "other-queue"})
val, err = other.Next(ctx, "request") // returns 1, isolated from my-queue
```

## Implementing a Backend

1. Create `platform/extension/counter/{backend}/` directory
2. Implement the `Counter` interface, binding the owner domain and queue at construction
3. Add a schema file under `platform/extension/counter/{backend}/schema/` if the backend requires it. The persisted key must preserve the complete `(owner domain, queue, resource kind)` scope.
4. Adapt the constructor to the `Factory` interface in the wiring layer, not here
