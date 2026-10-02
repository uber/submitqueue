# Counter

Vendor-agnostic interface for atomic sequential number generation, scoped by queue and domain.

The domain names a counter sequence (`request` or `batch`), not an application. SubmitQueue and Stovepipe use separate storage backends; the existing MySQL schema and `(queue, domain)` key remain unchanged.

## Interface

### Factory

Resolves the Counter bound to one queue. The host wiring decides which backend serves which queue; a resolved instance can only advance that queue's sequences.

### Counter

Generates unique, sequential values scoped to a domain within the bound queue.

- **domain**: A string key naming a sequence within the queue (max 255 characters). Each `(queue, domain)` pair maintains its own independent sequence.
- **Next**: Atomically increments and returns the next value. The first call for a new domain returns 1. Safe for concurrent use; values are unique but ordering is not guaranteed.

The domain is not an ID prefix. Callers pass `"request"` or `"batch"`; the returned number is formatted as a decimal string without embedding any scope.

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
2. Implement the `Counter` interface, binding the queue at construction
3. Add a schema file under `platform/extension/counter/{backend}/schema/` if the backend requires it. The queue must lead the primary key so the table is shardable by queue.
4. Adapt the constructor to the `Factory` interface in the wiring layer, not here
