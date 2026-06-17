# Counter

Vendor-agnostic interface for atomic sequential number generation.

## Interface

### Counter

Generates unique, sequential values scoped to a domain string.

```go
type Counter interface {
    Next(ctx context.Context, domain string) (int64, error)
}
```

- **domain**: A string key that scopes the counter (max 255 characters). Each domain maintains its own independent sequence.
- **Next**: Atomically increments and returns the next value. The first call for a new domain returns 1. Safe for concurrent use; values are unique but ordering is not guaranteed.

## Usage

```go
cnt := mysqlcounter.NewCounter(db)

// Generate sequential IDs for different domains
val, err := cnt.Next(ctx, "request/my-queue") // returns 1
val, err = cnt.Next(ctx, "request/my-queue")  // returns 2
val, err = cnt.Next(ctx, "request/other")     // returns 1
```

## Implementing a Backend

1. Create `platform/extension/counter/{backend}/` directory
2. Implement the `Counter` interface
3. Add a schema file under `platform/extension/counter/{backend}/schema/` if the backend requires it
