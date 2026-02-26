# Error Processing (`core/errs`)

The `errs` package provides a framework for classifying errors by origin and retryability. It wraps Go's standard `errors` package — all framework types support `errors.Is`/`errors.As` and participate in the standard error chain.

## Design

Errors are classified along two axes:

|               | Non-retryable (default) | Retryable                        |
|---------------|-------------------------|----------------------------------|
| **User**      | `NewUserError`          | *(not supported)*                |
| **Infra**     | *(any unclassified error)* | `NewRetryableError`           |
| **InfraDep**| `NewDependencyError`    | `NewRetryableDependencyError`    |

**Non-retryable by default.** A plain `fmt.Errorf(...)` is treated as a non-retryable infra error. Retryability must be explicitly opted into by wrapping with `NewRetryableError`. This prevents accidental infinite retry loops from unclassified errors.

**Only infra errors can be retryable.** User errors are never retryable — if a user action caused the failure, retrying the same operation will produce the same result. If an error is retryable with the same inputs, it is by definition an infrastructure issue.

**Infra by default.** Any error that is not explicitly wrapped with `NewUserError` is an infra error. There is no `NewInfraError` constructor — infra is the default classification.

## Who Classifies Errors

**Extensions return plain Go errors.** Extension interfaces (`MergeChecker`, `Storage`, `Publisher`) return standard `error` values. They may define their own domain-specific sentinel errors (e.g. `storage.ErrNotFound`, `storage.ErrVersionMismatch`) but they do not classify errors as user or infra.

**Service controllers classify errors.** The controller that calls an extension decides whether the error is user-caused or infrastructure-caused, and whether it should be retried:

```go
func (c *Controller) Process(ctx context.Context, delivery consumer.Delivery) error {
    // Extension returns a plain error
    result, err := c.mergeChecker.Check(ctx, queue, change)
    if err != nil {
        // Controller classifies: merge checker failure is infra, worth retrying
        return errs.NewRetryableError(fmt.Errorf("merge check failed: %w", err))
    }

    if !result.Mergeable {
        // Controller classifies: not mergeable is a user error, never retry
        return errs.NewUserError(fmt.Errorf("not mergeable: %s", result.Reason))
    }

    // ...
}
```

This separation keeps extensions reusable across contexts. The same `storage.ErrNotFound` might be a user error in one controller (user requested a non-existent resource) and an infra error in another (expected record is missing).

## Error Chain Compatibility

Framework types preserve the full error chain. Extensions can wrap their own custom errors, and both framework-level and cause-level matching work through `errors.Is`/`errors.As`:

```go
// Extension defines a domain error
var ErrNotFound = errors.New("record not found")

// Extension implementation wraps it
return fmt.Errorf("request id=%s: %w", id, ErrNotFound)

// Controller classifies and wraps again
return errs.NewUserError(fmt.Errorf("lookup failed: %w", extensionErr))

// All of these work on the resulting error:
errs.IsUserError(err)             // true — framework classification
errs.IsRetryable(err)             // false — user errors are never retryable
errors.Is(err, ErrNotFound)       // true — cause is in the chain
```

## Helpers

| Helper              | Returns `true` when                                          |
|---------------------|--------------------------------------------------------------|
| `IsUserError(err)`  | `err` is or wraps a `userError`                              |
| `IsRetryable(err)`  | `err` is or wraps an infra error with the retryable flag set |
| `IsDependencyError(err)` | `err` is or wraps an infra error marked as dependency   |
