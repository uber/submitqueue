# Error Processing (`core/errs`)

The `errs` package provides a framework for classifying errors by origin and retryability. It wraps Go's standard `errors` package — all framework types support `errors.Is`/`errors.As` and participate in the standard error chain.

## Design

Errors are classified along two axes:

|               | Non-retryable (default) | Retryable                        |
|---------------|-------------------------|----------------------------------|
| **User**      | `NewUserError`          | *(not supported)*                |
| **Infra**     | *(any unclassified error)* | `NewRetryableError`           |
| **Infra dep** | `NewDependencyError`    | `NewRetryableDependencyError`    |

**Non-retryable by default.** A plain `fmt.Errorf(...)` is treated as a non-retryable infra error. Retryability must be explicitly opted into by wrapping with `NewRetryableError`. This prevents accidental infinite retry loops from unclassified errors.

**Only infra errors can be retryable.** User errors are never retryable — if a user action caused the failure, retrying the same operation will produce the same result. If an error is retryable, it is by definition an infrastructure issue.

**Infra by default.** Any error that is not explicitly wrapped with `NewUserError` is an infra error. There is no `NewInfraError` constructor — infra is the default classification.

## Two Routes to a Classification

A returned `error` reaches `IsUserError` / `IsRetryable` / `IsDependencyError` carrying one of the framework types (`*userError` / `*infraError`). It gets there one of two ways:

1. **Explicit wrap by the controller** — the controller knows the meaning of the failure and wraps the cause with `NewUserError`, `NewRetryableError`, `NewDependencyError`, or `NewRetryableDependencyError` before returning.
2. **Automatic wrap by `Classify`** — the controller returns a raw driver/library/sentinel error, and a per-backend `Classifier` recognises it later in the pipeline (typically inside the consumer) and adds the appropriate framework wrap.

Both routes feed the same downstream helpers; the chain that reaches `IsRetryable` looks identical regardless of who wrapped it.

## `Classify` and the `Classifier` Interface

`Classifier` inspects a **single error node** and returns a `Verdict`:

```go
type Classifier interface {
    Classify(err error) Verdict
}
```

Verdicts: `Unknown` (this node carries no signal), `User`, `Infra`, `InfraRetryable`, `InfraDependency`, `InfraDependencyRetryable`.

`Classify(err, classifiers...)` is the single, explicit pass that turns a raw chain into a wrapped one. It is called **exactly once per chain** — typically by the consumer immediately after the controller returns. After that point, callers use only the `IsXxx` helpers, which are pure type checks.

`Classify` walks the chain twice:

1. **Pass 1 — framework-wrap check.** A cheap type switch looks for an existing `*userError` / `*infraError` anywhere in the chain. If found, the chain is already interpretable and `Classify` returns `err` unchanged. **No classifier is invoked.**
2. **Pass 2 — classifier walk.** From outermost to innermost node, each registered classifier is asked for a verdict. The first non-`Unknown` verdict wins and `err` is wrapped with the matching framework constructor.

If no classifier recognises anything, `err` is returned unchanged — and behaves as non-retryable infra at the helper layer.

## Adding a Backend-Specific Classifier

Backend classifiers live alongside the extension they classify, under `core/errs/<backend>/`. The canonical examples are `core/errs/mysql` (MySQL driver errors) and `core/errs/generic` (transport-agnostic concerns such as `context.Canceled`).

A classifier:

- Inspects exactly one node — the `err` argument passed in. **Do not call `errors.Is` / `errors.As`** from inside `Classify`; the framework owns the chain walk. Calling it yourself can shadow a deeper-but-different verdict and breaks the controller-override rules described below.
- Returns `Unknown` for anything it does not recognise, so the surrounding walker can continue.
- Is stateless. The convention is to expose a package-level singleton value rather than a constructor:

```go
// core/errs/foo/foo.go
package foo

import "github.com/uber/submitqueue/core/errs"

var Classifier errs.Classifier = classifier{}

type classifier struct{}

func (classifier) Classify(err error) errs.Verdict {
    // Type-assert / sentinel-compare on err directly, never errors.As / errors.Is.
    if fe, ok := err.(*FooError); ok {
        return classifyFooCode(fe.Code)
    }
    return errs.Unknown
}
```

Servers wire each classifier into the consumer as a vararg. Order matters only when two classifiers might both match a node — earlier classifiers win:

```go
import (
    genericerrs "github.com/uber/submitqueue/core/errs/generic"
    mysqlerrs   "github.com/uber/submitqueue/core/errs/mysql"
)

c := consumer.New(logger, scope, registry,
    genericerrs.Classifier,
    mysqlerrs.Classifier,
)
```

Tests follow the same shape: assert per-node behaviour against `Classifier.Classify(node)` directly, and assert end-to-end behaviour by running `errs.Classify(err, Classifier)` and checking the helpers (`IsRetryable`, `IsUserError`, …) on the result. See `core/errs/mysql/mysql_test.go` and `core/errs/generic/generic_test.go`.

## Overriding Classification from a Controller

Because pass 1 short-circuits on the first framework wrap it finds, **an explicit wrap by the controller always wins over any classifier**. Use this when the controller has context the classifier cannot — typically when the same low-level error means different things in different call sites.

```go
result, err := c.storage.Get(ctx, id)
if errors.Is(err, storage.ErrNotFound) {
    // This caller treats "not found" as a user error: the user asked for an
    // unknown resource. The mysql classifier never gets a vote because the
    // framework wrap short-circuits pass 1.
    return errs.NewUserError(fmt.Errorf("request %s: %w", id, err))
}
if err != nil {
    // Hand the raw error to Classify — the mysql classifier will recognise
    // deadlocks, lock-wait timeouts, etc. and wrap them as retryable infra.
    return fmt.Errorf("get %s: %w", id, err)
}
```

Two practical rules fall out of the short-circuit semantics:

- **Wrap with a framework constructor as soon as the controller knows the right verdict.** Any wrap added later in the chain still wins, but wrapping early keeps the intent close to the decision.
- **A wrap anywhere in the chain blocks all classifiers — including for nodes deeper than the wrap.** If you want a classifier to still get a look at the cause, do not wrap above it. (In practice this is rare: controllers wrap because they have the final answer.)

## Extensions Return Plain Go Errors

Extension interfaces (`MergeChecker`, `Storage`, `Publisher`) return standard `error` values. They may define their own domain-specific sentinel errors (e.g. `storage.ErrNotFound`, `storage.ErrVersionMismatch`) but they do **not** classify errors as user or infra — that is the controller's (and `Classify`'s) job.

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

All three are type-only checks. They do not invoke classifiers — pair them with a preceding `Classify` call when the controller's error may not carry an explicit wrap.
