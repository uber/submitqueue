# Error Processing (`platform/errs`)

The `errs` package provides a framework for classifying errors by origin and retryability. It wraps Go's standard `errors` package — all framework types support `errors.Is`/`errors.As` and participate in the standard error chain.

## Design

Errors are classified along two axes:

|               | Non-retryable (default) | Retryable                        |
|---------------|-------------------------|----------------------------------|
| **User**      | `NewUserError`          | *(not supported)*                |
| **Infra**     | *(any unclassified error)* | `NewRetryableError`           |
| **Infra dep** | `NewDependencyError`    | `NewRetryableDependencyError`    |

**Non-retryable by default.** A plain `fmt.Errorf(...)` is treated as a non-retryable infra error. Retryability must be explicitly recognized by a registered classifier or framework wrapper. This prevents accidental infinite retry loops from unclassified errors.

**Only infra errors can be retryable.** User errors are never retryable — if a user action caused the failure, retrying the same operation will produce the same result. If an error is retryable, it is by definition an infrastructure issue.

**Infra by default.** Any error that is not explicitly wrapped with `NewUserError` is an infra error. There is no `NewInfraError` constructor — infra is the default classification.

## Two Routes to a Classification

A returned `error` reaches `IsUserError` / `IsRetryable` / `IsDependencyError` carrying one of the framework types (`*userError` / `*infraError`). It gets there one of two ways:

1. **Explicit wrap by the controller** — the controller knows the meaning of the failure and wraps the cause with `NewUserError`, `NewRetryableError`, `NewDependencyError`, or `NewRetryableDependencyError` before returning.
2. **Automatic wrap by the classifier-based `ErrorProcessor`** — the controller returns a raw driver/library/sentinel error, and a per-backend `Classifier` recognises it later in the pipeline (typically inside the consumer, after `ErrorProcessor.Process` runs) and adds the appropriate framework wrap.

Both routes feed the same downstream helpers; the chain that reaches `IsRetryable` looks identical regardless of who wrapped it.

## `ErrorProcessor`, `Classifier`, and the Processing Pass

`Classifier` inspects a **single error node** and returns a `Verdict`:

```go
type Classifier interface {
    Classify(err error) Verdict
}
```

Verdicts: `Unknown` (this node carries no signal), `User`, `Infra`, `InfraRetryable`, `InfraDependency`, `InfraDependencyRetryable`.

An `ErrorProcessor` runs the per-chain pass that turns a raw chain into a wrapped one. It is called **exactly once per chain** — typically by the consumer immediately after the controller returns. After that point, callers use only the `IsXxx` helpers, which are pure type checks.

Two implementations ship in this package:

- **`NewClassifierProcessor(classifiers...)`** — the standard pass for primary pipeline consumers. Walks the chain twice:
  1. **Pass 1 — framework-wrap check.** A cheap type switch looks for an existing `*userError` / `*infraError` anywhere in the chain. If found, the chain is already interpretable and the processor returns `err` unchanged. **No classifier is invoked.**
  2. **Pass 2 — classifier walk.** From outermost to innermost node, each registered classifier is asked for a verdict. The first non-`Unknown` verdict wins and `err` is wrapped with the matching framework constructor.

  If no classifier recognises anything, `err` is returned unchanged — and behaves as non-retryable infra at the helper layer.

- **`AlwaysRetryableProcessor`** — unconditionally wraps every non-nil error with `NewRetryableError`, overriding any inner framework wrap. Use it for narrowly-scoped consumers — typically DLQ reconciliation — that must redeliver on any failure because there is no further dead-letter destination. Side-effect: an inner `*infraError(dependency=true)` is masked by the outer `retryable=true` wrap, since `errors.As` matches the outermost `*infraError` first. This is acceptable for the intended DLQ use case where only `IsRetryable` drives transport behaviour; do not pair this processor with a primary pipeline consumer or genuine user errors will retry forever instead of reaching their DLQ.

### Choosing a processor

- **Primary pipeline consumer** → `NewClassifierProcessor(...)`. Controllers' explicit `NewUserError` / `NewDependencyError` wraps must survive so user errors don't get retried, and unclassified backend errors must be inspected by the registered classifiers.
- **DLQ reconciliation consumer** → `AlwaysRetryableProcessor`. The DLQ is the last stop; any unprocessable message must come back for another attempt rather than silently drop. The DLQ subscription itself runs with a very high `Retry.MaxAttempts` and with its own DLQ disabled, so "always retryable + bounded-but-effectively-infinite attempts" is the convergence guarantee.

## Adding a Backend-Specific Classifier

Backend classifiers live alongside the extension they classify, under `platform/errs/<backend>/`. The canonical examples are `platform/errs/mysql` (MySQL driver errors) and `platform/errs/generic` (backend-independent errors such as `context.Canceled` and `errs.ErrVersionMismatch`).

`platform/errs` also owns shared error identities whose meaning is stable across domains. Identity and classification are separate: `ErrVersionMismatch` is classified as retryable, while `ErrNotFound` remains unclassified and therefore non-retryable by default.

A classifier:

- Inspects exactly one node — the `err` argument passed in. **Do not call `errors.Is` / `errors.As`** from inside `Classify`; the framework owns the chain walk. Calling it yourself can shadow a deeper-but-different verdict and breaks the controller-override rules described below.
- Returns `Unknown` for anything it does not recognise, so the surrounding walker can continue.
- Is stateless. The convention is to expose a package-level singleton value rather than a constructor:

```go
// platform/errs/foo/foo.go
package foo

import "github.com/uber/submitqueue/platform/errs"

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

Servers wire each classifier into the consumer's `ErrorProcessor`. Order matters only when two classifiers might both match a node — earlier classifiers win:

```go
import (
    "github.com/uber/submitqueue/platform/errs"
    genericerrs "github.com/uber/submitqueue/platform/errs/generic"
    mysqlerrs   "github.com/uber/submitqueue/platform/errs/mysql"
)

c := consumer.New(logger, scope, registry,
    errs.NewClassifierProcessor(
        genericerrs.Classifier,
        mysqlerrs.Classifier,
    ),
)
```

Tests follow the same shape: assert per-node behaviour against `Classifier.Classify(node)` directly, and assert end-to-end behaviour by running `errs.NewClassifierProcessor(Classifier).Process(err)` and checking the helpers (`IsRetryable`, `IsUserError`, …) on the result. See `platform/errs/mysql/mysql_test.go` and `platform/errs/generic/generic_test.go`.

## Overriding Classification from a Controller

Because pass 1 short-circuits on the first framework wrap it finds, **an explicit wrap by the controller always wins over any classifier**. Use this when the controller has context the classifier cannot — typically when the same low-level error means different things in different call sites.

```go
result, err := c.storage.Get(ctx, id)
if errors.Is(err, errs.ErrNotFound) {
    // This caller treats "not found" as a user error: the user asked for an
    // unknown resource. The mysql classifier never gets a vote because the
    // framework wrap short-circuits pass 1.
    return errs.NewUserError(fmt.Errorf("request %s: %w", id, err))
}
if err != nil {
    // Hand the raw error to the consumer's ErrorProcessor — the mysql
    // classifier will recognise deadlocks, lock-wait timeouts, etc. and wrap
    // them as retryable infra.
    return fmt.Errorf("get %s: %w", id, err)
}
```

Two practical rules fall out of the short-circuit semantics:

- **Wrap with a framework constructor as soon as the controller knows the right verdict.** Any wrap added later in the chain still wins, but wrapping early keeps the intent close to the decision.
- **A wrap anywhere in the chain blocks all classifiers — including for nodes deeper than the wrap.** If you want a classifier to still get a look at the cause, do not wrap above it. (In practice this is rare: controllers wrap because they have the final answer.)

### When *not* to classify in a controller

The controller-override path is for the rare case where the controller has certain knowledge a classifier cannot derive from the error value alone, such as `errs.ErrNotFound` meaning "the user asked for something missing" in this specific call site. The default and overwhelmingly common case is the opposite: the controller returns the raw error (`return fmt.Errorf("...: %w", err)`) and lets the consumer's `ErrorProcessor` classify it.

In particular, **do not reach for `NewRetryableError` just because replaying the message would be convenient.** A failed queue publish, a failed enqueue, a "the hand-off that keeps this alive" step — these are *not* a license to mark the error retryable. Whether such a failure is transient is exactly what a classifier exists to decide: a transport-level classifier wraps genuine connection/timeout blips as retryable, while a malformed-request or permission failure stays non-retryable and dead-letters instead of replaying forever. Blanket `NewRetryableError` on a publish path defeats that and turns every permanent failure into an infinite retry loop.

## Extensions Return Plain Go Errors

Extension interfaces (`MergeChecker`, `Storage`, `Publisher`) return standard `error` values. They use shared platform errors directly when the meaning is stable across domains, and may define domain-specific sentinels for domain-specific conditions. Extensions do **not** classify errors as user or infra. That is the controller's and the consumer's `ErrorProcessor`'s job.

This separation keeps extensions reusable across contexts. The same `errs.ErrNotFound` might be a user error in one controller (the user requested a missing resource) and an infra error in another (an expected record is missing).

## Error Chain Compatibility

Framework types preserve the full error chain. Extensions can wrap their own custom errors, and both framework-level and cause-level matching work through `errors.Is`/`errors.As`:

```go
// Extension implementation wraps a shared error
return fmt.Errorf("request id=%s: %w", id, errs.ErrNotFound)

// Controller classifies and wraps again
return errs.NewUserError(fmt.Errorf("lookup failed: %w", extensionErr))

// All of these work on the resulting error:
errs.IsUserError(err)             // true — framework classification
errs.IsRetryable(err)             // false — user errors are never retryable
errors.Is(err, errs.ErrNotFound)  // true: cause is in the chain
```

## Helpers

| Helper              | Returns `true` when                                          |
|---------------------|--------------------------------------------------------------|
| `IsUserError(err)`  | `err` is or wraps a `userError`                              |
| `IsRetryable(err)`  | `err` is or wraps an infra error with the retryable flag set |
| `IsDependencyError(err)` | `err` is or wraps an infra error marked as dependency   |

All three are type-only checks. They do not invoke classifiers — pair them with a preceding `ErrorProcessor.Process` call when the controller's error may not carry an explicit wrap.
