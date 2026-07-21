# Shared Platform Errors

Define shared error identities for conditions that have the same meaning across domains. A shared identity enables consistent `errors.Is` checks and, where appropriate, a common platform classification. Sharing an error identity does not by itself make the error retryable.

## Problem

SubmitQueue and Stovepipe define duplicate sentinels for common conditions such as a missing resource and an optimistic version conflict. Callers must know which domain package produced the error, and platform code cannot apply a common policy without importing domain packages or adding domain-specific classifiers.

| Concern | Current | Proposed |
|---|---|---|
| Error identity | Each domain defines equivalent sentinels | `platform/errs` owns shared semantic sentinels |
| Usage | Callers check domain aliases such as `storage.ErrVersionMismatch` | Callers use `platformerrs.ErrVersionMismatch` directly |
| Retry policy | Classification is repeated in controllers or domain wiring | The platform classifies only errors whose recovery policy is universal |
| Domain contracts | Storage and configuration contracts own both APIs and common error identities | Domain contracts remain domain-owned; only shared error identities move |

## Proposal

An error belongs in `platform/errs` when:

1. It has the same meaning across multiple domains.
2. Callers benefit from one `errors.Is` identity.
3. Its meaning is independent of a domain entity or extension API.
4. Any default classification applied by the platform is valid for every use.

Domains use shared errors directly rather than re-exporting them through permanent aliases:

```go
if errors.Is(err, platformerrs.ErrVersionMismatch) {
    // Handle workflow-specific convergence.
}
```

Implementations preserve context with wrapping:

```go
return fmt.Errorf("update batch %s: %w", batch.ID, platformerrs.ErrVersionMismatch)
```

A domain may define a more specific error that wraps a platform error when callers need both identities. Domain storage and configuration interfaces remain in their existing packages.

## Initial errors

| Error | Meaning | Default platform classification |
|---|---|---|
| `ErrVersionMismatch` | An optimistic conditional update lost a concurrent race | `InfraRetryable` |
| `ErrNotFound` | A requested resource does not exist | None; remains non-retryable by default |

`ErrVersionMismatch` has a universal recovery policy: reload current state and retry or converge. `ErrNotFound` does not. Depending on the call site, absence may be an expected result, a user error, or an infrastructure invariant violation. Controllers may add a contextual classification only when they have information the shared error does not carry.

The generic classifier recognizes `ErrVersionMismatch` through the existing error-chain walk. This proposal does not change retry limits, backoff, DLQ policy, or delivery guarantees.
