# Conduit client

Conduit is Phabricator's HTTP API. This package is the one way SubmitQueue code calls it: it encodes a method's parameters, authenticates the call, and turns Conduit's reply into either the caller's typed result or a typed error. It holds a single generic `Call` and no per-method wrappers.

## Setting it up

Build the client once in wiring and share it; it is safe for concurrent use.

- Start from an `*http.Client` whose transport resolves relative paths against the Phabricator host (`platform/http.NewClient(endpoint)` installs a `BaseURLTransport`). That client owns the timeout and connection limits.
- Wrap it with `NewClient(httpClient, token)`, where `token` is a Conduit API token. One client serves one host and one token.

The package never reads configuration or credentials itself.

## What a call sends

`Call(ctx, method, params, result)` posts a form to `/api/<method>` with two fields:

- `params`: the caller's parameters as a JSON object, with the token added under `__conduit__`. Parameters may be a map or a struct, or nil for none; anything that does not encode as a JSON object is rejected before a request is sent.
- `output`: `json`.

The context bounds the request, so cancelling it or hitting its deadline aborts the call.

### Why the token is on the client, not in a transport

Other HTTP clients in `platform/` leave auth to a transport the wiring installs. Conduit cannot work that way: once the parameters are JSON, it reads the token only from `__conduit__` and silently ignores an `api.token` query or form field, answering `ERR-INVALID-SESSION`. A transport would have to decode and rewrite the request body, so the token is passed to `NewClient` instead.

That makes `*Client` the counterpart of the authenticated `*http.Client` GitHub callers take (see `platform/githubapi`): service wiring builds it once from its own credentials, and extensions receive it.

## What a call returns

| Conduit's reply | `Call` returns |
|---|---|
| HTTP 200, `result` set | `nil`, with `result` decoded into the caller's value |
| HTTP 200, `result` is `null` or `[]` | `nil`, with the caller's value untouched. PHP encodes an empty map as `[]`, so an empty array means "nothing", not "wrong shape". |
| HTTP 200, `error_code` set | an `*Error` carrying `Code` and `Info` |
| any other HTTP status | a `platform/http.StatusError` carrying the status and body |
| a body that is not JSON, or a result that does not fit the caller's type | a plain error |

Every error is wrapped with the method name.

### Classifying failures

- A `StatusError` is classified by `platform/errs/http` like any other HTTP failure: a 5xx or 429 is retryable, other statuses are not.
- An `*Error` is not an HTTP status, because Conduit reports its own failures with HTTP 200. That includes a missing or rejected token (`ERR-INVALID-SESSION`). With no classifier for it, such an error is non-retryable infra by default, so a bad token dead-letters rather than retrying forever. A caller that knows a code means something else, such as "not applicable", matches on `Code` itself.

## What is deliberately not here

- **Per-method wrappers.** Each caller keeps its own typed request and result for the methods it calls, and decides what a missing object means for it. A method only one caller uses never lands in this package.
- **Retries, logging and metrics.** Retry decisions belong to the consumer's error classifiers, and callers already log and record metrics in their own scope.
