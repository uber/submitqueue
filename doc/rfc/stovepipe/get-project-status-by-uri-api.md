# Stovepipe GetProjectStatusByURI API

## Summary

Stovepipe exposes the current validation of a commit through `GetProjectStatusByURI`. The commit URI resolves to its newest authoritative request. Across one or more pages, the response combines that request's lifecycle, whole-repository result, and planned projects with any results recorded for them.

The endpoint reads `Request` for lifecycle and scope, `ValidationFact` for immutable results, and a request-owned project list for enumerating individual project results. The project list will be populated once individual project mapping is implemented.

The contract is based on the validation lookup flow in [Stovepipe <-> CD Integration: Event and API Contract](https://docs.google.com/document/d/1ouymU93l2a6lLiKwhqViSR1HFdwviuywj_sL6gDNeQM/edit?tab=t.n2tdz7ihz0sk).

## Published Contract

The wire contract lives in [`api/stovepipe/proto/stovepipe.proto`](../../../api/stovepipe/proto/stovepipe.proto). `GetProjectStatusByURIRequest` selects a queue and exact change URI, with either an optional exact project or cursor pagination over all planned projects.

The `repository_breakage_degree` and `ProjectStatus.breakage_degree` fields can be used to gate a commit or individual project. A missing degree means no result has been recorded; degree zero means fully green.

## How the Response Is Built

### Select the request

1. Use `queue` to select the queue-bound storage implementation.

2. Resolve `change_uri` to the authoritative `Request`.

3. Verify that the loaded request has the requested queue and URI before loading any validation results.

### Handle partially written state

Ingest writes the request-URI mapping before it creates the `Request` so concurrent ingests converge without a cross-record transaction. A lookup may race between those writes.

| State observed | API result |
| --- | --- |
| No request-URI mapping | Not found |
| Mapping exists but its `Request` is not visible yet | Unavailable and retryable |
| Mapping points to a `Request` with a different queue or URI | Internal consistency error |

### Assemble the response

| Response data | Durable source | Projection rule |
| --- | --- | --- |
| `request_id`, `queue`, `change_uri`, `base_uri` | `Request` | Copy from the verified authoritative request. |
| `request_state` | `Request.State` | Map to the stable public state vocabulary below. |
| `repository_breakage_degree` | Whole-repository `ValidationFact` | Leave absent until the fact exists; do not infer it from project results. |
| `projects` | Project list plus one exact `ValidationFact` read per project | Return every selected project even when its degree is not recorded yet. |
| `project_results_complete` | Durable completion marker | False until the project list is available; then set only after every listed project has a durable result. |
| `next_page_token` | Project list cursor | Return only when another project page exists. |

`request_state` uses one value for both lifecycle position and terminal outcome:

| Value | Meaning |
| --- | --- |
| `REQUEST_STATE_ACCEPTED` | The request is admitted but validation has not started. |
| `REQUEST_STATE_PROCESSING` | Validation is in progress. |
| `REQUEST_STATE_SUCCEEDED` | Validation reached a successful terminal outcome. |
| `REQUEST_STATE_FAILED` | Validation failed or the request could not continue. |
| `REQUEST_STATE_CANCELLED` | Validation was cancelled before reaching a verdict. |
| `REQUEST_STATE_SUPERSEDED` | A newer head replaced the request before it ran. |

The enum is explicitly projected from Stovepipe's internal `RequestState`; no domain enum crosses the boundary. Clients must tolerate unrecognized numeric values so states can be added compatibly. The API does not invent a `COMPLETED` versus `ERROR` distinction that the request does not persist. Adding that distinction later requires a durable outcome classification and an additive response field.

### Select projects

| Request shape | Result |
| --- | --- |
| `project` omitted | Return one bounded page of the project list, including projects whose degree is not recorded yet. |
| Non-empty `project` present | Return that project only, with no continuation token. The project must exist in the project list. |
| Empty `project`, or page fields combined with an exact project | Reject as invalid. |

The whole-repository fact and project facts are independent. The controller never infers one from the other. An initial coarse implementation may record the same derived outcome for every planned project, but it still persists the project list and each project fact so clients receive the same contract when finer analysis arrives.

## Project List and Completion

Each request owns one immutable project list. It contains unique project IDs in ascending bytewise order and is stored before any project results. The same list drives validation and the API; there is no separate response-only list.

`project_results_complete` becomes true only after every project in the list has a durable result and a completion marker has been recorded. A persisted empty project list may complete immediately. A terminal `request_state` does not imply that results are complete.

The `end_validation` event is published only after the whole-repository result and project completion are durable. If publishing fails, the durable state remains available and the event can be retried.

## Pagination and Consistency

| Rule | Behavior |
| --- | --- |
| Ordering | Projects are returned in ascending bytewise order. |
| `page_size` | Zero uses the default of 50; the maximum is 200. |
| `page_token` | Empty selects the first page. |
| `next_page_token` | Empty means the current page is the last page. |

Continuation tokens are opaque and tied to the queue, change URI, and resolved request ID. This keeps every page on the same request even if a newer request becomes authoritative during traversal. A malformed token or a token reused with a different query is invalid. Callers may change `page_size` between pages.

The project list does not change, so pagination does not skip or duplicate projects. Project results may appear while pages are being read; `project_results_complete`, not the contents of any one page, is the signal that every result is durable.

## Storage Identity and Evolution

URI, request ID, queue, and project identity use byte-exact comparison. API validation and storage schemas use the same explicit length limits, so an oversized selector is rejected before lookup rather than failing or truncating inside a backend.

The insert-once request-URI mapping selects the only request today. Supporting revalidation of the same URI widens validation-fact identity and changes the mapping into an explicitly versioned authoritative-request pointer advanced only after the new Request is durable. The point lookup remains singular; discovering older attempts can be added independently as a request-list API.

## Errors

- Empty queue or change URI is invalid.
- An empty explicit project, invalid page size, page fields used with a project selector, or invalid token is invalid.
- An unknown queue-scoped change URI or a project absent from its project list is not found.
- A URI mapping whose Request is not yet visible is unavailable and retryable; a mismatched Request, missing project list for a request that should have been planned, a completion marker with any missing fact, or a fact attributed to another request is an internal consistency error.
- Retryable storage failures are unavailable; context cancellation and deadline errors retain their canonical codes.

## Rollout Phases

The initial version returns an empty project list, `project_results_complete` set to false, and a repository degree of either 0 or 1. Once individual project-level mapping is available, project results and completion are included.
