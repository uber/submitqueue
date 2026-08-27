# Stovepipe GetProjectStatusByURI API

## Summary

Stovepipe exposes the current validation of one queue and commit through `GetProjectStatusByURI`. The response combines request lifecycle, the whole-repository result, and the request's complete planned project list with any results recorded for those projects.

This is a current-state API, not another history projection. It reads `Request` for lifecycle and scope, `ValidationFact` for immutable results, and a request-owned project manifest for enumeration and completion. The manifest is also the single project list used by validation planning and downstream consumers.

The contract is based on the validation lookup flow in [Stovepipe <-> CD Integration: Event and API Contract](https://docs.google.com/document/d/1ouymU93l2a6lLiKwhqViSR1HFdwviuywj_sL6gDNeQM/edit?tab=t.n2tdz7ihz0sk).

## Representative Contract

The final protobuf receives a separate compatibility review before implementation. Its representative shape is:

```proto
message GetProjectStatusByURIRequest {
    string queue = 1;
    string change_uri = 2;
    optional string project = 3;
    int32 page_size = 4;
    string page_token = 5;
}

message ValidationResult {
    double degree = 1;
}

message ProjectValidation {
    string project = 1;
    ValidationResult result = 2;
}

message GetProjectStatusByURIResponse {
    string request_id = 1;
    string queue = 2;
    string change_uri = 3;
    string base_uri = 4;
    string request_state = 5;
    ValidationResult repository_result = 6;
    bool project_results_complete = 7;
    repeated ProjectValidation projects = 8;
    string next_page_token = 9;
}

service Stovepipe {
    rpc GetProjectStatusByURI(GetProjectStatusByURIRequest) returns (GetProjectStatusByURIResponse) {}
}
```

Message fields preserve presence. An absent `repository_result` or `ProjectValidation.result` means no fact has been recorded; it never means green. `project_results_complete` is true only after every project in the persisted manifest has exactly one durable result and the request's completion marker has been written.

The existing degree scale answers “how broken is this scope”: `0.0` is fully green, `1.0` is fully broken, and project analysis may assign intermediate values. The API preserves those Stovepipe semantics rather than inverting the scale at the wire boundary.

## Selection and Projection

`queue` and `change_uri` identify one request through the existing request-URI mapping. Permanent ingest deduplication makes this cardinality one request per `(queue, change_uri)`. The controller binds storage to the queue, reads `request_uri` by its `(queue, URI)` primary key, then reads `request` by its `(queue, request ID)` primary key. It does not scan or query the Request table by an attribute.

The controller verifies that the loaded Request has the requested queue and URI before loading its project manifest and whole-repository fact. Request URI is immutable once the request and mapping are created; storage updates must not permit the two records to diverge.

Ingest claims `request_uri` before creating the Request so concurrent ingests converge without a cross-record transaction. A lookup racing that sequence can therefore observe a mapping whose Request is not present yet. `GetProjectStatusByURI` treats that condition as unavailable and retryable rather than not found or immediate corruption. Repeated occurrences are surfaced through consistency metrics and require repair; a mapping to a Request with a different queue or URI is an internal consistency error.

`request_state` uses the stable public vocabulary `accepted`, `processing`, `succeeded`, `failed`, `cancelled`, and `superseded`. One value identifies both lifecycle position and terminal outcome without permitting contradictory field combinations. The wire field is a string, following SubmitQueue's current-status and history APIs, so clients can tolerate additive vocabulary changes. It is projected explicitly from Stovepipe's internal `RequestState`; no SubmitQueue domain enum is shared across the boundary.

`failed` retains the current conservative meaning: either validation failed or the request could not continue. The API does not synthesize a `COMPLETED` versus `ERROR` distinction that the Request does not persist. Adding that distinction later requires a durable outcome classification first, followed by an additive response field.

When `project` is omitted, the response contains one bounded page of the manifest, including planned projects whose result is not yet present. When `project` is present, it must be a non-empty project in the manifest; the response contains only that project and no continuation token. A nonzero page size or nonempty page token with an exact project selector is invalid.

The whole-repository fact is independent of project facts. The controller does not infer project results from it. An initial coarse implementation may record the same derived outcome for every planned project, but it still persists the project list and each project fact so clients receive the same contract when finer analysis arrives.

## Project Manifest and Completion

Project analysis creates one immutable, request-owned logical manifest containing unique project IDs in ascending bytewise order. It persists the manifest before recording project facts. The manifest is addressed by request ID and serves as the index for exact `ValidationFactStore.Get(changeURI, project)` reads; `ValidationFactStore` does not gain `ListByURI` or another secondary-index query.

The storage representation does not require the entire list to fit in one row. A backend may keep a root record keyed by request ID that names immutable deterministic chunks and their ordered boundaries. This preserves exact get-by-key operations while allowing page reads to avoid loading a service list that was already too large for an event payload.

The project list used to execute validation is the list returned by this API. No separate response-only service list is maintained. A retry that proposes a different list for the same request is an invariant violation rather than an in-place replacement.

After facts have been recorded, the owner of project analysis verifies one fact for every manifest entry using exact reads and writes a durable, idempotent completion marker. Fact creation remains independent and retryable; no multi-record transaction or count query is required. Empty manifests may complete immediately.

Request terminal state and result completion are intentionally distinct. The current pipeline makes the request terminal before `record` persists its fact, so terminal state alone cannot promise result availability. A downstream `end_validation` event is published only after the whole-repository result is durable, the completion marker is durable, and every planned project has exactly one durable result. Persist-before-publish makes a failed publish safe to retry.

## Pagination and Consistency

Pagination follows SubmitQueue's queue-listing convention: an empty token selects the first page, zero page size selects the server default, and `next_page_token` is empty on the final page. The initial default is 50 projects and the maximum is 200.

The controller reads the immutable manifest in `project ASC` order and inspects one project beyond the effective page size before issuing a continuation token. The opaque, versioned token represents the exclusive position after the last returned project and is bound to the queue, change URI, resolved request ID, and all-project selector mode. Decoding produces a typed manifest cursor; neither the manifest store nor the fact store parses public tokens. The cursor may include a deterministic chunk position in addition to the last project ID without exposing either representation to clients. Page size is not bound, so callers may change it between pages. A malformed token, unsupported version, or token reused for another query is invalid.

After reading one manifest page, the controller loads its project facts through bounded parallel exact reads. The caller owns that concurrency; the fact-store contract remains a portable single-key `Get` rather than requiring SQL `IN`, batch atomicity, or a secondary index.

The manifest is immutable, so pages neither skip nor duplicate project identities. Facts are create-only and may appear between reads: an incomplete traversal can observe more results on later pages, while a new traversal reflects all facts available at its start. `project_results_complete` is the durable signal that no planned result remains absent; it is not computed by counting the current page.

## Storage Identity and Evolution

URI, request ID, queue, and project identity use byte-exact comparison. MySQL schemas declare an explicit binary collation for these key columns rather than inherit the server's case- or accent-insensitive default. API validation and storage schemas use the same explicit length limits, so an oversized selector is rejected before lookup rather than failing or truncating inside a backend.

The insert-once request-URI mapping intentionally selects only one request today. Supporting revalidation of the same URI requires a separate design that widens validation-fact identity and changes the mapping into an explicitly versioned authoritative-attempt pointer. It must not silently turn this method into an ambiguous multi-request lookup.

## Errors and Authorization

- Empty queue or change URI is invalid.
- An empty explicit project, invalid page size, page fields used with a project selector, or invalid token is invalid.
- An unknown queue-scoped change URI or a project absent from its manifest is not found.
- A URI mapping whose Request is not yet visible is unavailable and retryable; a mismatched Request, missing manifest for a request that should have been planned, a completion marker with any missing fact, or a fact attributed to another request is an internal consistency error.
- Retryable storage failures are unavailable; context cancellation and deadline errors retain their canonical codes.

Authorization follows the queue policy used by other Stovepipe reads. Both the request-URI mapping and every loaded entity remain scoped to the supplied queue.

## Rollout and Testing

The endpoint is enabled only after project-manifest persistence, project-fact recording, the completion marker, and `end_validation` publish ordering are deployed. This avoids exposing a nominal all-project response that silently omits the service list.

Contract and controller tests cover request-state projection, result presence at degree zero, whole-repository and project independence, exact project selection, incomplete and complete manifests, empty and chunked manifests, first/middle/final pages, token query binding, stable ordering, cross-queue isolation, mapping-before-Request retries, mismatched identities, byte-distinct keys, storage limits, and publish-after-completion ordering.
