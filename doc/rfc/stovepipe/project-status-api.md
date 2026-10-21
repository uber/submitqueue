# Stovepipe Project Status APIs

## Summary

`GetProjectStatusByID` and `GetProjectStatusByURI` expose the current validation status for one request ID or exact commit URI in a queue. The response identifies the validation request, its baseline and lifecycle state, its whole-repository result when one has been recorded, and any project results the implementation exposes.

A project is a consumer-defined deployable or consumable unit in the repository. The API does not define how projects are discovered, how validation work is selected, or how a project result is derived. Those are integration responsibilities outside this contract.

The API is the durable source of truth for validation status. Lifecycle notifications are advisory: consumers can reconcile missed or duplicate notifications by querying either endpoint.

## Contract

The request and response fields have the following API-level meaning. The published protobuf is the authoritative field-level contract.

| Request field | Required | Meaning |
| --- | --- | --- |
| `queue` | Yes | Identifies the queue in which to find the validation request. |
| `request_id` | ID lookup | Canonical positive decimal request ID returned by `Ingest`. |
| `change_uri` | URI lookup | Identifies the exact commit URI under validation. |
| `projects` | No | Consumer-defined project IDs whose results are requested. When omitted or empty, returns recorded project results, most broken first. |
| `page_size` | No | Limits one page of results for the requested projects. |
| `page_token` | No | Continues a previous page of results for the requested projects. |

| Response field | Meaning |
| --- | --- |
| `request_id` | Identifies the resolved authoritative validation request. |
| `queue`, `change_uri`, `base_uri` | Return the validated scope and incremental-validation baseline. |
| `request_state` | Returns the request's public lifecycle state. |
| `updated_at_ms` | Records, in Unix milliseconds, the newest durable lifecycle or result record represented by the response. |
| `repository_breakage_degree` | Returns the whole-repository result when it is durably recorded. |
| `project_results_complete` | Indicates whether the implementation has durably finished producing results for the supplied project IDs. |
| `projects` | Returns recorded results for requested project IDs in the supplied order, or recorded project results ordered by descending breakage degree, then ascending project ID when none are supplied. |
| `next_page_token` | Continues requested-project result pagination when another page exists. |

`request_state` is a stable public lifecycle vocabulary: `accepted`, `processing`, `succeeded`, `failed`, `cancelled`, or `superseded`. Clients must tolerate a future value. A terminal request state does not by itself mean that project results are complete.

Repository and project breakage degrees are independent projections. A degree is on `[0.0, 1.0]`: `0.0` is green and any value above `0.0` is not green. An absent degree means no durable result exists; it must never be interpreted as green. The API does not derive one scope's degree from another scope's results. `updated_at_ms` is derived only from durable lifecycle and result records.

| Response data | Durable source |
| --- | --- |
| Request identity, baseline, and lifecycle | Durable request summary |
| Repository breakage degree | Repository validation fact |
| Project results | Implementation-defined project-result records |
| Project completion and pagination | Project-result completion record and cursor |

## Request Selection

Both lookups are queue-scoped and use the same durable request summary and validation facts. `GetProjectStatusByID` loads the summary directly by `request_id`. `GetProjectStatusByURI` resolves `change_uri` through its request-URI mapping, then loads the summary. The selected summary must match the queue and request ID, and for URI lookups its URI must match the selector, before validation facts are read.

| Observed state | Result |
| --- | --- |
| ID lookup has no request summary | Not found |
| URI lookup has no request-URI mapping | Not found |
| URI mapping exists but the request summary is not visible | Unavailable and retryable |
| Selector and request summary disagree | Internal consistency error |

The request-URI mapping admits one request per `(queue, change_uri)`, so the endpoint returns one authoritative request. Revalidation support must introduce an explicit authoritative-request rule; it must not silently change this lookup's meaning.

Queue, URI, request ID, and project ID comparisons are byte-exact and are limited to 255 bytes. An empty queue, lookup selector, or individual project ID is invalid. A request cannot contain duplicate project IDs. `page_size=0` selects the default of 50; the maximum is 200. Request IDs use the same canonical positive decimal format as `Ingest` and request-history lookups. Page tokens are opaque and bound to the selected request and requested project list; either endpoint can continue a page for the same queue, request, and projects.

## Project Results and Pagination

The caller may supply the project IDs for which it wants recorded results. The API does not define project discovery, validation selection, or attribution. A project absent from the response has no universal meaning and consumers must not infer that it is green.

When project IDs are supplied, the response includes only results for those IDs, in the supplied order, and paginates that list. An implementation may record results for every requested project, only requested projects with a non-green result, or another documented subset.

With no project IDs supplied, the response lists recorded project results by descending breakage degree, then ascending project ID, excluding the repository fact. If the result for the cursor project is missing, the API returns an error. Results are a live view; restart pagination to discover facts recorded before the cursor.

Each project result is an immutable validation fact keyed by queue, commit URI, and project ID. A result must identify the selected request. `project_results_complete` is false until the implementation has durably finished producing results for the supplied project IDs; a completed empty set is valid.

Callers use `project_results_complete`, rather than the presence or absence of an individual project result, to determine whether the implementation has finished producing results for the requested projects. `next_page_token` is empty only on the final page. A terminal lifecycle notification is emitted only after the repository result and any applicable project completion record are durable. If notification delivery fails, consumers can recover the same result through either endpoint.

## Implementation

Both endpoints return durable lifecycle summaries, the whole-repository fact when present, and recorded facts for requested projects in paginated request order, or recorded project results by descending breakage degree then ascending project ID when none are requested. `project_results_complete` remains false until a durable completion projection is available. Missing facts are omitted; an explicitly recorded zero degree remains present. `updated_at_ms` is the maximum timestamp of the summary and the facts represented on the page.

A dedicated ordering mapping makes listing a bounded primary-key range read, not a sort over all facts. Listing reads at most `page_size + 1` ordering mappings and hydrates only the returned facts, plus the cursor fact on continuations. The continuation token identifies the last project; its immutable fact supplies the degree needed to resume this ordering.

## Errors and Authorization

- Invalid selectors, an invalid project ID, duplicate project IDs, or malformed page tokens are user errors.
- An unknown queue-scoped request ID or commit URI is not found.
- A request-URI mapping whose summary is not visible is retryable.
- A selector and summary that disagree, a result belonging to another request, or a completion record with a missing project result is an internal consistency error.
- Authorization follows the queue policy applied to other Stovepipe reads. A commit URI or request ID does not bypass queue access control.

## Testing

The initial implementation must test request selection, queue isolation, the request visibility race, absent versus green repository facts, lifecycle projection, and invalid selectors.

The project-result rollout additionally tests requested-project filtering, pagination and token binding, incomplete versus completed results, duplicate delivery, recovery after a partial write, and an unknown future lifecycle value.
