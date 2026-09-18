# Stovepipe GetProjectStatusByURI API

## Summary

`GetProjectStatusByURI` exposes the current validation status for one exact commit URI in a queue. The response identifies the validation request, its baseline and lifecycle state, its whole-repository result when one has been recorded, and any project results the implementation exposes.

A project is a consumer-defined deployable or consumable unit in the repository. The API does not define how projects are discovered, how validation work is selected, or how a project result is derived. Those are integration responsibilities outside this contract.

The API is the durable source of truth for validation status. Lifecycle notifications are advisory: consumers can reconcile missed or duplicate notifications by querying this endpoint.

## Contract

The request and response fields have the following API-level meaning. The published protobuf is the authoritative field-level contract.

| Request field | Required | Meaning |
| --- | --- | --- |
| `queue` | Yes | Identifies the queue in which to find the validation request. |
| `change_uri` | Yes | Identifies the exact commit URI under validation. |
| `projects` | Yes | Consumer-defined project IDs whose results are requested. At least one ID is required. |
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
| `projects` | Returns recorded results for requested project IDs. |
| `next_page_token` | Continues requested-project result pagination when another page exists. |

`request_state` is a stable public lifecycle vocabulary: `accepted`, `processing`, `succeeded`, `failed`, `cancelled`, or `superseded`. Clients must tolerate a future value. A terminal request state does not by itself mean that project results are complete.

Repository and project breakage degrees are independent projections. A degree is on `[0.0, 1.0]`: `0.0` is green and any value above `0.0` is not green. An absent degree means no durable result exists; it must never be interpreted as green. The API does not derive one scope's degree from another scope's results. `updated_at_ms` is derived only from durable lifecycle and result records.

| Response data | Durable source |
| --- | --- |
| Request identity, baseline, and lifecycle | Validation request |
| Repository breakage degree | Repository validation fact |
| Project results | Implementation-defined project-result records |
| Project completion and pagination | Project-result completion record and cursor |

## Request Selection

The lookup is queue-scoped. Stovepipe resolves `change_uri` through its request-URI mapping, loads the resulting request, and verifies that its queue and URI match the selector before reading validation facts.

| Observed state | Result |
| --- | --- |
| No request-URI mapping | Not found |
| Mapping exists but the Request is not visible | Unavailable and retryable |
| Mapping and Request disagree | Internal consistency error |

The initial request-URI mapping admits one request per `(queue, change_uri)`, so the endpoint returns one authoritative request. Revalidation support must introduce an explicit authoritative-request rule; it must not silently change this lookup's meaning.

Queue, URI, request ID, and project ID comparisons are byte-exact and are limited to 255 bytes. An empty queue, URI, project list, or project ID is invalid. A request cannot contain duplicate project IDs. `page_size=0` selects the default of 50; the maximum is 200. Page tokens are opaque and bound to the selected request and requested project list.

## Project Results and Pagination

The caller supplies the project IDs for which it wants recorded results. The API does not define project discovery, validation selection, or attribution. A project absent from the response has no universal meaning and consumers must not infer that it is green.

The response includes only results for the supplied project IDs, in the supplied order, and paginates that list. An implementation may record results for every requested project, only requested projects with a non-green result, or another documented subset.

Each project result is an immutable validation fact keyed by queue, commit URI, and project ID. A result must identify the selected request. `project_results_complete` is false until the implementation has durably finished producing results for the supplied project IDs; a completed empty set is valid.

Callers use `project_results_complete`, rather than the presence or absence of an individual project result, to determine whether the implementation has finished producing results for the requested projects. `next_page_token` is empty only on the final page. A terminal lifecycle notification is emitted only after the repository result and any applicable project completion record are durable. If notification delivery fails, consumers can recover the same result through this endpoint.

## Rollout

The initial implementation returns request lifecycle and whole-repository result only. It returns an empty `projects` list and `project_results_complete=false` because it does not yet produce project results. Its `updated_at_ms` value is returned only after a durable lifecycle record reflects the request state in the response.

A later implementation can add durable project results, completion recording, and pagination using its documented result-set semantics. The public response shape remains unchanged.

## Errors and Authorization

- Invalid selectors, an empty project list, an invalid project ID, duplicate project IDs, or malformed page tokens are user errors.
- An unknown queue-scoped commit URI is not found.
- A request-URI mapping whose Request is not visible is retryable.
- A mapping and Request that disagree, a result belonging to another request, or a completion record with a missing project result is an internal consistency error.
- Authorization follows the queue policy applied to other Stovepipe reads. A commit URI or request ID does not bypass queue access control.

## Testing

The initial implementation must test request selection, queue isolation, the request visibility race, absent versus green repository facts, lifecycle projection, and invalid selectors.

The project-result rollout additionally tests requested-project filtering, pagination and token binding, incomplete versus completed results, duplicate delivery, recovery after a partial write, and an unknown future lifecycle value.
