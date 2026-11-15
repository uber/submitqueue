# Request issue/project attribution API

The two read-only RPCs expose historical attribution for a single validation request:

- `ListRequestProjectsByIssue` selects a queue, request, task, and unit. It returns associations in ascending bytewise project order.
- `ListRequestIssuesByProject` selects a queue, request, and project. It returns associations in ascending bytewise task/unit order.

Both return issue/project pairs from that request's retained attribution, not the current open-issue set. Closing or reopening an issue in a later request does not change these results. Task-wide issues have no project attribution; the issue lookup requires a nonempty unit ID.

## Completion

Responses contain `attribution_complete`. It becomes true only after the request's `project_issues_recorded` event is retained, following all source and reverse-lookup writes. Until then, both RPCs return no associations and no continuation, even if some rows have already been written. Clients can retry the first page when attribution is incomplete.

An empty completed result means no recorded associations match the selected issue or project. An incomplete result does not establish that there are none. Abandoned recording does not become complete; request history exposes the abandonment reason. An unknown or no-longer-retained request history returns `NotFound`.

The APIs read immutable attribution sources and projections; they do not read operational issue state or invoke the attributer.

## Pagination and errors

Page size defaults to 50 and accepts 1 through 200. Each lookup reads the selected primary-key prefix with one extra row. It returns up to the page size and continues after the final returned association; the extra row is left for the next page. There are no post-scan filters or inspection budget because every row in the selected prefix matches.

Tokens are versioned and bound to the queue, request, lookup direction, and issue or project selector. Changing the page size is allowed; changing the selection requires starting at the first page. Results are exposed only after attribution completes, so recording cannot add earlier associations between pages under the stable-per-request attribution contract.

Malformed selectors, invalid page sizes, and mismatched or malformed tokens return `InvalidArgument`. Only configured queues can be queried. Reads use the existing primary keys without a secondary index or cross-table join.
