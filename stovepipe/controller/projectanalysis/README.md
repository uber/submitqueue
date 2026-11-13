# Project analysis

Project analysis consumes request IDs after terminal build analysis has retained all final build issues. A missing terminal-analysis completion is an invalid handoff, not a reason to poll.

The controller resolves project statuses and records their facts and lookup entries before attributing issues. It materializes both directions of each issue/project association. Each step retains its own event after its writes. Once both steps finish, the controller records `project_analysis_completed`, updates the request-summary completion timestamp, publishes `project.analysis.completed`, and queues repository recording. Project-fact and attribution events do not set that timestamp; attribution queries still require the successful attribution event.

Retries adopt immutable facts and repair missing lookup entries. A completed request skips provider calls, repairs its summary if needed, and retries hook publication followed by the recording handoff. Empty results still record completion. Cancelled requests bypass project analysis; superseded requests require no work.

## Completion hook

The `project.analysis.completed` payload contains only `queue` and `request_id`. Consumers resolve project counts from the request summary and project details from the retained facts and associations. Completion is published even when no projects are affected. Cancelled, superseded, abandoned, and incomplete analyses do not publish this hook.

Publication follows the durable completion log and its summary projection, and precedes the recording handoff. The event ID hashes the queue/request identity and the timestamp comes from the retained completion log. A failed publication is retried without calling providers again. Delivery is at least once; consumers performing external side effects must durably deduplicate by event ID.

## Recovery

DLQ recovery leaves existing facts and association rows untouched. If `project_analysis_completed` was already recorded, it repairs the summary. Otherwise it records `project_analysis_abandoned`, without calling either provider or marking attribution complete. Recovery never publishes hooks or queues repository recording. If normal publication exhausts retries, the completion notification may be lost. History and summary repairs are safe to retry.

Normal redeliveries also recognize abandonment, repair its summary, and stop. An already-running delivery may still finish immutable writes; abandonment does not roll them back or make a partial association set complete.
