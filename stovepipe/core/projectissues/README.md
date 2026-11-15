# Request issue attribution materialization

The materializer retains each issue/project pair in `request_projects_by_issue` before creating its reverse lookup in `request_issues_by_project`. It uses single-row creates, with no transaction or separate consumer.

Both rows are immutable and contain only their primary-key fields. A duplicate therefore identifies the same pair; there is no payload to reconcile. Replaying a retained pair still attempts the reverse write, repairing a lookup left missing by an interrupted attempt. Requests have separate keys, so an older request cannot replace a newer request's attribution.

The helper handles one pair at a time. Its caller owns attribution computation and the request-level completion event, and must retry failed pairs before marking attribution complete. A successful pair does not mean the entire request's attribution is complete.
