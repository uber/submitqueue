UPDATE request_summary AS summary
JOIN request_log AS completion
    ON completion.queue = summary.queue
    AND completion.request_id = summary.request_id
    AND completion.log_id = 'event/project_facts_recorded/all'
SET summary.project_results_completed_at_ms = completion.timestamp_ms,
    summary.version = summary.version + 1
WHERE summary.queue = CAST(@queue AS BINARY)
    AND summary.request_id >= CAST(@first_request_id AS BINARY)
    AND summary.request_id <= CAST(@last_request_id AS BINARY)
    AND summary.project_results_completed_at_ms = 0
    AND completion.event = 'project_facts_recorded'
    AND completion.state = ''
    AND completion.request_version = 0
    AND completion.timestamp_ms > 0;
