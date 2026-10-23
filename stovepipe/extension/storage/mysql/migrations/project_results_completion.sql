ALTER TABLE request_summary
    ADD COLUMN project_results_completed_at_ms BIGINT NOT NULL DEFAULT 0;
