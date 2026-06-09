CREATE TABLE IF NOT EXISTS request_summary (
    request_id VARCHAR(255) NOT NULL,
    queue VARCHAR(255) NOT NULL,
    change_uri JSON NOT NULL,
    status VARCHAR(64) NOT NULL,
    request_version INT NOT NULL,
    status_timestamp_ms BIGINT NOT NULL,
    winner_terminal_version BOOLEAN NOT NULL,
    last_error TEXT NOT NULL,
    metadata JSON NOT NULL,
    started_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    completed_at_ms BIGINT NOT NULL,
    terminal BOOLEAN NOT NULL,
    version BIGINT NOT NULL,
    PRIMARY KEY (request_id),
    KEY idx_request_summary_queue_started (queue, started_at_ms DESC, request_id DESC),
    KEY idx_request_summary_queue_completed (queue, completed_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
