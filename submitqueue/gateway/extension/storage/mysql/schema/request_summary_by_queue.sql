CREATE TABLE IF NOT EXISTS request_summary_by_queue (
    queue VARCHAR(255) NOT NULL,
    received_at_ms BIGINT NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    change_uris JSON NOT NULL,
    status VARCHAR(64) NOT NULL,
    version INT NOT NULL,
    last_error TEXT NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (queue, received_at_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
