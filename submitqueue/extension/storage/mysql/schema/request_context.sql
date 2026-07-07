CREATE TABLE IF NOT EXISTS request_context (
    request_id VARCHAR(255) NOT NULL,
    queue VARCHAR(255) NOT NULL,
    change_uri JSON NOT NULL,
    admitted_at_ms BIGINT NOT NULL,
    PRIMARY KEY (request_id),
    KEY idx_queue_admitted (queue, admitted_at_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
