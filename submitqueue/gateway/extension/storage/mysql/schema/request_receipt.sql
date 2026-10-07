-- Immutable lookup keys; the full projection remains in request_summary.
-- Match the existing queue projection's key types to preserve List ordering.
CREATE TABLE IF NOT EXISTS request_receipt (
    queue VARCHAR(255) NOT NULL,
    received_at_ms BIGINT NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (queue, received_at_ms, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
