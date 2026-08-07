-- request_summary is the authoritative per-request materialized view serving direct Status
-- lookup. queue leads the PK so the table is shardable by queue; request_id is unique only
-- within its queue.
CREATE TABLE IF NOT EXISTS request_summary (
    queue VARCHAR(255) NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    change_uris JSON NOT NULL,
    received_at_ms BIGINT NOT NULL,
    status VARCHAR(64) NOT NULL,
    request_version INT NOT NULL,
    status_timestamp_ms BIGINT NOT NULL,
    version INT NOT NULL,
    last_error TEXT NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (queue, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
