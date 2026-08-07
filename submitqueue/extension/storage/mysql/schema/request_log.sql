-- request_log is the append-only audit trail of request status transitions. queue leads the
-- PK so the table is shardable by queue; request_id is unique only within its queue. salt
-- disambiguates entries sharing a request, queue and millisecond.
CREATE TABLE IF NOT EXISTS request_log (
    queue VARCHAR(255) NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    salt BIGINT NOT NULL,
    status VARCHAR(64) NOT NULL,
    request_version INT NOT NULL,
    last_error TEXT NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (queue, request_id, timestamp_ms, salt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
