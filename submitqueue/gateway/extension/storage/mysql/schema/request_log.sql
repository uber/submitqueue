-- request_log is the append-only audit trail of what happened to a request. queue leads the
-- PK so the table is shardable by queue; request_id is unique only within its queue. salt
-- disambiguates entries sharing a request, queue and millisecond.
--
-- type says whether a row records a status the request reached or an event that happened
-- while it sat at one, and exactly one of status and event is set accordingly. It defaults
-- to 'status' so rows written before the column existed, when every row was a status, read
-- back correctly.
CREATE TABLE IF NOT EXISTS request_log (
    queue VARCHAR(255) NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    salt BIGINT NOT NULL,
    type VARCHAR(16) NOT NULL DEFAULT 'status',
    status VARCHAR(64) NOT NULL,
    event VARCHAR(64) NOT NULL DEFAULT '',
    request_version INT NOT NULL,
    last_error TEXT NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (queue, request_id, timestamp_ms, salt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
