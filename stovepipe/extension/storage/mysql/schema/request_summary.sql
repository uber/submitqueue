-- request_summary is the request-ID-keyed materialized view used by public status reads. The
-- request version identifies the winning lifecycle state; version guards concurrent projection
-- writers. queue leads the primary key so the table remains shardable by queue.
CREATE TABLE IF NOT EXISTS request_summary (
    queue              VARCHAR(255) NOT NULL,
    request_id         VARCHAR(255) NOT NULL,
    uri                VARCHAR(255) NOT NULL,
    base_uri           VARCHAR(255) NOT NULL,
    state              VARCHAR(64)  NOT NULL,
    request_version    INT          NOT NULL,
    state_timestamp_ms BIGINT       NOT NULL,
    version            INT          NOT NULL,
    PRIMARY KEY (queue, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
