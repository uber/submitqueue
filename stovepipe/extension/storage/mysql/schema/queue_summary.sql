-- queue_summary is the current read model projected from retained queue logs.
CREATE TABLE IF NOT EXISTS queue_summary (
    queue                     VARBINARY(255) NOT NULL,
    state                     VARCHAR(32)  NOT NULL,
    effective_from_commit_uri VARCHAR(255) NOT NULL,
    state_timestamp_ms        BIGINT       NOT NULL,
    queue_state_version       BIGINT       NOT NULL,
    version                   BIGINT       NOT NULL,
    PRIMARY KEY (queue)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
