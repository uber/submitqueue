-- queue_log retains immutable successful state occurrences. State version determines
-- ordering and derives log identity, so every history read can use the primary key.
CREATE TABLE IF NOT EXISTS queue_log (
    queue VARBINARY(255) NOT NULL,
    log_id VARBINARY(255) NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    state VARCHAR(32) NOT NULL,
    queue_state_version BIGINT NOT NULL,
    effective_from_commit_uri VARBINARY(255) NOT NULL,
    PRIMARY KEY (queue, log_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
