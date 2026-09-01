-- request_log retains one append-only row per logical request occurrence. Exactly one of
-- state and event is populated; core lifecycle fields remain columns, while occurrence-specific
-- context and optional diagnostics share metadata.
CREATE TABLE IF NOT EXISTS request_log (
    queue                     VARCHAR(255) NOT NULL,
    request_id                VARCHAR(255) NOT NULL,
    log_id                    VARCHAR(255) NOT NULL,
    timestamp_ms              BIGINT       NOT NULL,
    state                     VARCHAR(64)  NOT NULL,
    event                     VARCHAR(64)  NOT NULL,
    request_version           INT          NOT NULL,
    outcome_reason            VARCHAR(64)  NOT NULL,
    metadata                  JSON         NOT NULL,
    PRIMARY KEY (queue, request_id, log_id),
    KEY idx_request_log_order (queue, request_id, timestamp_ms, log_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
