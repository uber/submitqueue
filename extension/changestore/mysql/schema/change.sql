-- request_id is part of the PK so concurrent claims by different requests on the
-- same URI coexist as distinct rows. Same-request retry → PK conflict (no-op via
-- INSERT IGNORE). Different-request collision → distinct row, surfaced by
-- FindOverlapping. Queue leads the PK so queue-scoped lookups are PK-prefix scans
-- and the table is shardable by queue.
CREATE TABLE IF NOT EXISTS `change` (
    uri         VARCHAR(255) NOT NULL,
    request_id  VARCHAR(255) NOT NULL,
    queue       VARCHAR(255) NOT NULL,
    metadata    JSON NOT NULL,
    created_at  BIGINT NOT NULL,
    updated_at  BIGINT NOT NULL,
    PRIMARY KEY (queue, uri, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
