-- Membership records filing each in-queue request under its public status, mirroring
-- request_summary.status. The (queue, status, request_id) PK *is* the lookup: "requests of
-- queue Q at status S" is a PK-prefix scan, so request_summary needs no secondary index.
-- Records are advisory: readers confirm each listed request against its summary.
CREATE TABLE IF NOT EXISTS queue_request_status (
    queue          VARCHAR(255) NOT NULL,
    status         VARCHAR(64)  NOT NULL,
    request_id     VARCHAR(255) NOT NULL,
    received_at_ms BIGINT       NOT NULL,
    entered_at_ms  BIGINT       NOT NULL,
    PRIMARY KEY (queue, status, request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
