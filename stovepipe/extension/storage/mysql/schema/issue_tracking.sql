-- Persistent source-order boundary; unfinished passes are resumed before admitting another source.
CREATE TABLE IF NOT EXISTS issue_tracking (
    queue                       VARBINARY(255)  NOT NULL,
    request_id                  VARBINARY(255)  NOT NULL,
    build_id                    VARBINARY(1020) NOT NULL,
    notification_id             VARBINARY(1020) NOT NULL,

    cursor_task_id              VARBINARY(255)  NOT NULL,
    cursor_unit_id              VARBINARY(1024) NOT NULL,

    planned_status              VARBINARY(32)   NOT NULL DEFAULT '',
    planned_first_seen_build_id VARBINARY(1020) NOT NULL DEFAULT '',
    planned_expected_version    INT             NOT NULL DEFAULT 0,
    planned_timestamp_ms        BIGINT          NOT NULL DEFAULT 0,

    phase                       VARBINARY(32)   NOT NULL,
    version                     INT             NOT NULL,
    PRIMARY KEY (queue)
) ENGINE=InnoDB;
