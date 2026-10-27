-- Persistent source-order boundary; unfinished passes are resumed before admitting another source.
CREATE TABLE IF NOT EXISTS issue_tracking (
    queue                VARBINARY(255)  NOT NULL,
    request_id           VARBINARY(255)  NOT NULL,
    build_id             VARBINARY(1020) NOT NULL,
    notification_id      VARBINARY(1020) NOT NULL,

    cursor_task_id       VARBINARY(255)  NOT NULL,
    cursor_unit_id       VARBINARY(1024) NOT NULL,

    finalize             BOOLEAN         NOT NULL DEFAULT FALSE,
    has_task_wide_issues BOOLEAN         NOT NULL DEFAULT FALSE,
    phase                VARBINARY(32)   NOT NULL,
    version              INT             NOT NULL,
    has_new_issues       BOOLEAN         NOT NULL DEFAULT FALSE,
    PRIMARY KEY (queue)
) ENGINE=InnoDB;
