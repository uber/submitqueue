-- Each opening, repeat report, or closure is retained once per build and event.
CREATE TABLE IF NOT EXISTS issue_log (
    queue                VARBINARY(255) NOT NULL,
    task_id              VARBINARY(255) NOT NULL,
    unit_id              VARBINARY(1024) NOT NULL,
    build_id             VARBINARY(1020) NOT NULL, -- Accommodates existing VARCHAR(255) utf8mb4 build IDs.
    event                VARBINARY(32) NOT NULL,

    timestamp_ms         BIGINT NOT NULL,

    first_seen_build_id  VARBINARY(1020) NOT NULL DEFAULT '',

    PRIMARY KEY (queue, task_id, unit_id, build_id, event)
) ENGINE=InnoDB;
