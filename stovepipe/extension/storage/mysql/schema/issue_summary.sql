-- Closed summaries retain their source order independently of the projection's CAS version.
CREATE TABLE IF NOT EXISTS issue_summary (
    queue               VARBINARY(255)  NOT NULL,
    task_id             VARBINARY(255)  NOT NULL,
    unit_id             VARBINARY(1024) NOT NULL,

    status              VARBINARY(32)   NOT NULL,
    first_seen_build_id VARBINARY(1020) NOT NULL,
    request_id          VARBINARY(255)  NOT NULL,
    build_id            VARBINARY(1020) NOT NULL,
    changed_at_ms       BIGINT          NOT NULL,
    version             INT             NOT NULL,

    PRIMARY KEY (queue, task_id, unit_id)
) ENGINE=InnoDB;
