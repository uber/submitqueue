-- Closed records retain their versions so stale writes cannot recreate old openings.
CREATE TABLE IF NOT EXISTS issue_state (
    queue                   VARBINARY(255)  NOT NULL,
    task_id                 VARBINARY(255)  NOT NULL,
    unit_id                 VARBINARY(1024) NOT NULL,

    status                  VARBINARY(32)   NOT NULL,
    version                 INT             NOT NULL,

    first_seen_build_id     VARBINARY(1020) NOT NULL, -- Accommodates existing VARCHAR(255) utf8mb4 build IDs.

    last_processed_build_id VARBINARY(1020) NOT NULL,
    last_changed_build_id   VARBINARY(1020) NOT NULL,
    changed_at_ms           BIGINT          NOT NULL,

    PRIMARY KEY (queue, task_id, unit_id)
) ENGINE=InnoDB;
