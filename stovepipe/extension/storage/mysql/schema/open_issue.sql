-- Retained for schema compatibility with existing deployments; active issue state uses issue_state.
CREATE TABLE IF NOT EXISTS open_issue (
    queue                    VARBINARY(255) NOT NULL,
    task_id                  VARBINARY(255) NOT NULL,
    unit_id                  VARBINARY(1024) NOT NULL,

    first_seen_build_id      VARBINARY(1020) NOT NULL, -- Accommodates existing VARCHAR(255) utf8mb4 build IDs.

    PRIMARY KEY (queue, task_id, unit_id)
) ENGINE=InnoDB;
