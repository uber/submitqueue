-- Accepted issue results within each request, ordered by task and unit.
CREATE TABLE IF NOT EXISTS request_issue_summary (
    queue                 VARBINARY(255) NOT NULL,
    request_id            VARBINARY(255) NOT NULL,
    task_id               VARBINARY(255) NOT NULL,
    unit_id               VARBINARY(1024) NOT NULL,

    kind                  VARCHAR(16) NOT NULL,
    build_id              VARBINARY(1020) NOT NULL,
    first_seen_build_id   VARBINARY(1020) NOT NULL,

    version               INT NOT NULL DEFAULT 0,

    PRIMARY KEY (queue, request_id, task_id, unit_id)
) ENGINE=InnoDB;
