-- Request-scoped observations are derived independently of operational issue and repair state.
CREATE TABLE IF NOT EXISTS request_issue_summary (
    queue       VARBINARY(255) NOT NULL,
    request_id  VARBINARY(255) NOT NULL,
    build_id    VARBINARY(1020) NOT NULL,
    task_id     VARBINARY(255) NOT NULL,
    unit_id     VARBINARY(1024) NOT NULL,
    fix_id      VARBINARY(1020) NOT NULL DEFAULT '',
    version     INT NOT NULL,
    PRIMARY KEY (queue, request_id, build_id, task_id, unit_id)
) ENGINE=InnoDB;
