CREATE TABLE IF NOT EXISTS request_projects_by_issue (
    queue           VARBINARY(255)  NOT NULL,
    request_id      VARBINARY(255)  NOT NULL,
    task_id         VARBINARY(255)  NOT NULL,
    unit_id         VARBINARY(1024) NOT NULL,
    project         VARBINARY(1020) NOT NULL,
    PRIMARY KEY (queue, request_id, task_id, unit_id, project)
) ENGINE=InnoDB;
