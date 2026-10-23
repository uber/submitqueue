-- Persistent source-order boundary; pending passes are resumed before admitting another source.
CREATE TABLE IF NOT EXISTS issue_tracking (
    queue           VARBINARY(255) NOT NULL,
    request_id      VARBINARY(255) NOT NULL,
    build_id        VARBINARY(1020) NOT NULL,
    notification_id VARBINARY(1020) NOT NULL,
    after_task_id   VARBINARY(255) NOT NULL,
    after_unit_id   VARBINARY(1024) NOT NULL,
    phase           VARBINARY(32) NOT NULL,
    version         INT NOT NULL,
    PRIMARY KEY (queue)
) ENGINE=InnoDB;
