-- Per-opening, per-executor submission state. Empty fix_id and skip_reason mean pending.
CREATE TABLE IF NOT EXISTS issue_repair (
    queue VARBINARY(255) NOT NULL,
    opening_build_id VARBINARY(1020) NOT NULL,
    task_id VARBINARY(255) NOT NULL,
    unit_id VARBINARY(1024) NOT NULL,
    executor_id VARBINARY(128) NOT NULL,
    fix_id VARCHAR(255) NOT NULL DEFAULT '',
    skip_reason VARCHAR(255) NOT NULL DEFAULT '',
    metadata JSON NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, opening_build_id, task_id, unit_id, executor_id)
) ENGINE=InnoDB;
