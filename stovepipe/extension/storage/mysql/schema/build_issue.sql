-- Durable observation identity with a versioned repair association; not an open-issue projection.
-- Binary identifiers preserve byte-exact identity; the full key is at most 2554 bytes.
CREATE TABLE IF NOT EXISTS build_issue (
    queue       VARBINARY(255) NOT NULL,
    build_id    VARBINARY(1020) NOT NULL, -- Accommodates existing VARCHAR(255) utf8mb4 build IDs.

    task_id     VARBINARY(255) NOT NULL,
    unit_id     VARBINARY(1024) NOT NULL,

    fix_id      VARCHAR(255) NOT NULL DEFAULT '',
    version     INT NOT NULL DEFAULT 0,

    PRIMARY KEY (queue, build_id, task_id, unit_id)
) ENGINE=InnoDB;
