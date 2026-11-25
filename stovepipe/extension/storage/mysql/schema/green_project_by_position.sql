-- Rebuildable ordering projection of explicit green validation facts.
CREATE TABLE IF NOT EXISTS green_project_by_position (
    queue VARBINARY(255) NOT NULL,
    lineage VARBINARY(255) NOT NULL,
    project VARBINARY(255) NOT NULL,
    position BIGINT NOT NULL,
    uri VARBINARY(255) NOT NULL,
    PRIMARY KEY (queue, lineage, project, position)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
