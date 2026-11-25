-- A marker exists only after the green projection was backfilled for a lineage.
CREATE TABLE IF NOT EXISTS green_project_lineage (
    queue VARBINARY(255) NOT NULL,
    lineage VARBINARY(255) NOT NULL,
    PRIMARY KEY (queue, lineage)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
