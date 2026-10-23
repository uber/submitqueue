-- Persistent source-order boundary; pending passes are resumed before admitting another source.
CREATE TABLE IF NOT EXISTS issue_tracking (
    queue       VARBINARY(255) NOT NULL,
    request_id  VARBINARY(255) NOT NULL,
    build_id    VARBINARY(1020) NOT NULL,
    phase       VARBINARY(32) NOT NULL,
    version     INT NOT NULL,
    PRIMARY KEY (queue)
) ENGINE=InnoDB;
