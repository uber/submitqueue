-- project_fact_by_degree is a rebuildable ordering mapping for named project facts.
-- The mixed-direction primary key supports bounded, most-broken-first reads.
CREATE TABLE IF NOT EXISTS project_fact_by_degree (
    queue        VARBINARY(255) NOT NULL,
    uri          VARBINARY(255) NOT NULL,
    degree       DOUBLE NOT NULL,
    project      VARBINARY(255) NOT NULL,
    PRIMARY KEY (queue, uri, degree DESC, project ASC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
