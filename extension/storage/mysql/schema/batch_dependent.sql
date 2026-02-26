CREATE TABLE IF NOT EXISTS batch_dependent (
    batch_id VARCHAR(255) NOT NULL,
    dependents JSON NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
