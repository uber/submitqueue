CREATE TABLE IF NOT EXISTS batch_dependent (
    queue VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    dependents JSON NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
