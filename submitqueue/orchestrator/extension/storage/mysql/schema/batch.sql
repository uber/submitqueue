CREATE TABLE IF NOT EXISTS batch (
    queue VARCHAR(255) NOT NULL,
    id VARCHAR(255) NOT NULL,
    contains JSON NOT NULL,
    dependencies JSON NOT NULL,
    state VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
