CREATE TABLE IF NOT EXISTS batch (
    id VARCHAR(255) NOT NULL,
    queue VARCHAR(255) NOT NULL,
    contains JSON NOT NULL,
    dependencies JSON NOT NULL,
    score DOUBLE NOT NULL,
    state VARCHAR(255) NOT NUll,
    version INT NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_queue_state (queue, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
