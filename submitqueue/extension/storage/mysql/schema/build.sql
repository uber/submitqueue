CREATE TABLE IF NOT EXISTS build (
    id VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    speculation_path JSON NOT NULL,
    score FLOAT NOT NULL,
    status VARCHAR(64) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uniq_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
