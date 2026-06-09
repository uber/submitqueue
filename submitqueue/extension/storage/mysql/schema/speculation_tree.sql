CREATE TABLE IF NOT EXISTS speculation_tree (
    batch_id VARCHAR(255) NOT NULL,
    paths JSON NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
