CREATE TABLE IF NOT EXISTS request_batch (
    request_id VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
