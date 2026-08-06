CREATE TABLE IF NOT EXISTS request_batch (
    queue VARCHAR(255) NOT NULL,
    request_id VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, request_id, batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
