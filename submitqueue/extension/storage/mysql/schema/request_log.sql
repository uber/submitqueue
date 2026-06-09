CREATE TABLE IF NOT EXISTS request_log (
    request_id VARCHAR(255) NOT NULL,
    queue VARCHAR(255) NOT NULL,
    change_uri JSON NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    salt BIGINT NOT NULL,
    status VARCHAR(64) NOT NULL,
    request_version INT NOT NULL,
    last_error TEXT NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (request_id, timestamp_ms, salt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
