CREATE TABLE IF NOT EXISTS change_provider (
    request_id VARCHAR(255) NOT NULL,
    change_provider_src VARCHAR(255) NOT NULL,
    change_provider_id VARCHAR(255) NOT NULL,
    metadata JSON NOT NUll,
    PRIMARY KEY (request_id,change_provider_src,change_provider_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
