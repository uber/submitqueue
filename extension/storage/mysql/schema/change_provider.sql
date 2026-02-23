CREATE TABLE IF NOT EXISTS change_provider (
    id VARCHAR(255) NOT NULL,
    queue VARCHAR(255) NOT NULL,
    change_provider_src VARCHAR(255) NOT NULL,
    change_provider_id VARCHAR(255) NOT NULL,
    metadata JSON NOT NUll,
    version INT NOT NULL,
    PRIMARY KEY (id,queue,change_provider_src,change_provider_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
