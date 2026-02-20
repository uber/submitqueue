CREATE TABLE IF NOT EXISTS request (
    queue VARCHAR(255) NOT NULL,
    seq BIGINT NOT NULL,
    change_source VARCHAR(255) NOT NULL,
    change_ids JSON NOT NULL,
    land_strategy VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
