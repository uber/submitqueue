CREATE TABLE IF NOT EXISTS request (
    queue VARCHAR(255) NOT NULL,
    seq BIGINT NOT NULL,
    change_source VARCHAR(255) NOT NULL,
    change_ids JSON NOT NULL,
    land_strategy INT NOT NULL,
    state INT NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
