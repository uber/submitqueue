CREATE TABLE IF NOT EXISTS request (
    queue VARCHAR(255) NOT NULL,
    id VARCHAR(255) NOT NULL,
    change_uri JSON NOT NULL,
    land_strategy VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (queue, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
