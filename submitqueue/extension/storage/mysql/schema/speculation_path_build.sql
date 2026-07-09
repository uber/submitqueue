CREATE TABLE IF NOT EXISTS speculation_path_build (
    path_id VARCHAR(255) NOT NULL,
    build_id VARCHAR(255) NOT NULL,
    batch_id VARCHAR(255) NOT NULL,
    version INT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (path_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
