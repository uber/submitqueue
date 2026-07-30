CREATE TABLE IF NOT EXISTS speculation_path_set (
    head VARCHAR(255) NOT NULL,
    paths JSON NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (head)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
